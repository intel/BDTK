/*
 * Copyright(c) 2022-2023 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "util/CiderBitUtils.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

using namespace CiderBitUtils;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderBitUtilsTest : public ::testing::Test {};

template <size_t alignFactor>
void testAlignment() {
  size_t bitsNum = (alignFactor << 3);
  CiderBitVector<alignFactor> bit_vec_1(allocator, bitsNum - 1);
  CiderBitVector<alignFactor> bit_vec_2(allocator, bitsNum);
  CiderBitVector<alignFactor> bit_vec_3(allocator, bitsNum + 1);

  EXPECT_EQ(bit_vec_1.getBitsNum(), bitsNum);
  EXPECT_EQ(bit_vec_2.getBitsNum(), bitsNum);
  EXPECT_EQ(bit_vec_3.getBitsNum(), 2 * bitsNum);

  EXPECT_EQ(reinterpret_cast<uintptr_t>(bit_vec_1.template as<void>()) % alignFactor,
            0ul);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(bit_vec_2.template as<void>()) % alignFactor,
            0ul);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(bit_vec_3.template as<void>()) % alignFactor,
            0ul);
}

template <size_t alignFactor>
void testBasicOp(size_t bit_num) {
  CiderBitVector<alignFactor> bit_vec_1(allocator, bit_num);
  auto actual_bit_num = bit_vec_1.getBitsNum();
  auto bit_vec_uint8 = bit_vec_1.template as<uint8_t>();
  for (size_t i = 0; i < actual_bit_num; ++i) {
    EXPECT_EQ(isBitSetAt(bit_vec_uint8, i), false);
  }
  for (size_t i = 0; i < actual_bit_num; i += 2) {
    setBitAt(bit_vec_uint8, i);
  }
  for (size_t i = 0; i < actual_bit_num; ++i) {
    EXPECT_EQ(isBitSetAt(bit_vec_uint8, i), 0 == (i & 1));
  }
  for (size_t i = 0; i < actual_bit_num; i += 2) {
    clearBitAt(bit_vec_uint8, i);
  }
  for (size_t i = 0; i < actual_bit_num; ++i) {
    EXPECT_EQ(isBitSetAt(bit_vec_uint8, i), false);
  }
}

TEST_F(CiderBitUtilsTest, MemAlignmentTest) {
  testAlignment<16>();
  testAlignment<32>();
  testAlignment<64>();
}

TEST_F(CiderBitUtilsTest, BasicOpTest) {
  testBasicOp<16>(1023);
  testBasicOp<16>(1024);
  testBasicOp<16>(1025);

  testBasicOp<32>(1023);
  testBasicOp<32>(1024);
  testBasicOp<32>(1025);

  testBasicOp<64>(1023);
  testBasicOp<64>(1024);
  testBasicOp<64>(1025);
}

TEST_F(CiderBitUtilsTest, CountTest) {
  CiderBitVector<16> bit_vec_1(allocator, 1025);
  auto bit_vec = bit_vec_1.as<uint8_t>();

  EXPECT_EQ(countSetBits(bit_vec, 1025), 0ul);

  setBitAt(bit_vec, 0);
  EXPECT_EQ(countSetBits(bit_vec, 1025), 1ul);

  setBitAt(bit_vec, 1024);
  EXPECT_EQ(countSetBits(bit_vec, 1025), 2ul);

  setBitAt(bit_vec, 1025);
  EXPECT_EQ(countSetBits(bit_vec, 1025), 2ul);

  setBitAt(bit_vec, 100);
  EXPECT_EQ(countSetBits(bit_vec, 1025), 3ul);
}

TEST_F(CiderBitUtilsTest, CheckEqTest) {
  const int length = 100;
  CiderBitVector<> bit_vec_1(allocator, length);
  CiderBitVector<> bit_vec_2(allocator, length);
  auto bv1 = bit_vec_1.as<uint8_t>();
  auto bv2 = bit_vec_2.as<uint8_t>();

  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));
  EXPECT_TRUE(CheckBitVectorEq(bv2, bv1, length));

  // set at beginning
  setBitAt(bv1, 0);
  EXPECT_FALSE(CheckBitVectorEq(bv1, bv2, length));
  EXPECT_FALSE(CheckBitVectorEq(bv2, bv1, length));
  setBitAt(bv2, 0);
  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));

  // set somewhere in the middle
  setBitAt(bv2, 8);
  EXPECT_FALSE(CheckBitVectorEq(bv1, bv2, length));
  setBitAt(bv1, 8);
  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));

  // set last bit
  setBitAt(bv2, length - 1);
  EXPECT_FALSE(CheckBitVectorEq(bv1, bv2, length));
  setBitAt(bv1, length - 1);
  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));
}

TEST_F(CiderBitUtilsTest, ByteToBitTest) {
  std::random_device rand_dev;
  std::mt19937 rand_engine(rand_dev());
  std::uniform_int_distribution<uint8_t> dist(0,1);
  auto gen = [&dist, &rand_engine]() {return dist(rand_engine);};

  auto check_byte_bit = [](uint8_t* byte, uint8_t* bit, size_t len) {
    for (size_t i = 0; i < len; ++i) {
      EXPECT_EQ((bool)byte[i], isBitSetAt(bit, i));
    }
  };

  std::vector<uint8_t> byte_vec(32 * 200, 0);
  std::generate(byte_vec.begin(), byte_vec.end(), gen);
  std::vector<uint8_t> bit_vec(byte_vec.size(), 0);

  byteToBit(byte_vec.data(), bit_vec.data(), byte_vec.size());
  check_byte_bit(byte_vec.data(), bit_vec.data(), byte_vec.size());

  auto byte_vec_backup = byte_vec;
  byteToBit(byte_vec.data(), byte_vec.data(), byte_vec.size());
  check_byte_bit(byte_vec_backup.data(), bit_vec.data(), byte_vec_backup.size());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}