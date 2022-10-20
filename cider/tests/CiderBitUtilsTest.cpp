/*
 * Copyright (c) 2022 Intel Corporation.
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

#include <glob.h>
#include <gtest/gtest.h>
#include <boost/program_options.hpp>
#include "util/CiderBitUtils.h"

#include <cstdint>
#include <iostream>
#include <utility>
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

  setBitAt(bv1, 0);
  EXPECT_FALSE(CheckBitVectorEq(bv1, bv2, length));
  EXPECT_FALSE(CheckBitVectorEq(bv2, bv1, length));

  setBitAt(bv2, 0);
  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));

  setBitAt(bv2, length - 1);
  EXPECT_FALSE(CheckBitVectorEq(bv1, bv2, length));

  setBitAt(bv1, length - 1);
  EXPECT_TRUE(CheckBitVectorEq(bv1, bv2, length));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  namespace po = boost::program_options;

  po::options_description desc("Options");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
