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

#include <gtest/gtest.h>
#include "CiderInt128.h"

TEST(CiderInt128Test, ConstructorTest) {
  auto val_1 = CiderInt128(10, 3);
  EXPECT_EQ(val_1.high, 10);
  EXPECT_EQ(val_1.low, 3);

  auto val_2 = CiderInt128(1024);
  EXPECT_EQ(val_2.high, 0);
  EXPECT_EQ(val_2.low, 1024);

  auto val_3 = CiderInt128(-2048);
  EXPECT_EQ(val_3.high, -1);
  EXPECT_EQ(val_3.low, uint64_t(18446744073709549568));

  auto val_4 = CiderInt128(std::numeric_limits<int64_t>::min());
  EXPECT_EQ(val_4.high, -1);
  EXPECT_EQ(val_4.low, uint64_t(9223372036854775808));

  auto val_5 = CiderInt128(std::numeric_limits<int64_t>::max());
  EXPECT_EQ(val_5.high, 0);
  EXPECT_EQ(val_5.low, std::numeric_limits<int64_t>::max());
}

TEST(CiderInt128Test, EqualityTest) {
  auto val_1 = CiderInt128(100, 100);
  auto val_2 = CiderInt128(100, 100);
  EXPECT_TRUE(CiderInt128Utils::Equal(val_1, val_2));
  EXPECT_TRUE(CiderInt128Utils::Equal(val_2, val_1));

  EXPECT_TRUE(val_1 == val_2);

  auto val_3 = CiderInt128(100, 200);
  EXPECT_FALSE(CiderInt128Utils::Equal(val_1, val_3));
  EXPECT_FALSE(CiderInt128Utils::Equal(val_3, val_1));

  EXPECT_FALSE(val_1 == val_3);
}

TEST(CiderInt128Test, LShiftTest) {
  auto val = CiderInt128(0, 0xFFFFFFFFFFFFFFFF);
  auto shifted_1 = CiderInt128Utils::LeftShift(val);
  EXPECT_EQ(shifted_1, CiderInt128(1, 0xFFFFFFFFFFFFFFFE));

  auto shifted_16 = CiderInt128Utils::LeftShift(val, 16);
  EXPECT_EQ(shifted_16, CiderInt128(0xFFFF, 0xFFFFFFFFFFFF0000));
}

TEST(CiderInt128Test, NegationTest) {
  auto val = CiderInt128(2038);
  auto negval = CiderInt128Utils::Negate(val);
  EXPECT_EQ(negval, CiderInt128(-2038));

  auto val_2 = CiderInt128(0);
  auto negval_2 = CiderInt128Utils::Negate(val_2);
  EXPECT_EQ(negval_2, CiderInt128(0));

  auto val_3 = CiderInt128(std::numeric_limits<int64_t>::max());
  auto negval_3 = CiderInt128Utils::Negate(val_3);
  EXPECT_EQ(negval_3, CiderInt128(-std::numeric_limits<int64_t>::max()));
}

TEST(CiderInt128Test, DivModTest) {
  auto val = CiderInt128(1024);
  uint64_t remainder = 0;
  auto res = CiderInt128Utils::DivModPositive(val, 5, remainder);

  EXPECT_EQ(res, CiderInt128(204));
  EXPECT_EQ(remainder, 4);

  res = CiderInt128Utils::DivModPositive(res, 2, remainder);
  EXPECT_EQ(res, CiderInt128(102));
  EXPECT_EQ(remainder, 0);

  res = CiderInt128Utils::DivModPositive(res, 10, remainder);
  EXPECT_EQ(res, CiderInt128(10));
  EXPECT_EQ(remainder, 2);

  res = CiderInt128Utils::DivModPositive(res, 100, remainder);
  EXPECT_EQ(res, CiderInt128(0));
  EXPECT_EQ(remainder, 10);
}

TEST(CiderInt128Test, ToStringTest) {
  auto ival = CiderInt128(33252311246);
  auto istr = CiderInt128Utils::Int128ToString(ival);
  auto dstr = CiderInt128Utils::Decimal128ToString(ival, 38, 0);
  EXPECT_EQ(istr, "33252311246");
  EXPECT_EQ(dstr, "33252311246");

  auto dval = CiderInt128(187694187358912);
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 5);
  EXPECT_EQ(dstr, "1876941873.58912");
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 10);
  EXPECT_EQ(dstr, "18769.4187358912");
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 20);
  EXPECT_EQ(dstr, "0.00000187694187358912");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
