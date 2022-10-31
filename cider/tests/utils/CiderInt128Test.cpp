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

TEST(CiderInt128Test, ToStringTest) {
  // integer test
  auto op1 = __int128_t(191929293939);
  auto op2 = __int128_t(191929293939);
  // since there's no way to directly declare an int128_t value
  // we manually create one by multiplying int64 values
  auto ival = op1 * op2;
  auto istr = CiderInt128Utils::Int128ToString(ival);
  auto dstr = CiderInt128Utils::Decimal128ToString(ival, 38, 0);
  EXPECT_EQ(istr, "36836853871923062135721");
  EXPECT_EQ(dstr, "36836853871923062135721");

  // decimal test
  auto dval = __int128_t(187694187358912);
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 5);
  EXPECT_EQ(dstr, "1876941873.58912");
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 10);
  EXPECT_EQ(dstr, "18769.4187358912");
  dstr = CiderInt128Utils::Decimal128ToString(dval, 38, 20);
  EXPECT_EQ(dstr, "0.00000187694187358912");

  // negative value test
  auto nval = __int128_t(-1134596187461531);
  istr = CiderInt128Utils::Int128ToString(nval);
  dstr = CiderInt128Utils::Decimal128ToString(nval, 38, 10);
  EXPECT_EQ(istr, "-1134596187461531");
  EXPECT_EQ(dstr, "-113459.6187461531");
}

TEST(CiderInt128Test, ToDoubleTest) {
  auto decimal = __int128_t(483248120643921598);
  auto fp64_val = CiderInt128Utils::Decimal128ToDouble(decimal, 38, 5);
  EXPECT_EQ(fp64_val, 4832481206439.21598);
  fp64_val = CiderInt128Utils::Decimal128ToDouble(decimal, 38, 10);
  EXPECT_EQ(fp64_val, 48324812.0643921598);
  fp64_val = CiderInt128Utils::Decimal128ToDouble(decimal, 38, 20);
  EXPECT_EQ(fp64_val, 0.00483248120643921598);
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
