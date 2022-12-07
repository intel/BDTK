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
#include "CiderAggTestHelper.h"
#include "TestHelpers.h"
#include "cider/CiderTypes.h"
#include "common/hashtable/FixedHashMap.h"

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

using namespace TestHelpers;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderNewHashTableTest : public ::testing::Test {
 protected:
  static MockTable* MockTableForTest;

  static void SetUpTestSuite() {
    if (nullptr == MockTableForTest) {
      size_t element_num = 5;

      std::vector<std::string> col_names = {"tinyint_notnull",
                                            "tinyint_null",
                                            "smallint_notnull",
                                            "smallint_null",
                                            "int_notnull",
                                            "int_null",
                                            "bigint_notnull",
                                            "bigint_null",
                                            "float_notnull",
                                            "float_null",
                                            "double_notnull",
                                            "double_null",
                                            "bool_true_notnull",
                                            "bool_true_null",
                                            "bool_false_notnull",
                                            "bool_false_null"};

      std::vector<SQLTypeInfo> col_types = {
          SQLTypeInfo(kTINYINT, true),
          SQLTypeInfo(kTINYINT, false),
          SQLTypeInfo(kSMALLINT, true),
          SQLTypeInfo(kSMALLINT, false),
          SQLTypeInfo(kINT, true),
          SQLTypeInfo(kINT, false),
          SQLTypeInfo(kBIGINT, true),
          SQLTypeInfo(kBIGINT, false),
          SQLTypeInfo(kFLOAT, true),
          SQLTypeInfo(kFLOAT, false),
          SQLTypeInfo(kDOUBLE, true),
          SQLTypeInfo(kDOUBLE, false),
          SQLTypeInfo(kBOOLEAN, true),
          SQLTypeInfo(kBOOLEAN, false),
          SQLTypeInfo(kBOOLEAN, true),
          SQLTypeInfo(kBOOLEAN, false),
      };

      int8_t* tinyint_ptr = new int8_t[element_num];
      int16_t* smallint_ptr = new int16_t[element_num];
      int32_t* int_ptr = new int32_t[element_num];
      int64_t* bigint_ptr = new int64_t[element_num];
      float* float_ptr = new float[element_num];
      double* double_ptr = new double[element_num];
      bool* bool_true_ptr = new bool[element_num];
      bool* bool_false_ptr = new bool[element_num];

      for (size_t i = 0; i < element_num; ++i) {
        tinyint_ptr[i] = i;
        smallint_ptr[i] = i;
        int_ptr[i] = i;
        bigint_ptr[i] = i;
        float_ptr[i] = i;
        double_ptr[i] = i;
        bool_true_ptr[i] = true;
        bool_false_ptr[i] = false;
      }

      std::vector<int8_t*> col_datas = {reinterpret_cast<int8_t*>(tinyint_ptr),
                                        reinterpret_cast<int8_t*>(tinyint_ptr),
                                        reinterpret_cast<int8_t*>(smallint_ptr),
                                        reinterpret_cast<int8_t*>(smallint_ptr),
                                        reinterpret_cast<int8_t*>(int_ptr),
                                        reinterpret_cast<int8_t*>(int_ptr),
                                        reinterpret_cast<int8_t*>(bigint_ptr),
                                        reinterpret_cast<int8_t*>(bigint_ptr),
                                        reinterpret_cast<int8_t*>(float_ptr),
                                        reinterpret_cast<int8_t*>(float_ptr),
                                        reinterpret_cast<int8_t*>(double_ptr),
                                        reinterpret_cast<int8_t*>(double_ptr),
                                        reinterpret_cast<int8_t*>(bool_true_ptr),
                                        reinterpret_cast<int8_t*>(bool_true_ptr),
                                        reinterpret_cast<int8_t*>(bool_false_ptr),
                                        reinterpret_cast<int8_t*>(bool_false_ptr)};

      MockTableForTest = new MockTable(col_names, col_types, col_datas, element_num);
    }
  }

  static void TearDownTestSuite() {
    delete MockTableForTest;
    MockTableForTest = nullptr;
  }
};

MockTable* CiderNewHashTableTest::MockTableForTest = nullptr;

struct Block {
  int64_t sum = 0;
  size_t count = 0;

  void add(int8_t value) {
    sum += value;
    ++count;
  }

  template <size_t unroll_count = 128 / sizeof(int8_t)>
  void addBatch(const int8_t* ptr, size_t size) {
    /// Compiler cannot unroll this loop, do it manually.
    /// (at least for floats, most likely due to the lack of -fassociative-math)

    int8_t partial_sums[unroll_count]{};

    const auto* end = ptr + size;
    const auto* unrolled_end = ptr + (size / unroll_count * unroll_count);

    while (ptr < unrolled_end) {
      for (size_t i = 0; i < unroll_count; ++i)
        partial_sums[i] += ptr[i];
      ptr += unroll_count;
    }

    for (size_t i = 0; i < unroll_count; ++i)
      sum += partial_sums[i];

    while (ptr < end) {
      sum += *ptr;
      ++ptr;
    }

    count += size;
  }

  void merge(const Block& other) {
    sum += other.sum;
    count += other.count;
  }

  int64_t getSum() const { return sum; }

  size_t getCount() const { return count; }

  double getAvg() const { return (double)sum / count; }

  bool operator!() const { return !count; }
};

TEST_F(CiderNewHashTableTest, aggInt8Test) {
  using AggregatedDataWithUInt8Key =
      FixedImplicitZeroHashMapWithCalculatedSize<UInt8, Block>;
  AggregatedDataWithUInt8Key map;

  std::vector<UInt8> keys{1, 2, 3, 4, 5};
  std::vector<Int64> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<UInt8> keys2{1, 2, 3, 4, 5};
  std::vector<Int64> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = map[keys2[i]];
    block.add(values2[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), 60);
    CHECK_EQ(map[keys[i]].getCount(), 2);
    CHECK_EQ(map[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggInt16Test) {
  using AggregatedDataWithUInt16Key =
      FixedImplicitZeroHashMapWithCalculatedSize<UInt16, Block>;
  AggregatedDataWithUInt16Key map;

  std::vector<UInt16> keys{1, 2, 3, 4, 5};
  std::vector<Int64> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<UInt8> keys2{1, 2, 3, 4, 5};
  std::vector<Int64> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = map[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), 60);
    CHECK_EQ(map[keys[i]].getCount(), 2);
    CHECK_EQ(map[keys[i]].getAvg(), 30);
  }
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  namespace po = boost::program_options;

  po::options_description desc("Options");

  logger::LogOptions log_options(argv[0]);
  log_options.max_files_ = 0;  // stderr only by default
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
