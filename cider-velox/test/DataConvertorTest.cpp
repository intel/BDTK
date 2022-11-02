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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "DataConvertor.h"
#include "velox/type/IntervalDayTime.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using facebook::velox::test::VectorMaker;
using namespace facebook::velox::plugin;
using facebook::velox::plugin::DataConvertor;

class DataConvertorTest : public testing::Test {
 public:
  template <typename T>
  FlatVectorPtr<T> makeNullableFlatVector(const std::vector<std::optional<T>>& data) {
    return vectorMaker_.flatVectorNullable(data);
  }

  RowVectorPtr makeRowVector(const std::vector<VectorPtr>& children) {
    return vectorMaker_.rowVector(children);
  }

  template <typename T>
  DictionaryVectorPtr<T> makeDictionaryVector(const std::vector<std::optional<T>>& data) {
    return vectorMaker_.dictionaryVector(data);
  }

  template <typename T>
  ConstantVectorPtr<T> makeConstantVector(const std::vector<std::optional<T>>& data) {
    return vectorMaker_.constantVector(data);
  }

 protected:
  std::unique_ptr<memory::ScopedMemoryPool> pool_{memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

template <typename T>
void testToCiderDirect(RowVectorPtr rowVector,
                       const std::vector<std::optional<T>>& data,
                       int numRows,
                       memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  const T* col_0 = reinterpret_cast<const T*>(cb.column(0));
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      if (std::is_integral<T>::value) {
        EXPECT_EQ(plugin::inline_int_null_value<T>(), col_0[idx]);
      } else if (std::is_same<T, float>::value) {
        EXPECT_EQ(FLT_MIN, col_0[idx]);
      } else if (std::is_same<T, double>::value) {
        // decimal should also be handled here as velox treats decimal as double
        EXPECT_EQ(DBL_MIN, col_0[idx]);
      } else {
        VELOX_NYI("Conversion is not supported yet");
      }
    } else {
      EXPECT_EQ(data[idx], col_0[idx]);
    }
  }
}

template <>
void testToCiderDirect<bool>(RowVectorPtr rowVector,
                             const std::vector<std::optional<bool>>& data,
                             int numRows,
                             memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  const int8_t* col_0 = cb.column(0);
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(plugin::inline_int_null_value<int8_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(data[idx].value(), static_cast<bool>(col_0[idx]));
    }
  }
}

template <>
void testToCiderDirect<Timestamp>(RowVectorPtr rowVector,
                                  const std::vector<std::optional<Timestamp>>& data,
                                  int numRows,
                                  memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  const int64_t* col_0 = reinterpret_cast<const int64_t*>(cb.column(0));
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(plugin::inline_int_null_value<int64_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(data[idx].value().toMicros(), col_0[idx]);
    }
  }
}

template <>
void testToCiderDirect<Date>(RowVectorPtr rowVector,
                             const std::vector<std::optional<Date>>& data,
                             int numRows,
                             memory::MemoryPool* pool) {
  static const int64_t SecondsInOneDay = 24 * 60 * 60;
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  auto col_0 = reinterpret_cast<const int64_t*>(cb.column(0));
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(plugin::inline_int_null_value<int64_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(data[idx]->days() * SecondsInOneDay, col_0[idx]);
    }
  }
}

template <>
void testToCiderDirect<StringView>(RowVectorPtr rowVector,
                                   const std::vector<std::optional<StringView>>& data,
                                   int numRows,
                                   memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  auto col_0 = reinterpret_cast<const CiderByteArray*>(cb.column(0));
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      EXPECT_EQ(col_0[idx].len, 0);
      EXPECT_EQ(col_0[idx].ptr, nullptr);
    } else {
      EXPECT_EQ(0, memcmp(data[idx]->data(), col_0[idx].ptr, col_0[idx].len));
    }
  }
}

TEST_F(DataConvertorTest, directToCiderVarcharOneCol) {
  std::vector<std::optional<StringView>> data = {StringView("10aaaaaaaa", 10),
                                                 StringView("12aaaaaaaaaa", 12),
                                                 StringView("14aaaaaaaaaaaa", 14),
                                                 StringView("10bbbbbbbb", 10),
                                                 StringView("12bbbbbbbbbb", 12),
                                                 std::nullopt,
                                                 StringView("16bbbbbbbbbbbbbb", 16),
                                                 StringView("10cccccccc", 10),
                                                 std::nullopt,
                                                 StringView("16cccccccccccccc", 16)};
  auto col_dict = makeDictionaryVector<StringView>(data);
  auto rowVector_dict = makeRowVector({col_dict});
  testToCiderDirect<StringView>(rowVector_dict, data, data.size(), pool_.get());

  auto col_flat = makeNullableFlatVector<StringView>(data);
  auto rowVector_flat = makeRowVector({col_flat});
  testToCiderDirect<StringView>(rowVector_flat, data, data.size(), pool_.get());

  std::vector<std::optional<StringView>> data_constant = {
      StringView("abc", 3), StringView("abc", 3), StringView("abc", 3)};
  auto col_constant = makeConstantVector<StringView>(data_constant);
  auto rowVector_constant = makeRowVector({col_constant});
  testToCiderDirect<StringView>(
      rowVector_constant, data_constant, data_constant.size(), pool_.get());

  std::vector<std::optional<StringView>> data_null = {
      std::nullopt, std::nullopt, std::nullopt};
  auto col_null = makeConstantVector<StringView>(data_null);
  auto rowVector_null = makeRowVector({col_null});
  testToCiderDirect<StringView>(rowVector_null, data_null, data_null.size(), pool_.get());
}

TEST_F(DataConvertorTest, directToCiderIntegerOneCol) {
  std::vector<std::optional<int32_t>> data = {
      0, std::nullopt, 1, 3, std::nullopt, -1234, -99, -999, 1000, -1};
  auto col_dict = makeDictionaryVector<int32_t>(data);
  auto rowVector_dict = makeRowVector({col_dict});
  testToCiderDirect<int32_t>(rowVector_dict, data, data.size(), pool_.get());

  auto col_flat = makeNullableFlatVector<int32_t>(data);
  auto rowVector_flat = makeRowVector({col_flat});
  testToCiderDirect<int32_t>(rowVector_flat, data, data.size(), pool_.get());

  std::vector<std::optional<int32_t>> data_constant = {1, 1, 1};
  auto col_constant = makeConstantVector<int32_t>(data_constant);
  auto rowVector_constant = makeRowVector({col_constant});
  testToCiderDirect<int32_t>(
      rowVector_constant, data_constant, data_constant.size(), pool_.get());

  std::vector<std::optional<int32_t>> data_null = {
      std::nullopt, std::nullopt, std::nullopt};
  auto col_null = makeConstantVector<int32_t>(data_null);
  auto rowVector_null = makeRowVector({col_null});
  testToCiderDirect<int32_t>(rowVector_null, data_null, data_null.size(), pool_.get());
}

TEST_F(DataConvertorTest, directToCiderBigintOneCol) {
  std::vector<std::optional<int64_t>> data = {
      0, 1, std::nullopt, 3, 1024, -123456, -99, -999, std::nullopt, -1};
  auto col_dict = makeDictionaryVector<int64_t>(data);
  auto rowVector_dict = makeRowVector({col_dict});
  testToCiderDirect<int64_t>(rowVector_dict, data, data.size(), pool_.get());

  auto col_flat = makeNullableFlatVector<int64_t>(data);
  auto rowVector_flat = makeRowVector({col_flat});
  testToCiderDirect<int64_t>(rowVector_flat, data, data.size(), pool_.get());

  std::vector<std::optional<int64_t>> data_constant = {1, 1, 1};
  auto col_constant = makeConstantVector<int64_t>(data_constant);
  auto rowVector_constant = makeRowVector({col_constant});
  testToCiderDirect<int64_t>(
      rowVector_constant, data_constant, data_constant.size(), pool_.get());

  std::vector<std::optional<int64_t>> data_null = {
      std::nullopt, std::nullopt, std::nullopt};
  auto col_null = makeConstantVector<int64_t>(data_null);
  auto rowVector_null = makeRowVector({col_null});
  testToCiderDirect<int64_t>(rowVector_null, data_null, data_null.size(), pool_.get());
}

TEST_F(DataConvertorTest, directToCiderDoubleOneCol) {
  std::vector<std::optional<double>> data = {
      0.5, 1, std::nullopt, 3.14, 1024, -123456, -99.99, -999, std::nullopt, -1};
  auto col_dict = makeDictionaryVector<double>(data);
  auto rowVector_dict = makeRowVector({col_dict});
  testToCiderDirect<double>(rowVector_dict, data, data.size(), pool_.get());

  auto col_flat = makeNullableFlatVector<double>(data);
  auto rowVector_flat = makeRowVector({col_flat});
  testToCiderDirect<double>(rowVector_flat, data, data.size(), pool_.get());

  std::vector<std::optional<double>> data_constant = {1.23, 1.23, 1.23};
  auto col_constant = makeConstantVector<double>(data_constant);
  auto rowVector_constant = makeRowVector({col_constant});
  testToCiderDirect<double>(
      rowVector_constant, data_constant, data_constant.size(), pool_.get());

  std::vector<std::optional<double>> data_null = {
      std::nullopt, std::nullopt, std::nullopt};
  auto col_null = makeConstantVector<double>(data_null);
  auto rowVector_null = makeRowVector({col_null});
  testToCiderDirect<double>(rowVector_null, data_null, data_null.size(), pool_.get());
}

TEST_F(DataConvertorTest, directToCiderBoolOneCol) {
  int numRows = 10;
  std::vector<std::optional<bool>> data = {
      true,
      false,
      std::nullopt,
      false,
      true,
      true,
      false,
      std::nullopt,
      false,
      true,
  };
  auto col = makeNullableFlatVector<bool>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<bool>(rowVector, data, numRows, pool_.get());
}

TEST_F(DataConvertorTest, directToCiderTimestampOneCol) {
  int numRows = 10;
  std::vector<std::optional<Timestamp>> data = {
      Timestamp(28800, 10),
      Timestamp(946713600, 0),
      Timestamp(0, 0),
      std::nullopt,
      Timestamp(946758116, 20),
      Timestamp(-21600, 0),
      std::nullopt,
      Timestamp(957164400, 30),
      Timestamp(946729316, 0),
      Timestamp(7200, 0),
  };
  auto col = makeNullableFlatVector<Timestamp>(data);
  auto rowVector = makeRowVector({col});
  testToCiderDirect<Timestamp>(rowVector, data, numRows, pool_.get());
}

TEST_F(DataConvertorTest, directToCiderDateOneCol) {
  std::vector<std::optional<Date>> data = {
      Date(1),
      Date(2),
      Date(3),
      std::nullopt,
      Date(5),
      Date(6),
      std::nullopt,
      Date(8),
      Date(9),
      Date(10),
  };
  auto col_flat = makeNullableFlatVector<Date>(data);
  auto rowVector_flat = makeRowVector({col_flat});
  testToCiderDirect<Date>(rowVector_flat, data, data.size(), pool_.get());

  auto col_dict = makeDictionaryVector<Date>(data);
  auto rowVector_dict = makeRowVector({col_dict});
  testToCiderDirect<Date>(rowVector_dict, data, data.size(), pool_.get());

  std::vector<std::optional<Date>> data_constant = {Date(1), Date(1), Date(1)};
  auto col_constant = makeConstantVector<Date>(data_constant);
  auto rowVector_constant = makeRowVector({col_constant});
  testToCiderDirect<Date>(
      rowVector_constant, data_constant, data_constant.size(), pool_.get());

  std::vector<std::optional<Date>> data_null = {std::nullopt, std::nullopt, std::nullopt};
  auto col_null = makeConstantVector<Date>(data_null);
  auto rowVector_null = makeRowVector({col_null});
  testToCiderDirect<Date>(rowVector_null, data_null, data_null.size(), pool_.get());
}

template <typename T>
void testToCiderWithArrow(RowVectorPtr rowVector,
                          const std::vector<std::optional<T>>& data,
                          int numRows,
                          memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::ARROW);
  CiderBatch cb = convertor->convertToCider(rowVector, numRows, nullptr, pool);
  EXPECT_EQ(numRows, cb.row_num());

  const T* col_0 = reinterpret_cast<const T*>(cb.column(0));
  for (auto idx = 0; idx < numRows; idx++) {
    if (data[idx] == std::nullopt) {
      if (std::is_integral<T>::value) {
        EXPECT_EQ(plugin::inline_int_null_value<T>(), col_0[idx]);
      } else if (std::is_same<T, float>::value) {
        EXPECT_EQ(FLT_MIN, col_0[idx]);
      } else if (std::is_same<T, double>::value) {
        EXPECT_EQ(DBL_MIN, col_0[idx]);
      } else {
        VELOX_NYI("Conversion is not supported yet");
      }
    } else {
      EXPECT_EQ(data[idx], col_0[idx]);
    }
  }
}

TEST_F(DataConvertorTest, toCiderIntegerOneColArrow) {
  int numRows = 10;
  std::vector<std::optional<int32_t>> data = {
      0, std::nullopt, 1, 3, std::nullopt, -1234, -99, -999, 1000, -1};
  auto col = makeNullableFlatVector<int32_t>(data);
  auto rowVector = makeRowVector({col});
  testToCiderWithArrow<int32_t>(rowVector, data, numRows, pool_.get());
}

TEST_F(DataConvertorTest, toCiderBigintOneColArrow) {
  int numRows = 10;
  std::vector<std::optional<int64_t>> data = {
      0, 1, std::nullopt, 3, 1024, -123456, -99, -999, std::nullopt, -1};
  auto col = makeNullableFlatVector<int64_t>(data);
  auto rowVector = makeRowVector({col});
  testToCiderWithArrow<int64_t>(rowVector, data, numRows, pool_.get());
}

TEST_F(DataConvertorTest, toCiderDoubleOneColArrow) {
  int numRows = 10;
  std::vector<std::optional<double>> data = {
      0.5, 1, std::nullopt, 3.14, 1024, -123456, -99.99, -999, std::nullopt, -1};
  auto col = makeNullableFlatVector<double>(data);
  auto rowVector = makeRowVector({col});
  testToCiderWithArrow<double>(rowVector, data, numRows, pool_.get());
}

template <typename T>
void testToVeloxDirect(CiderBatch& input,
                       const CiderTableSchema& schema,
                       memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<T>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const T* col_0 = reinterpret_cast<const T*>(input.column(0));
  int num_rows = input.row_num();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (std::is_integral<T>::value) {
      if (col_0[idx] == plugin::inline_int_null_value<T>()) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, float>::value) {
      if (col_0[idx] == FLT_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, double>::value) {
      if (col_0[idx] == DBL_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else {
      VELOX_NYI("Conversion is not supported yet");
    }
  }
}

template <>
void testToVeloxDirect<bool>(CiderBatch& input,
                             const CiderTableSchema& schema,
                             memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<bool>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const int8_t* col_0 = input.column(0);
  int num_rows = input.row_num();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == plugin::inline_int_null_value<int8_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(childVal_0->valueAt(idx), col_0[idx]);
    }
  }
}

template <>
void testToVeloxDirect<StringView>(CiderBatch& input,
                                   const CiderTableSchema& schema,
                                   memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);

  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());

  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());

  auto childVal_0 = child_0->asFlatVector<StringView>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const CiderByteArray* col_0 = reinterpret_cast<const CiderByteArray*>(input.column(0));
  int num_rows = input.row_num();

  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx].ptr == nullptr) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(0,
                memcmp(childVal_0->valueAt(idx).data(), col_0[idx].ptr, col_0[idx].len));
    }
  }
}

template <>
void testToVeloxDirect<Timestamp>(CiderBatch& input,
                                  const CiderTableSchema& schema,
                                  memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<Timestamp>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const int64_t* col_0 = reinterpret_cast<const int64_t*>(input.column(0));
  int num_rows = input.row_num();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == plugin::inline_int_null_value<int64_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(childVal_0->valueAt(idx),
                Timestamp(col_0[idx] / 1000000, (col_0[idx] % 1000000) * 1000));
    }
  }
}

template <>
void testToVeloxDirect<Date>(CiderBatch& input,
                             const CiderTableSchema& schema,
                             memory::MemoryPool* pool) {
  static const int64_t SecondsInOneDay = 24 * 60 * 60;
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);

  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());

  auto childVal_0 = child_0->asFlatVector<Date>();
  auto nulls_0 = child_0->rawNulls();
  const int64_t* col_0 = reinterpret_cast<const int64_t*>(input.column(0));
  int num_rows = input.row_num();

  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == plugin::inline_int_null_value<int64_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(childVal_0->valueAt(idx), Date(col_0[idx] / SecondsInOneDay));
    }
  }
}

void testToVeloxDecimalDirect(CiderBatch& input,
                              const CiderTableSchema& schema,
                              memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::DIRECT);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<double>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const double* col_0 = reinterpret_cast<const double*>(input.column(0));
  int num_rows = input.row_num();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == plugin::inline_int_null_value<int64_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(rawValues_0[idx], col_0[idx]);
    }
  }
}

TEST_F(DataConvertorTest, directToVeloxIntegerOneCol) {
  std::vector<const int8_t*> col_buffer;
  int32_t* col_0 = reinterpret_cast<int32_t*>(pool_->allocate(sizeof(int32_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int32_t>();
  }
  col_buffer.push_back(reinterpret_cast<int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "i32": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<int32_t>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxBigintOneCol) {
  std::vector<const int8_t*> col_buffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 123;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int64_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "i64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<int64_t>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxDoubleOneCol) {
  std::vector<const int8_t*> col_buffer;
  double* col_0 = reinterpret_cast<double*>(pool_->allocate(sizeof(double) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 3.14;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = DBL_MIN;
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "fp64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<double>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxDecimalOneCol) {
  std::vector<const int8_t*> col_buffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 1.00;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int64_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "decimal": {
        "scale": 2,
        "precision": 19,
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDecimalDirect(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxVarcharOneCol) {
  std::vector<const int8_t*> col_buffer;
  // old ciderbatch do not support null string, so use empty string instead.
  std::vector<CiderByteArray> data = {
      CiderByteArray(10, reinterpret_cast<const uint8_t*>("10aaaaaaaa")),
      CiderByteArray(12, reinterpret_cast<const uint8_t*>("12aaaaaaaaaa")),
      CiderByteArray(14, reinterpret_cast<const uint8_t*>("14aaaaaaaaaaaa")),
      CiderByteArray(10, reinterpret_cast<const uint8_t*>("10bbbbbbbb")),
      CiderByteArray(12, reinterpret_cast<const uint8_t*>("12bbbbbbbbbb")),
      CiderByteArray(0, nullptr),
      CiderByteArray(16, reinterpret_cast<const uint8_t*>("16bbbbbbbbbbbbbb")),
      CiderByteArray(10, reinterpret_cast<const uint8_t*>("10cccccccc")),
      CiderByteArray(0, reinterpret_cast<const uint8_t*>("")),
      CiderByteArray(16, reinterpret_cast<const uint8_t*>("16cccccccccccccc"))};

  int8_t* buf = reinterpret_cast<int8_t*>(
      pool_.get()->allocate(sizeof(CiderByteArray) * data.size()));
  std::memcpy(buf, data.data(), sizeof(CiderByteArray) * data.size());
  col_buffer.push_back(buf);
  CiderBatch input(data.size(), col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "varchar": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<StringView>(input, schema, pool_.get());

  std::free(buf);
}

TEST_F(DataConvertorTest, directToVeloxBoolOneCol) {
  std::vector<const int8_t*> col_buffer;
  int8_t* col_0 = reinterpret_cast<int8_t*>(pool_->allocate(sizeof(int8_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i % 2 ? true : false;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int8_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "bool": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<bool>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxTimestampOneCol) {
  std::vector<const int8_t*> col_buffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i + 86400000000;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int64_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "timestamp": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<Timestamp>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, directToVeloxDateOneCol) {
  std::vector<const int8_t*> col_buffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 86400;
  }
  for (int i = 3; i < num_rows; i += 4) {
    col_0[i] = plugin::inline_int_null_value<int64_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "date": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxDirect<Date>(input, schema, pool_.get());
  std::free(col_0);
}

template <typename T>
void testToVeloxWithArrow(CiderBatch& input,
                          const CiderTableSchema& schema,
                          memory::MemoryPool* pool) {
  std::shared_ptr<DataConvertor> convertor = DataConvertor::create(CONVERT_TYPE::ARROW);
  RowVectorPtr rvp = convertor->convertToRowVector(input, schema, pool);
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(1, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<T>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  const T* col_0 = reinterpret_cast<const T*>(input.column(0));
  int num_rows = input.row_num();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (std::is_integral<T>::value) {
      if (col_0[idx] == plugin::inline_int_null_value<T>()) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, float>::value) {
      if (col_0[idx] == FLT_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else if (std::is_same<T, double>::value) {
      if (col_0[idx] == DBL_MIN) {
        EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
      } else {
        EXPECT_EQ(rawValues_0[idx], col_0[idx]);
      }
    } else {
      VELOX_NYI("Conversion is not supported yet");
    }
  }
}

TEST_F(DataConvertorTest, toVeloxIntegerOneColArrow) {
  std::vector<const int8_t*> col_buffer;
  int32_t* col_0 = reinterpret_cast<int32_t*>(pool_->allocate(sizeof(int32_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int32_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "i32": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxWithArrow<int32_t>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, toVeloxBigintOneColArrow) {
  std::vector<const int8_t*> col_buffer;
  int64_t* col_0 = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 123;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = plugin::inline_int_null_value<int64_t>();
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "i64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxWithArrow<int64_t>(input, schema, pool_.get());
  std::free(col_0);
}

TEST_F(DataConvertorTest, toVeloxDoubleOneColArrow) {
  std::vector<const int8_t*> col_buffer;
  double* col_0 = reinterpret_cast<double*>(pool_->allocate(sizeof(double) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i * 3.14;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = DBL_MIN;
  }
  col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
  CiderBatch input(num_rows, col_buffer);

  std::vector<std::string> col_names = {"col_0"};
  std::vector<::substrait::Type> col_types;
  ::substrait::Type col_type;
  std::string type_json = R"(
    {
      "fp64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  col_types.push_back(col_type);
  CiderTableSchema schema(col_names, col_types);
  testToVeloxWithArrow<double>(input, schema, pool_.get());
  std::free(col_0);
}
