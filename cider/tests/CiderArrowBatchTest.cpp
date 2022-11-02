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
#include "cider/batch/CiderBatch.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"

using namespace CiderBatchUtils;
using namespace std;

static const std::shared_ptr<CiderAllocator> ciderAllocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderArrowBatchTest : public ::testing::Test {};

template <typename T, SQLTypes SQLT>
void runScalarBatchTest(ScalarBatch<T>* batch,
                        const vector<T>& data,
                        const vector<bool>& not_null = {}) {
  assert(data.size() == not_null.size() || not_null.empty());
  CHECK(batch);

  EXPECT_EQ(batch->getCiderType(), SQLT);

  EXPECT_TRUE(batch->resizeBatch(data.size()));
  EXPECT_EQ(batch->getLength(), data.size());
  EXPECT_EQ(batch->getNullCount(), 0);
  {
    auto raw_data = batch->getMutableRawData();
    EXPECT_NE(raw_data, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      raw_data[i] = data[i];
    }
  }

  int64_t null_count = 0;
  if (!not_null.empty()) {
    auto not_null_data = batch->getMutableNulls();
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      if (!not_null[i]) {
        CiderBitUtils::clearBitAt(not_null_data, i);
        ++null_count;
      }
    }
    batch->setNullCount(null_count);
  }

  EXPECT_TRUE(batch->resizeBatch(2 * data.size()));
  EXPECT_EQ(batch->getLength(), 2 * data.size());
  EXPECT_EQ(batch->getNullCount(),
            not_null.empty() ? 0 : null_count + (int64_t)data.size());

  {
    auto raw_data = batch->getRawData();
    EXPECT_NE(raw_data, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      EXPECT_EQ(raw_data[i], data[i]);
    }
  }

  if (!not_null.empty()) {
    auto not_null_data = batch->getNulls();
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i), not_null[i]);
    }
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i + not_null.size()), false);
    }
  }
}

template <SQLTypes SQLT>
void runScalarBatchTest(ScalarBatch<bool>* batch,
                        const vector<bool>& data,
                        const vector<bool>& not_null = {}) {
  assert(data.size() == not_null.size() || not_null.empty());
  CHECK(batch);

  EXPECT_EQ(batch->getCiderType(), SQLT);

  EXPECT_TRUE(batch->resizeBatch(data.size()));
  EXPECT_EQ(batch->getLength(), data.size());
  EXPECT_EQ(batch->getNullCount(), 0);
  {
    auto raw_data = batch->getMutableRawData();
    EXPECT_NE(raw_data, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i]) {
        CiderBitUtils::setBitAt(raw_data, i);
      } else {
        CiderBitUtils::clearBitAt(raw_data, i);
      }
    }
  }

  int64_t null_count = 0;
  if (!not_null.empty()) {
    auto not_null_data = batch->getMutableNulls();
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      if (!not_null[i]) {
        CiderBitUtils::clearBitAt(not_null_data, i);
        ++null_count;
      }
    }
    batch->setNullCount(null_count);
  }

  EXPECT_TRUE(batch->resizeBatch(2 * data.size()));
  EXPECT_EQ(batch->getLength(), 2 * data.size());
  EXPECT_EQ(batch->getNullCount(),
            not_null.empty() ? 0 : null_count + (int64_t)data.size());

  {
    auto raw_data = batch->getRawData();
    EXPECT_NE(raw_data, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(raw_data, i), data[i]);
    }
  }

  if (!not_null.empty()) {
    auto not_null_data = batch->getNulls();
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i), not_null[i]);
    }
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i + not_null.size()), false);
    }
  }
}

template <>
void runScalarBatchTest<bool, kBOOLEAN>(ScalarBatch<bool>* batch,
                                        const vector<bool>& data,
                                        const vector<bool>& not_null) {
  assert(data.size() == not_null.size() || not_null.empty());
  CHECK(batch);

  EXPECT_EQ(batch->getCiderType(), kBOOLEAN);

  EXPECT_TRUE(batch->resizeBatch(data.size()));
  EXPECT_EQ(batch->getLength(), data.size());
  EXPECT_EQ(batch->getNullCount(), 0);
  {
    auto raw_data = batch->getMutableRawData();
    EXPECT_NE(raw_data, nullptr);
    // ScalarBatch<bool> now uses bit-packed values, same as L142
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i]) {
        CiderBitUtils::setBitAt(raw_data, i);
      } else {
        CiderBitUtils::clearBitAt(raw_data, i);
      }
    }
  }

  int64_t null_count = 0;
  if (!not_null.empty()) {
    auto not_null_data = batch->getMutableNulls();
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      if (!not_null[i]) {
        CiderBitUtils::clearBitAt(not_null_data, i);
        ++null_count;
      }
    }
    batch->setNullCount(null_count);
  }

  EXPECT_TRUE(batch->resizeBatch(2 * data.size()));
  EXPECT_EQ(batch->getLength(), 2 * data.size());
  EXPECT_EQ(batch->getNullCount(),
            not_null.empty() ? 0 : null_count + (int64_t)data.size());

  {
    auto raw_data = batch->getRawData();
    EXPECT_NE(raw_data, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(raw_data, i), data[i]);
    }
  }

  if (!not_null.empty()) {
    auto not_null_data = batch->getNulls();
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i), not_null[i]);
    }
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i + not_null.size()), false);
    }
  }
}

template <typename T, SQLTypes SQLT>
void scalarBatchTest(const vector<T>& data, const vector<bool>& not_null = {}) {
  SQLTypeInfo type_info(SQLT, not_null.empty());
  ArrowSchema* schema = convertCiderTypeInfoToArrowSchema(type_info);

  auto batch = ScalarBatch<T>::Create(schema, ciderAllocator);
  EXPECT_TRUE(batch->isRootOwner());

  if constexpr (std::is_same_v<T, bool>) {
    runScalarBatchTest<SQLT>(batch.get(), data, not_null);
  } else {
    runScalarBatchTest<T, SQLT>(batch.get(), data, not_null);
  }
}

TEST_F(CiderArrowBatchTest, ScalarBatchTest) {
  scalarBatchTest<bool, kBOOLEAN>({true, true, false, false, false, true});
  scalarBatchTest<bool, kBOOLEAN>({true, true, false, false, false, true},
                                  {true, false, true, false, true, false});

  scalarBatchTest<int8_t, kTINYINT>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
  scalarBatchTest<int8_t, kTINYINT>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
                                    {
                                        true,
                                        true,
                                        true,
                                        false,
                                        false,
                                        false,
                                        true,
                                        true,
                                        true,
                                        false,
                                    });

  scalarBatchTest<int16_t, kSMALLINT>({199, 21, 33, 44, 55, 26, 17, 68, 19, 30});
  scalarBatchTest<int16_t, kSMALLINT>({199, 21, 33, 44, 55, 26, 17, 68, 19, 30},
                                      {
                                          true,
                                          true,
                                          false,
                                          true,
                                          false,
                                          true,
                                          false,
                                          true,
                                          false,
                                          true,
                                      });

  scalarBatchTest<int32_t, kINT>(
      {1994, 221, 333, 144, 255, 2426, 117, 6328, 11239, 3110});
  scalarBatchTest<int32_t, kINT>({1994, 221, 333, 144, 255, 2426, 117, 6328, 11239, 3110},
                                 {
                                     true,
                                     true,
                                     false,
                                     false,
                                     true,
                                     false,
                                     true,
                                     true,
                                     false,
                                     true,
                                 });

  scalarBatchTest<int64_t, kBIGINT>(
      {19944, 2321, 12333, 13444, 212355, 2231426, 13417, 46328, 151239, 23110});
  scalarBatchTest<int64_t, kBIGINT>(
      {19944, 2321, 12333, 13444, 212355, 2231426, 13417, 46328, 151239, 23110},
      {
          true,
          false,
          true,
          true,
          false,
          false,
          true,
          false,
          true,
          true,
      });

  scalarBatchTest<float, kFLOAT>({1.0, 2.0, 3.0, 4.0, 5.0});
  scalarBatchTest<float, kFLOAT>({1.0, 2.0, 3.0, 4.0, 5.0},
                                 {true, true, false, true, true});

  scalarBatchTest<double, kDOUBLE>({1.0, 2.0, 3.0, 4.0, 5.0});
  scalarBatchTest<double, kDOUBLE>({1.0, 2.0, 3.0, 4.0, 5.0},
                                   {true, true, false, true, true});
}

void runVarcharBatchTest(VarcharBatch* batch,
                         const vector<string>& data,
                         const vector<int>& offset,
                         const vector<bool>& not_null = {}) {
  assert(data.size() == not_null.size() || not_null.empty());
  CHECK(batch);

  EXPECT_EQ(batch->getCiderType(), kVARCHAR);

  EXPECT_TRUE(batch->resizeBatch(data.size()));
  size_t total_len = [&]() {
    size_t len = 0;
    for (auto s : data) {
      len += s.length();
    }
    return len;
  }();
  EXPECT_EQ(total_len, offset[offset.size() - 1]);
  EXPECT_TRUE(batch->resizeDataBuffer(total_len));
  EXPECT_EQ(batch->getLength(), data.size());
  EXPECT_EQ(batch->getNullCount(), 0);

  {
    auto raw_data = batch->getMutableRawData();
    auto raw_offset = batch->getMutableRawOffset();
    EXPECT_NE(raw_data, nullptr);
    EXPECT_NE(raw_offset, nullptr);

    for (size_t i = 0; i < data.size(); ++i) {
      std::memcpy(raw_data + raw_offset[i], data[i].c_str(), data[i].length());
      raw_offset[i + 1] = raw_offset[i] + data[i].length();
    }
  }

  int64_t null_count = 0;
  if (!not_null.empty()) {
    auto not_null_data = batch->getMutableNulls();
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      if (!not_null[i]) {
        CiderBitUtils::clearBitAt(not_null_data, i);
        ++null_count;
      }
    }
    batch->setNullCount(null_count);
  }

  EXPECT_TRUE(batch->resizeBatch(2 * data.size()));
  EXPECT_EQ(batch->getLength(), 2 * data.size());
  EXPECT_EQ(batch->getNullCount(),
            not_null.empty() ? 0 : null_count + (int64_t)data.size());

  {
    auto raw_data = batch->getRawData();
    auto raw_offset = batch->getMutableRawOffset();
    EXPECT_NE(raw_data, nullptr);
    EXPECT_NE(raw_offset, nullptr);
    for (size_t i = 0; i < data.size(); ++i) {
      EXPECT_EQ(0,
                std::memcmp(raw_data + raw_offset[i], data[i].c_str(), data[i].length()));
    }
  }

  if (!not_null.empty()) {
    auto not_null_data = batch->getNulls();
    EXPECT_NE(not_null_data, nullptr);
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i), not_null[i]);
    }
    for (size_t i = 0; i < not_null.size(); ++i) {
      EXPECT_EQ(CiderBitUtils::isBitSetAt(not_null_data, i + not_null.size()), false);
    }
  }
}

TEST_F(CiderArrowBatchTest, VarcharBatchNoNullTest) {
  SQLTypeInfo type_info(kVARCHAR, true);
  ArrowSchema* schema = convertCiderTypeInfoToArrowSchema(type_info);

  auto batch = VarcharBatch::Create(schema, ciderAllocator);
  EXPECT_TRUE(batch->isRootOwner());

  std::vector<string> data{"a", "b", "c", "d"};
  std::vector<int> offset{0, 1, 2, 3, 4};
  runVarcharBatchTest(batch.get(), data, offset);
}

TEST_F(CiderArrowBatchTest, VarcharBatchNullTest) {
  SQLTypeInfo type_info(kVARCHAR, true);
  ArrowSchema* schema = convertCiderTypeInfoToArrowSchema(type_info);

  auto batch = VarcharBatch::Create(schema, ciderAllocator);
  EXPECT_TRUE(batch->isRootOwner());

  std::vector<string> data{"a", "b", "c", "d"};
  std::vector<int> offset{0, 1, 2, 3, 4};
  std::vector<bool> null{true, false, true, false};
  runVarcharBatchTest(batch.get(), data, offset, null);
}

TEST_F(CiderArrowBatchTest, StructBatchTest) {
  {
    SQLTypeInfo type(
        kSTRUCT,
        false,
        {SQLTypeInfo(kBOOLEAN, true),
         SQLTypeInfo(kTINYINT, false),
         SQLTypeInfo(kSMALLINT, true),
         SQLTypeInfo(kINT, false),
         SQLTypeInfo(kBIGINT, true),
         SQLTypeInfo(kFLOAT, false),
         SQLTypeInfo(kDOUBLE, true),
         SQLTypeInfo(
             kSTRUCT, true, {SQLTypeInfo(kBIGINT, false), SQLTypeInfo(kFLOAT, true)})});

    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, ciderAllocator);

    EXPECT_TRUE(batch->isRootOwner());
    EXPECT_TRUE(batch->resizeBatch(10));
    EXPECT_EQ(batch->getLength(), 10);
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_EQ(batch->getChildrenNum(), 8);

    {
      auto child = batch->getChildAt(0);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<kBOOLEAN>(child->asMutable<ScalarBatch<bool>>(),
                                   {true, true, false, false, false, true});
    }
    {
      auto child = batch->getChildAt(1);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<int8_t, kTINYINT>(child->asMutable<ScalarBatch<int8_t>>(),
                                           {1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
                                           {
                                               true,
                                               true,
                                               true,
                                               false,
                                               false,
                                               false,
                                               true,
                                               true,
                                               true,
                                               false,
                                           });
    }
    {
      auto child = batch->getChildAt(2);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<int16_t, kSMALLINT>(child->asMutable<ScalarBatch<int16_t>>(),
                                             {199, 21, 33, 44, 55, 26, 17, 68, 19, 30});
    }
    {
      auto child = batch->getChildAt(3);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<int32_t, kINT>(
          child->asMutable<ScalarBatch<int32_t>>(),
          {1994, 221, 333, 144, 255, 2426, 117, 6328, 11239, 3110},
          {
              true,
              true,
              false,
              false,
              true,
              false,
              true,
              true,
              false,
              true,
          });
    }
    {
      auto child = batch->getChildAt(4);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<int64_t, kBIGINT>(
          child->asMutable<ScalarBatch<int64_t>>(),
          {19944, 2321, 12333, 13444, 212355, 2231426, 13417, 46328, 151239, 23110});
    }
    {
      auto child = batch->getChildAt(5);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<float, kFLOAT>(child->asMutable<ScalarBatch<float>>(),
                                        {1.0, 2.0, 3.0, 4.0, 5.0},
                                        {true, true, false, true, true});
    }
    {
      auto child = batch->getChildAt(6);
      EXPECT_FALSE(child->isRootOwner());
      runScalarBatchTest<double, kDOUBLE>(child->asMutable<ScalarBatch<double>>(),
                                          {1.0, 2.0, 3.0, 4.0, 5.0});
    }
    {
      auto child = batch->getChildAt(7);
      EXPECT_FALSE(child->isRootOwner());
      EXPECT_TRUE(child->resizeBatch(10));
      EXPECT_EQ(child->getLength(), 10);
      EXPECT_EQ(child->getNullCount(), 0);
      EXPECT_EQ(child->getChildrenNum(), 2);

      {
        auto child1 = child->getChildAt(0);
        EXPECT_FALSE(child1->isRootOwner());
        runScalarBatchTest<int64_t, kBIGINT>(
            child1->asMutable<ScalarBatch<int64_t>>(),
            {19944, 2321, 12333, 13444, 212355, 2231426, 13417, 46328, 151239, 23110},
            {
                true,
                false,
                true,
                true,
                false,
                false,
                true,
                false,
                true,
                true,
            });
      }

      {
        auto child2 = child->getChildAt(1);
        EXPECT_FALSE(child2->isRootOwner());
        runScalarBatchTest<float, kFLOAT>(child2->asMutable<ScalarBatch<float>>(),
                                          {1.0, 2.0, 3.0, 4.0, 5.0});
      }
    }
  }
  {
    SQLTypeInfo type(kSTRUCT, true);

    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, ciderAllocator);

    EXPECT_TRUE(batch->isRootOwner());
    EXPECT_TRUE(batch->resizeBatch(10));
    EXPECT_EQ(batch->getLength(), 10);
    EXPECT_EQ(batch->getNullCount(), 0);
    EXPECT_EQ(batch->getChildrenNum(), 0);
  }
}

TEST_F(CiderArrowBatchTest, CopyFuncTest) {
  auto type = SQLTypeInfo(
      kSTRUCT, true, {SQLTypeInfo(kBIGINT, false), SQLTypeInfo(kFLOAT, true)});

  {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, ciderAllocator);
    {
      StructBatch copy_batch(*batch);
      EXPECT_FALSE(copy_batch.isRootOwner());
      EXPECT_EQ(copy_batch.getChildrenNum(), batch->getChildrenNum());

      copy_batch.resizeBatch(10);
      EXPECT_EQ(copy_batch.getLength(), 10);
      EXPECT_EQ(copy_batch.getNullCount(), 0);

      auto child1 = copy_batch.getChildAt(0);
      auto child2 = copy_batch.getChildAt(1);
      child1->resizeBatch(10);
      EXPECT_EQ(child1->getLength(), 10);
      child2->resizeBatch(10);
      EXPECT_EQ(child2->getLength(), 10);

      {
        auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getMutableRawData();
        for (size_t i = 0; i < 10; ++i) {
          ptr[i] = (int64_t)i;
        }
      }
      {
        auto ptr = child2->asMutable<ScalarBatch<float>>()->getMutableRawData();
        for (size_t i = 0; i < 10; ++i) {
          ptr[i] = (float)i;
        }
      }
    }

    EXPECT_TRUE(batch->isRootOwner());
    EXPECT_FALSE(batch->isMoved());
    auto child1 = batch->getChildAt(0);
    EXPECT_EQ(child1->getLength(), 10);
    auto child2 = batch->getChildAt(1);
    EXPECT_EQ(child2->getLength(), 10);
    {
      auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getRawData();
      for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(ptr[i], (int64_t)i);
      }
    }
    {
      auto ptr = child2->asMutable<ScalarBatch<float>>()->getRawData();
      for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(ptr[i], (float)i);
      }
    }
  }
  {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, ciderAllocator);
    {
      auto type1 = SQLTypeInfo(kSTRUCT, true);
      auto schema1 = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type1);
      StructBatch copy_batch(schema1, ciderAllocator);

      copy_batch.resizeBatch(100);
      copy_batch = *batch;

      EXPECT_FALSE(copy_batch.isRootOwner());
      EXPECT_EQ(copy_batch.getChildrenNum(), batch->getChildrenNum());

      copy_batch.resizeBatch(10);
      EXPECT_EQ(copy_batch.getLength(), 10);
      EXPECT_EQ(copy_batch.getNullCount(), 0);

      auto child1 = copy_batch.getChildAt(0);
      auto child2 = copy_batch.getChildAt(1);
      child1->resizeBatch(10);
      EXPECT_EQ(child1->getLength(), 10);
      child2->resizeBatch(10);
      EXPECT_EQ(child2->getLength(), 10);

      {
        auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getMutableRawData();
        for (size_t i = 0; i < 10; ++i) {
          ptr[i] = (int64_t)i;
        }
      }
      {
        auto ptr = child2->asMutable<ScalarBatch<float>>()->getMutableRawData();
        for (size_t i = 0; i < 10; ++i) {
          ptr[i] = (float)i;
        }
      }
    }

    EXPECT_TRUE(batch->isRootOwner());
    EXPECT_FALSE(batch->isMoved());
    auto child1 = batch->getChildAt(0);
    EXPECT_EQ(child1->getLength(), 10);
    auto child2 = batch->getChildAt(1);
    EXPECT_EQ(child2->getLength(), 10);
    {
      auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getRawData();
      for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(ptr[i], (int64_t)i);
      }
    }
    {
      auto ptr = child2->asMutable<ScalarBatch<float>>()->getRawData();
      for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(ptr[i], (float)i);
      }
    }
  }
}

TEST_F(CiderArrowBatchTest, MoveFuncTest) {
  auto type1 = SQLTypeInfo(
      kSTRUCT, true, {SQLTypeInfo(kBIGINT, true), SQLTypeInfo(kFLOAT, false)});
  auto type2 = SQLTypeInfo(kSTRUCT, false, {SQLTypeInfo(kINT, false)});
  {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type2);
    auto batch = StructBatch::Create(schema, ciderAllocator);
    {
      batch->resizeBatch(10);
      auto child = batch->getChildAt(0);
      child->resizeBatch(10);
      EXPECT_EQ(child->getLength(), 10);
      auto ptr = child->asMutable<ScalarBatch<int32_t>>()->getMutableRawData();
      for (size_t i = 0; i < 10; ++i) {
        ptr[i] = (int32_t)i;
      }
    }
    {
      auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type1);
      StructBatch move_batch(schema, ciderAllocator);
      move_batch = std::move(*batch);
      EXPECT_FALSE(batch->isRootOwner());
      EXPECT_TRUE(batch->isMoved());

      EXPECT_EQ(move_batch.getChildrenNum(), 1);
      EXPECT_EQ(move_batch.getLength(), 10);
      {
        auto child = move_batch.getChildAt(0);
        auto ptr = child->asMutable<ScalarBatch<int32_t>>()->getMutableRawData();
        for (size_t i = 0; i < 10; ++i) {
          EXPECT_EQ(ptr[i], (int32_t)i);
        }
      }
    }
  }
  {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type1);
    auto batch = StructBatch::Create(schema, ciderAllocator);
    batch->resizeBatch(100);
    EXPECT_EQ(batch->getChildrenNum(), 2);
    {
      auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type2);
      batch = StructBatch::Create(schema, ciderAllocator);
      EXPECT_TRUE(batch->isRootOwner());
      EXPECT_FALSE(batch->isMoved());
      EXPECT_EQ(batch->getChildrenNum(), 1);
      EXPECT_EQ(batch->getLength(), 0);
      batch->resizeBatch(1000);
      EXPECT_EQ(batch->getLength(), 1000);
    }
  }
  {
    auto schema1 = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type1);
    StructBatch batch(std::move(*StructBatch::Create(schema1, ciderAllocator)));
    EXPECT_TRUE(batch.isRootOwner());
    EXPECT_FALSE(batch.isMoved());
    EXPECT_EQ(batch.getChildrenNum(), 2);
    batch.resizeBatch(1000);
    EXPECT_EQ(batch.getLength(), 1000);
  }
}

TEST_F(CiderArrowBatchTest, ArrowEntryMoveTest) {
  SQLTypeInfo type(
      kSTRUCT, false, {SQLTypeInfo(kBIGINT, false), SQLTypeInfo(kFLOAT, false)});
  {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, ciderAllocator);
    batch->resizeBatch(100);

    auto child1 = batch->getChildAt(0);
    child1->resizeBatch(100);
    auto child2 = batch->getChildAt(1);
    child2->resizeBatch(100);
    {
      auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getMutableRawData();
      for (size_t i = 0; i < 100; ++i) {
        ptr[i] = (int64_t)i;
      }
    }
    {
      auto ptr = child2->asMutable<ScalarBatch<float>>()->getMutableRawData();
      for (size_t i = 0; i < 100; ++i) {
        ptr[i] = (float)i;
      }
    }

    auto [moved_schema, moved_array] = batch->move();
    EXPECT_TRUE(batch->isMoved());
    batch.reset();

    auto new_batch = StructBatch::Create(moved_schema, ciderAllocator, moved_array);
    EXPECT_TRUE(new_batch->isRootOwner());
    EXPECT_FALSE(new_batch->isMoved());

    EXPECT_FALSE(new_batch->resizeBatch(1000));
    EXPECT_EQ(new_batch->getLength(), 100);

    {
      auto child1 = new_batch->getChildAt(0);
      auto child2 = new_batch->getChildAt(1);
      {
        auto ptr = child1->asMutable<ScalarBatch<int64_t>>()->getMutableRawData();
        for (size_t i = 0; i < 100; ++i) {
          CHECK_EQ(ptr[i], (int64_t)i);
        }
      }
      {
        auto ptr = child2->asMutable<ScalarBatch<float>>()->getMutableRawData();
        for (size_t i = 0; i < 100; ++i) {
          CHECK_EQ(ptr[i], (float)i);
        }
      }
    }
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
