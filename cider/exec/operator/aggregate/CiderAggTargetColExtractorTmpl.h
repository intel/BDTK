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

#ifndef CIDER_CIDERAGGTARGETCOLEXTRACTORTMPL_H
#define CIDER_CIDERAGGTARGETCOLEXTRACTORTMPL_H

#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "util/SqlTypesLayout.h"

#include "exec/operator/aggregate/CiderAggHashTable.h"
#include "exec/operator/aggregate/CiderAggTargetColExtractor.h"
#include "util/CiderBitUtils.h"

struct DecimalPlaceHolder {};
struct VarCharPlaceHolder {};

template <typename ST, typename TT>
class SimpleAggExtractor : public CiderAggTargetColExtractor {
 public:
  SimpleAggExtractor(const std::string& name,
                     size_t colIndex,
                     const CiderAggHashTable* hashtable)
      : CiderAggTargetColExtractor(name, colIndex) {
    auto& colInfo = hashtable->getColEntryInfo(colIndex);
    offset_ = colInfo.slot_offset;
    null_offset_ = colInfo.is_key ? hashtable->getKeyNullVectorOffset()
                                  : hashtable->getTargetNullVectorOffset();
    index_in_null_vector_ =
        colInfo.is_key ? col_index_ : col_index_ - hashtable->getKeyColNum();
    null_ = !colInfo.sql_type_info.get_notnull();
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, int8_t* outAddrs) override {
    size_t rowNum = rowAddrs.size();

    TT* targetVector = reinterpret_cast<TT*>(outAddrs);

    for (size_t i = 0; i < rowNum; ++i) {
      const int8_t* rowPtr = rowAddrs[i];
      targetVector[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
    }
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, CiderBatch* output) override {
    size_t rowNum = rowAddrs.size();
    auto scalarOutput = output->asMutable<ScalarBatch<TT>>();

    CHECK(scalarOutput->resizeBatch(rowNum, true));
    TT* buffer = scalarOutput->getMutableRawData();
    uint8_t* nulls = null_ ? scalarOutput->getMutableNulls() : nullptr;

    if (nulls) {
      int64_t null_count = 0;
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        if (!CiderBitUtils::isBitSetAt(
                reinterpret_cast<const uint8_t*>(rowPtr + null_offset_),
                index_in_null_vector_)) {
          CiderBitUtils::clearBitAt(nulls, i);
          ++null_count;
        } else {
          buffer[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
        }
      }
      output->setNullCount(null_count);
    } else {
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        buffer[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
      }
    }
  }

 private:
  size_t offset_;
  size_t index_in_null_vector_;
};

template <typename ST>
class SimpleAggExtractor<ST, VarCharPlaceHolder> : public CiderAggTargetColExtractor {
 public:
  SimpleAggExtractor(const std::string& name,
                     size_t colIndex,
                     const CiderAggHashTable* hashtable)
      : CiderAggTargetColExtractor(name, colIndex), hasher_(&hashtable->getHasher()) {
    auto& colInfo = hashtable->getColEntryInfo(colIndex);
    offset_ = colInfo.slot_offset;
    null_offset_ = colInfo.is_key ? hashtable->getKeyNullVectorOffset()
                                  : hashtable->getTargetNullVectorOffset();
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, int8_t* outAddrs) override {
    size_t rowNum = rowAddrs.size();

    CiderByteArray* targetVector = reinterpret_cast<CiderByteArray*>(outAddrs);

    for (size_t i = 0; i < rowNum; ++i) {
      const int8_t* rowPtr = rowAddrs[i];
      const ST* id = reinterpret_cast<const ST*>(rowPtr + offset_);
      CiderByteArray raw_str = hasher_->lookupValueById(*id);
      // Old CiderBatch can't manage memory of VarChar type properly, there will exist
      // memory leak problem.
      targetVector[i].len = raw_str.len;
      if (raw_str.len) {
        uint8_t* output_buffer = new uint8_t[raw_str.len];
        std::memcpy(output_buffer, raw_str.ptr, raw_str.len);
        targetVector[i].ptr = output_buffer;
      }
    }
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, CiderBatch* output) override {
    UNREACHABLE();
  }

 private:
  size_t offset_;
  const CiderHasher* hasher_;
};

template <typename SUMT, typename COUNTT, typename AVGT>
class AVGAggExtractor : public CiderAggTargetColExtractor {
 public:
  AVGAggExtractor(const std::string& name,
                  size_t colIndex,
                  const CiderAggHashTable* hashtable)
      : CiderAggTargetColExtractor(name, colIndex) {
    auto& sumColInfo = hashtable->getColEntryInfo(colIndex);
    auto& countColInfo = hashtable->getColEntryInfo(colIndex + 1);

    sum_offset_ = sumColInfo.slot_offset;
    count_offset_ = countColInfo.slot_offset;
    null_ = !sumColInfo.sql_type_info.get_notnull();
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, int8_t* outAddr) override {
    size_t rowNum = rowAddrs.size();

    AVGT* avgResultVector = reinterpret_cast<AVGT*>(outAddr);
    for (size_t i = 0; i < rowNum; ++i) {
      const int8_t* rowPtr = rowAddrs[i];

      AVGT sumVal = *reinterpret_cast<const SUMT*>(rowPtr + sum_offset_);
      int64_t countVal = *reinterpret_cast<const COUNTT*>(rowPtr + count_offset_);
      avgResultVector[i] = sumVal / countVal;
    }
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, CiderBatch* output) override {
    size_t rowNum = rowAddrs.size();
    auto scalarOutput = output->asMutable<ScalarBatch<AVGT>>();

    CHECK(scalarOutput->resizeBatch(rowNum, true));
    AVGT* buffer = scalarOutput->getMutableRawData();
    uint8_t* nulls = null_ ? scalarOutput->getMutableNulls() : nullptr;

    if (nulls) {
      int64_t null_count = 0;
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];

        AVGT sumVal = *reinterpret_cast<const SUMT*>(rowPtr + sum_offset_);
        int64_t countVal = *reinterpret_cast<const COUNTT*>(rowPtr + count_offset_);

        if (countVal) {
          buffer[i] = sumVal / countVal;
        } else {
          CiderBitUtils::clearBitAt(nulls, i);
          ++null_count;
        }
      }
      output->setNullCount(null_count);
    } else {
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];

        AVGT sumVal = *reinterpret_cast<const SUMT*>(rowPtr + sum_offset_);
        int64_t countVal = *reinterpret_cast<const COUNTT*>(rowPtr + count_offset_);
        buffer[i] = sumVal / countVal;
      }
    }
  }

 private:
  size_t sum_offset_;
  size_t count_offset_;
};

template <typename COUNTT, typename AVGT>
class AVGAggExtractor<DecimalPlaceHolder, COUNTT, AVGT>
    : public CiderAggTargetColExtractor {
 public:
  AVGAggExtractor(const std::string& name,
                  size_t colIndex,
                  const CiderAggHashTable* hashtable)
      : CiderAggTargetColExtractor(name, colIndex) {
    auto& sumColInfo = hashtable->getColEntryInfo(colIndex);
    auto& countColInfo = hashtable->getColEntryInfo(colIndex + 1);

    sum_offset_ = sumColInfo.slot_offset;
    count_offset_ = countColInfo.slot_offset;
    scale_ = exp_to_scale(sumColInfo.arg_type_info.get_scale());
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, int8_t* outAddr) override {
    size_t rowNum = rowAddrs.size();

    AVGT* avgResultVector = reinterpret_cast<AVGT*>(outAddr);
    for (size_t i = 0; i < rowNum; ++i) {
      const int8_t* rowPtr = rowAddrs[i];

      AVGT sumVal = static_cast<double>(
          *reinterpret_cast<const int64_t*>(rowPtr + sum_offset_) / scale_);
      int64_t countVal = *reinterpret_cast<const COUNTT*>(rowPtr + count_offset_);
      avgResultVector[i] = sumVal / countVal;
    }
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, CiderBatch* output) override {
    abort();
  }

 private:
  size_t sum_offset_;
  size_t count_offset_;
  size_t scale_;
};

template <typename ST, typename TT>
class CountAggExtractor : public CiderAggTargetColExtractor {
 public:
  CountAggExtractor(const std::string& name,
                    size_t colIndex,
                    const CiderAggHashTable* hashtable)
      : CiderAggTargetColExtractor(name, colIndex) {
    auto& colInfo = hashtable->getColEntryInfo(colIndex);
    offset_ = colInfo.slot_offset;
    null_offset_ = colInfo.is_key ? hashtable->getKeyNullVectorOffset()
                                  : hashtable->getTargetNullVectorOffset();
    index_in_null_vector_ =
        colInfo.is_key ? col_index_ : col_index_ - hashtable->getKeyColNum();
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, int8_t* outAddrs) override {
    size_t rowNum = rowAddrs.size();

    TT* targetVector = reinterpret_cast<TT*>(outAddrs);

    for (size_t i = 0; i < rowNum; ++i) {
      const int8_t* rowPtr = rowAddrs[i];
      targetVector[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
    }
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, CiderBatch* output) override {
    size_t rowNum = rowAddrs.size();
    auto scalarOutput = output->asMutable<ScalarBatch<TT>>();

    CHECK(scalarOutput->resizeBatch(rowNum, true));
    TT* buffer = scalarOutput->getMutableRawData();
    uint8_t* nulls = scalarOutput->getMutableNulls();

    if (nulls) {
      int64_t null_count = 0;
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        auto value = *reinterpret_cast<const ST*>(rowPtr + offset_);
        if (!value) {
          CiderBitUtils::clearBitAt(nulls, i);
          ++null_count;
        } else {
          buffer[i] = value;
        }
      }
      output->setNullCount(null_count);
    } else {
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        buffer[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
      }
    }
  }

 private:
  size_t offset_;
  size_t index_in_null_vector_;
};

#endif
