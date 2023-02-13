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

#ifndef NEXTGEN_AGG_EXTRACTOR_H
#define NEXTGEN_AGG_EXTRACTOR_H

#include "util/CiderBitUtils.h"
#include "util/sqldefs.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"

namespace cider::exec::nextgen::operators {
class NextgenAggExtractor {
 public:
  explicit NextgenAggExtractor(const std::string& name) : name_(name) {}

  virtual void extract(const std::vector<const int8_t*>&, ArrowArray*) = 0;

  std::string getName() { return name_; }

  virtual ~NextgenAggExtractor() = default;

 protected:
  int8_t null_offset_;
  bool is_nullable_;
  const std::string name_;
};

template <typename ST, typename TT>
class NextgenBasicAggExtractor : public NextgenAggExtractor {
 public:
  NextgenBasicAggExtractor(const std::string& name,
                           const int8_t* buffer,
                           context::AggExprsInfo& info)
      : NextgenAggExtractor(name) {
    offset_ = info.start_offset_;
    null_offset_ = info.null_offset_;
    is_nullable_ = !info.sql_type_info_.get_notnull();
  }

  void extract(const std::vector<const int8_t*>& rowAddrs, ArrowArray* output) override {
    size_t rowNum = rowAddrs.size();
    void** no_const_buffer = const_cast<void**>(output->buffers);

    uint8_t* null_buffer = reinterpret_cast<uint8_t*>(no_const_buffer[0]);
    TT* buffer = reinterpret_cast<TT*>(no_const_buffer[1]);

    if (is_nullable_) {
      int64_t null_count_num = 0;
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        if (*reinterpret_cast<const bool*>(rowPtr + null_offset_)) {
          CiderBitUtils::clearBitAt(null_buffer, i);
          ++null_count_num;
        } else {
          // avoid precision loss
          if (this->getName() == "FLOAT_DOUBLE") {
            buffer[i] =
                std::stod(std::to_string(*reinterpret_cast<const ST*>(rowPtr + offset_)));
          } else {
            buffer[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
          }
        }
      }
      output->null_count = null_count_num;
    } else {
      for (size_t i = 0; i < rowNum; ++i) {
        const int8_t* rowPtr = rowAddrs[i];
        // avoid precision loss
        if (this->getName() == "FLOAT_DOUBLE") {
          buffer[i] =
              std::stod(std::to_string(*reinterpret_cast<const ST*>(rowPtr + offset_)));
        } else {
          buffer[i] = *reinterpret_cast<const ST*>(rowPtr + offset_);
        }
      }
    }
  }

 private:
  size_t offset_;
  size_t index_in_null_vector_;
};
}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_AGG_EXTRACTOR_H
