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

#ifndef CIDER_CIDERBATCHBUILDER_H
#define CIDER_CIDERBATCHBUILDER_H

#include "cider/CiderBatch.h"
#include "cider/CiderTypes.h"
#include "exec/plan/parser/TypeUtils.h"
#include "substrait/type.pb.h"
#include "util/Logger.h"

class CiderBatchBuilder {
 public:
  CiderBatchBuilder()
      : row_num_(0)
      , is_row_num_set_(false)
      , allocator_(std::make_shared<CiderDefaultAllocator>()) {}

  CiderBatchBuilder& setRowNum(int row_num) {
    if (is_row_num_set_) {  // have set before, throw exception
      CIDER_THROW(CiderCompileException, "row num have been set!");
    }
    is_row_num_set_ = true;
    row_num_ = row_num;
    return *this;
  }

  CiderBatchBuilder& setTableName(const std::string& table_name) {
    table_name_ = table_name;
    return *this;
  }

  template <class T>
  CiderBatchBuilder& addColumn(const std::string& col_name,
                               const ::substrait::Type& col_type,
                               const std::vector<T>& col_data,
                               const std::vector<bool>& null_data = {}) {
    col_names_.push_back(col_name);
    col_types_.push_back(col_type);
    null_vecs_.push_back(null_data);
    if (!is_row_num_set_ ||  // have not set row num, use this col_data's row num
        row_num_ == 0) {     // previous columns are all empty
      is_row_num_set_ = true;
      row_num_ = col_data.size();
    }
    int8_t* buf = nullptr;
    if (col_data.empty()) {
      // append an empty buffer.
    } else {
      // check row num
      if (row_num_ != col_data.size()) {
        CIDER_THROW(CiderCompileException, "Row num is not equal to previous columns!");
      }
      CHECK_EQ(row_num_, col_data.size());
      // check null data num
      if (!null_data.empty()) {
        CHECK_EQ(row_num_, null_data.size());
      }
      buf = allocator_->allocate(sizeof(T) * row_num_);
      std::memcpy(buf, col_data.data(), sizeof(T) * row_num_);
    }
    table_ptr_.push_back(buf);

    return *this;
  }

  template <typename T>
  CiderBatchBuilder& addTimingColumn(const std::string& col_name,
                               const ::substrait::Type& col_type,
                               const std::vector<T>& col_data,
                               const std::vector<bool>& null_data = {}) {
    col_names_.push_back(col_name);
    col_types_.push_back(col_type);
    null_vecs_.push_back(null_data);
    if (!is_row_num_set_ ||  // have not set row num, use this col_data's row num
        row_num_ == 0) {     // previous columns are all empty
      is_row_num_set_ = true;
      row_num_ = col_data.size();
    }
    int8_t* buf = nullptr;
    if (col_data.empty()) {
      // append an empty buffer
    } else {
      // check row num
      if (row_num_ != col_data.size()) {
        CIDER_THROW(CiderCompileException, "Row num is not equal to previous columns!");
      }
      CHECK_EQ(row_num_, col_data.size());
      // check null data num
      if (!null_data.empty()) {
        CHECK_EQ(row_num_, null_data.size());
      }
      buf = allocator_->allocate(sizeof(int64_t) * row_num_);
      int64_t* dump = reinterpret_cast<int64_t*>(buf);
      for (auto i = 0; i < row_num_; i++) {
        dump[i] = col_data[i].getInt64Val();
      }
    }
    table_ptr_.push_back(buf);
    return *this;
  }

  template <typename T,
            std::enable_if_t<std::is_same<T, CiderByteArray>::value, bool> = true>
  CiderBatchBuilder& addColumn(const std::string& col_name,
                               const ::substrait::Type& col_type,
                               const std::vector<CiderByteArray>& col_data,
                               const std::vector<bool>& null_data = {}) {
    col_names_.push_back(col_name);
    col_types_.push_back(col_type);
    null_vecs_.push_back(null_data);
    if (!is_row_num_set_ ||  // have not set row num, use this col_data's row num
        row_num_ == 0) {     // previous columns are all empty
      is_row_num_set_ = true;
      row_num_ = col_data.size();
    }
    int8_t* buf = nullptr;
    if (!col_data.empty()) {
      // check row num
      if (row_num_ != col_data.size()) {
        CIDER_THROW(CiderCompileException, "Row num is not equal to previous columns!");
      }
      CHECK_EQ(row_num_, col_data.size());
      // check null data num
      if (!null_data.empty()) {
        CHECK_EQ(row_num_, null_data.size());
      }
      int len = sizeof(CiderByteArray) * row_num_;
      buf = allocator_->allocate(len);
      std::memcpy(buf, col_data.data(), len);
    }
    table_ptr_.push_back(buf);
    return *this;
  }

  CiderBatch build() {
    if (!is_row_num_set_) {
      CIDER_THROW(CiderCompileException, "Invalid build!");
    }
    std::shared_ptr<CiderTableSchema> schema =
        std::make_shared<CiderTableSchema>(col_names_, col_types_, table_name_);
    CiderBatch batch = CiderBatch(row_num_, schema);
    for (int i = 0; i < table_ptr_.size(); i++) {
      if (table_ptr_[i]) {
        std::memcpy(const_cast<int8_t*>(batch.column(i)),
                    table_ptr_[i],
                    batch.column_type_size(i) * row_num_);
        std::free(const_cast<int8_t*>(table_ptr_[i]));
      }
    }
    batch.set_null_vecs(null_vecs_);
    return batch;
  }

 private:
  size_t row_num_;
  bool is_row_num_set_;
  std::vector<const int8_t*> table_ptr_;
  std::vector<std::string> col_names_;
  std::vector<::substrait::Type> col_types_;
  std::string table_name_ = "";
  std::vector<std::vector<bool>> null_vecs_;  // mark null data when builder adds columns
  std::shared_ptr<CiderAllocator> allocator_;
};

#endif  // CIDER_CIDERBATCHBUILDER_H
