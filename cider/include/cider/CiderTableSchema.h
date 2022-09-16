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

#ifndef CIDER_CIDERTABLESCHEMA_H
#define CIDER_CIDERTABLESCHEMA_H

#include <util/Logger.h>
#include <string>
#include <vector>
#include "substrait/type.pb.h"

enum ColumnHint { Normal, PartialAVG };

class CiderTableSchema {
 public:
  explicit CiderTableSchema(std::vector<std::string> columnNames = {},
                            std::vector<substrait::Type> columnTypes = {},
                            std::string tableName = "",
                            std::vector<ColumnHint> col_hints = {})
      : columnNames_(columnNames)
      , columnTypes_(columnTypes)
      , tableName_(tableName)
      , col_hints_(col_hints) {
    CHECK_EQ(columnNames_.size(), columnTypes_.size());
    totalColumnNum_ = columnNames_.size();
    if (!col_hints_.size()) {
      col_hints_ = std::vector<ColumnHint>{columnNames_.size(), ColumnHint::Normal};
    }
    // flatten column types
    for (auto colType : columnTypes_) {
      flattenColumnTypes(colType);
    }
  }

  ~CiderTableSchema() = default;

  std::vector<std::string> getColumnNames() const { return columnNames_; }

  std::string getColumnNameById(const int column) const {
    CHECK(columnNames_.size() > 0 && column < columnNames_.size());
    return columnNames_[column];
  }

  std::vector<substrait::Type> getColumnTypes() const { return columnTypes_; }

  substrait::Type getColumnTypeById(const int column) const {
    CHECK(columnTypes_.size() > 0 && column < columnTypes_.size());
    return columnTypes_[column];
  }

  std::vector<substrait::Type> getFlattenColumnTypes() const {
    return flattenColumnTypes_;
  }

  int getFlattenColIndex(const int col) {
    int flattenIndex = 0;
    for (int i = 0; i < col; i++) {
      flattenIndex += getColsNum(columnTypes_[i], 0);
    }
    return flattenIndex;
  }

  void setColumn(std::string columnName, substrait::Type columnType) {
    columnNames_.push_back(columnName);
    columnTypes_.push_back(columnType);
  }

  void setTableName(std::string tableName) { tableName_ = tableName; }

  std::string getTableName() { return tableName_; }

  int getColumnCount() const { return totalColumnNum_; }

  std::vector<ColumnHint> getColHints() const { return col_hints_; }

  int GetColumnTypeSize(const int column) const {
    auto type_kind = columnTypes_[column].kind_case();
    switch (type_kind) {
      case substrait::Type::kBool:
      case substrait::Type::kI8:
        return 1;
      case substrait::Type::kI16:
        return 2;
      case substrait::Type::kI32:
      case substrait::Type::kFp32:
        return 4;
      case substrait::Type::kI64:
      case substrait::Type::kFp64:
      case substrait::Type::kDate:
      case substrait::Type::kTime:
      case substrait::Type::kTimestamp:
        return 8;
      case substrait::Type::kVarchar:
      case substrait::Type::kString:
      case substrait::Type::kFixedChar:
        return 16;  // sizeof(CiderByteArray)
      default:
        // TODO: handle unsupport type
        // throw std::runtime_error("not supported type: " + type_kind);
        return 8;
    }
  }

 private:
  void flattenColumnTypes(substrait::Type& columnType) {
    if (columnType.has_struct_()) {
      for (auto type : columnType.struct_().types()) {
        flattenColumnTypes(type);
      }
    } else {
      flattenColumnTypes_.push_back(columnType);
    }
  }

  int getColsNum(substrait::Type& columnType, int cur_size) {
    if (columnType.has_struct_()) {
      for (auto type : columnType.struct_().types()) {
        cur_size += getColsNum(type, 0);
      }
      return cur_size;
    } else {
      return cur_size + 1;
    }
  }

  std::vector<std::string> columnNames_;
  std::vector<substrait::Type> columnTypes_;
  std::vector<substrait::Type> flattenColumnTypes_;
  std::vector<bool> isUniqued;
  std::string tableName_;
  //  Hints of the col data, indicating it is the result of avg_partial, or belongs to a
  //  nested data type, etc.
  std::vector<ColumnHint> col_hints_;
  int totalColumnNum_;
};

#endif  // CIDER_CIDERTABLESCHEMA_H
