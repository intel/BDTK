/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#pragma once

#include "type/data/sqltypes.h"
#include "util/toString.h"

#include <memory>
#include <unordered_set>

struct ColumnRef {
  ColumnRef(int db_id_, int table_id_, int column_id_)
      : db_id(db_id_), table_id(table_id_), column_id(column_id_) {}

  int db_id;
  int table_id;
  int column_id;

  bool operator==(const ColumnRef& other) const {
    return column_id == other.column_id && table_id == other.table_id &&
           db_id == other.db_id;
  }

  std::string toString() const;
};

using ColumnRefSet = std::unordered_set<ColumnRef>;

struct ColumnInfo : public ColumnRef {
  ColumnInfo(int db_id,
             int table_id,
             int column_id,
             const std::string name_,
             SQLTypeInfo type_,
             bool is_rowid_)
      : ColumnRef(db_id, table_id, column_id)
      , name(name_)
      , type(type_)
      , is_rowid(is_rowid_) {}

  std::string name;
  SQLTypeInfo type;
  // Virtual rowid column.
  bool is_rowid;

  std::string toString() const;
};

using ColumnInfoPtr = std::shared_ptr<ColumnInfo>;
using ColumnInfoList = std::vector<ColumnInfoPtr>;
using ColumnInfoMap = std::unordered_map<ColumnRef, ColumnInfoPtr>;

namespace std {

template <>
struct hash<ColumnRef> {
  size_t operator()(const ColumnRef& col) const {
    return col.db_id ^ col.table_id ^ col.column_id;
  }
};

}  // namespace std
