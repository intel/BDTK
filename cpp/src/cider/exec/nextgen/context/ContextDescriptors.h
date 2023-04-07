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
#ifndef NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_H
#define NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_H

#include "exec/nextgen/context/ContextDescriptors-fwd.h"

#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/Buffer.h"
#include "exec/nextgen/context/CiderSet.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/operator/join/CiderJoinHashTable.h"

#include "exec/nextgen/utils/TypeUtils.h"
#include "type/data/sqltypes.h"
#include "util/sqldefs.h"

namespace cider::exec::nextgen::context {

struct BatchDescriptor {
  int64_t ctx_id;
  std::string name;
  SQLTypeInfo type;

  BatchDescriptor(int64_t id, const std::string& n, const SQLTypeInfo& t)
      : ctx_id(id), name(n), type(t) {}
};

struct BufferDescriptor {
  int64_t ctx_id;
  std::string name;
  // SQLTypeInfo type;
  int32_t capacity;

  BufferInitializer initializer_;

  BufferDescriptor(int64_t id,
                   const std::string& n,
                   int32_t c,
                   const BufferInitializer& initializer)
      : ctx_id(id), name(n), capacity(c), initializer_(initializer) {}

  virtual ~BufferDescriptor() = default;
};

struct CrossJoinBuildTableDescriptor {
  int64_t ctx_id;
  std::string name;
  BatchSharedPtr build_table;

  CrossJoinBuildTableDescriptor(int64_t id,
                                const std::string& n,
                                BatchSharedPtr table = std::make_shared<Batch>())
      : ctx_id(id), name(n), build_table(table) {}
};

struct CiderSetDescriptor {
  int64_t ctx_id;
  std::string name;
  SQLTypeInfo type;
  CiderSetPtr cider_set;
  CiderSetDescriptor(int64_t id,
                     const std::string& n,
                     const SQLTypeInfo& t,
                     CiderSetPtr c_set)
      : ctx_id(id), name(n), type(t), cider_set(std::move(c_set)) {}
};

struct AggExprsInfo {
 public:
  SQLTypeInfo sql_type_info_;
  jitlib::JITTypeTag jit_value_type_;
  SQLAgg agg_type_;
  int8_t start_offset_;
  int8_t null_offset_;
  std::string agg_name_;
  bool is_partial_;

  AggExprsInfo(SQLTypeInfo sql_type_info,
               SQLTypeInfo arg_sql_type_info,
               SQLAgg agg_type,
               int8_t start_offset,
               bool is_partial = false)
      : sql_type_info_(sql_type_info)
      , jit_value_type_(utils::getJITTypeTag(sql_type_info_.get_type()))
      , agg_type_(agg_type)
      , start_offset_(start_offset)
      , null_offset_(-1)
      , agg_name_(
            getAggName(agg_type, sql_type_info_.get_type(), arg_sql_type_info.get_type()))
      , is_partial_(is_partial) {}

  void setNotNull(bool n) {
    // true -- not null, flase -- nullable
    sql_type_info_.set_notnull(n);
  }

 private:
  std::string getAggName(SQLAgg agg_type, SQLTypes sql_type, SQLTypes arg_sql_type);
};

using AggExprsInfoVector = std::vector<AggExprsInfo>;

struct AggBufferDescriptor : public BufferDescriptor {
  std::vector<AggExprsInfo> info_;
  AggBufferDescriptor(int64_t id,
                      const std::string& n,
                      int32_t c,
                      const BufferInitializer& initializer,
                      const std::vector<AggExprsInfo>& info)
      : BufferDescriptor(id, n, c, initializer), info_(info) {}
};

struct HashTableDescriptor {
  int64_t ctx_id;
  std::string name;
  processor::JoinHashTablePtr hash_table;

  HashTableDescriptor(
      int64_t id,
      const std::string& n,
      processor::JoinHashTablePtr table = std::make_shared<processor::JoinHashTable>())
      : ctx_id(id), name(n), hash_table(table) {}
};
};  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_H
