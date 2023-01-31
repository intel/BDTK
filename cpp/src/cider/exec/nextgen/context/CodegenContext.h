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
#ifndef NEXTGEN_CONTEXT_CODEGENCONTEXT_H
#define NEXTGEN_CONTEXT_CODEGENCONTEXT_H

#include "common/interpreters/AggregationHashTable.h"
#include "exec/nextgen/context/Buffer.h"
#include "exec/nextgen/context/CiderSet.h"
#include "exec/nextgen/jitlib/base/JITModule.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "exec/nextgen/utils/TypeUtils.h"
#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#include "exec/operator/join/CiderJoinHashTable.h"
#endif
#include "util/sqldefs.h"

namespace cider::exec::nextgen::context {

class RuntimeContext;
class Batch;
using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;
using namespace cider_hashtable;

struct AggExprsInfo {
 public:
  SQLTypeInfo sql_type_info_;
  jitlib::JITTypeTag jit_value_type_;
  SQLAgg agg_type_;
  int8_t start_offset_;
  int8_t null_offset_;
  std::string agg_name_;

  AggExprsInfo(SQLTypeInfo sql_type_info, SQLAgg agg_type, int8_t start_offset)
      : sql_type_info_(sql_type_info)
      , jit_value_type_(utils::getJITTypeTag(sql_type_info_.get_type()))
      , agg_type_(agg_type)
      , start_offset_(start_offset)
      , null_offset_(-1)
      , agg_name_(getAggName(agg_type, sql_type_info_.get_type())) {}

  void setNotNull(bool n) {
    // true -- not null, flase -- nullable
    sql_type_info_.set_notnull(n);
  }

 private:
  std::string getAggName(SQLAgg agg_type, SQLTypes sql_type);
};

using AggExprsInfoVector = std::vector<AggExprsInfo>;

struct CodegenOptions {
  bool needs_error_check = false;
  bool check_bit_vector_clear_opt = false;
  bool set_null_bit_vector_opt = false;
  bool branchless_logic = false;

  jitlib::CompilationOptions co = jitlib::CompilationOptions{};
};

class CodegenContext {
 public:
  CodegenContext() : jit_func_(nullptr) {}

  void setJITFunction(jitlib::JITFunctionPointer jit_func) {
    CHECK(nullptr == jit_func_);
    jit_func_ = jit_func;
  }

  jitlib::JITFunctionPointer getJITFunction() { return jit_func_; }

  CodegenOptions getCodegenOptions() { return codegen_options_; }

  std::pair<jitlib::JITValuePointer, utils::JITExprValue>& getArrowArrayValues(
      size_t local_offset) {
    return arrow_array_values_[local_offset - 1];
  }

  template <typename ValuesT>
  size_t appendArrowArrayValues(jitlib::JITValuePointer& arrow_array, ValuesT&& values) {
    arrow_array_values_.emplace_back(arrow_array.get(), std::forward<ValuesT>(values));
    return arrow_array_values_.size();
  }

  jitlib::JITValuePointer registerBatch(const SQLTypeInfo& type,
                                        const std::string& name = "",
                                        bool arrow_array_output = true);

  // TBD: HashTable (GroupBy, Join), other objects registration.
  jitlib::JITValuePointer registerBuffer(
      const int32_t capacity,
      const std::string& name = "",
      const BufferInitializer& initializer =
          [](Buffer* buf) { memset(buf->getBuffer(), 0, buf->getCapacity()); },
      bool output_raw_buffer = true);

  jitlib::JITValuePointer registerBuffer(
      const int32_t capacity,
      const std::vector<AggExprsInfo>& info,
      const std::string& name = "",
      const BufferInitializer& initializer =
          [](Buffer* buf) { memset(buf->getBuffer(), 0, buf->getCapacity()); },
      bool output_raw_buffer = true);

  jitlib::JITValuePointer registerHashTable(const std::string& name = "");
  jitlib::JITValuePointer registerCiderSet(const std::string& name,
                                           const SQLTypeInfo& type,
                                           CiderSetPtr c_set);

  RuntimeCtxPtr generateRuntimeCTX(const CiderAllocatorPtr& allocator) const;

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
    // TODO(qiuyang) : LinearProbeHashTable will provide the default template
    cider::exec::processor::JoinHashTable* hash_table;

    HashTableDescriptor(
        int64_t id,
        const std::string& n,
        // TODO(qiuyang) : LinearProbeHashTable will provide the default template
        cider::exec::processor::JoinHashTable* table =
            new cider::exec::processor::JoinHashTable())
        : ctx_id(id), name(n), hash_table(table) {}
  };

  void setHashTable(cider::exec::processor::JoinHashTable* join_hash_table) {
    hashtable_descriptor_.first->hash_table = join_hash_table;
  }

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

  void setJITModule(jitlib::JITModulePointer jit_module) { jit_module_ = jit_module; }

  void setCodegenOptions(CodegenOptions codegen_options) {
    codegen_options_ = codegen_options;
  }

  using BatchDescriptorPtr = std::shared_ptr<BatchDescriptor>;
  using BufferDescriptorPtr = std::shared_ptr<BufferDescriptor>;
  using HashTableDescriptorPtr = std::shared_ptr<HashTableDescriptor>;
  using CiderSetDescriptorPtr = std::shared_ptr<CiderSetDescriptor>;
  using TrimCharMapsPtr = std::shared_ptr<std::vector<std::vector<int8_t>>>;

  // registers a set of trim characters for TrimStringOper, to be used at runtime
  // returns an index used for retrieving the charset at runtime
  int registerTrimStringOperCharMap(const std::string& trim_chars);

 private:
  std::vector<std::pair<BatchDescriptorPtr, jitlib::JITValuePointer>>
      batch_descriptors_{};
  std::vector<std::pair<BufferDescriptorPtr, jitlib::JITValuePointer>>
      buffer_descriptors_{};
  std::pair<HashTableDescriptorPtr, jitlib::JITValuePointer> hashtable_descriptor_;
  std::vector<std::pair<CiderSetDescriptorPtr, jitlib::JITValuePointer>>
      cider_set_descriptors_{};
  std::vector<std::pair<jitlib::JITValuePointer, utils::JITExprValue>>
      arrow_array_values_{};

  jitlib::JITFunctionPointer jit_func_;
  int64_t id_counter_{0};
  jitlib::JITModulePointer jit_module_;
  CodegenOptions codegen_options_;

  int64_t acquireContextID() { return id_counter_++; }
  int64_t getNextContextID() const { return id_counter_; }
  jitlib::JITValuePointer getBufferContentPtr(
      int64_t id,
      bool output_raw_buffer = false,
      const std::string& raw_buffer_func_name = "");

  // use shared_ptr here to avoid copying the entire 2d vector when creating runtime ctx
  TrimCharMapsPtr trim_char_maps_;
};

using CodegenCtxPtr = std::unique_ptr<CodegenContext>;

namespace codegen_utils {
jitlib::JITValuePointer getArrowArrayLength(jitlib::JITValuePointer& arrow_array);

void setArrowArrayLength(jitlib::JITValuePointer& arrow_array,
                         jitlib::JITValuePointer& len);

jitlib::JITValuePointer getArrowArrayBuffer(jitlib::JITValuePointer& arrow_array,
                                            int64_t index);

jitlib::JITValuePointer getArrowArrayChild(jitlib::JITValuePointer& arrow_array,
                                           int64_t index);

jitlib::JITValuePointer allocateArrowArrayBuffer(jitlib::JITValuePointer& arrow_array,
                                                 int64_t index,
                                                 jitlib::JITValuePointer& bytes);
}  // namespace codegen_utils
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CODEGENCONTEXT_H
