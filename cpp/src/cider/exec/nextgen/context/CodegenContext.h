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
#ifndef NEXTGEN_CONTEXT_CODEGENCONTEXT_H
#define NEXTGEN_CONTEXT_CODEGENCONTEXT_H

#include <memory>

#include "exec/nextgen/context/ContextDescriptors-fwd.h"
#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/base/JITModule.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/Options.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "include/cider/CiderOptions.h"
#include "type/data/sqltypes.h"

class CiderAllocator;
class SQLTypeInfo;
using CiderAllocatorPtr = std::shared_ptr<CiderAllocator>;

namespace cider::exec::nextgen::context {

class RuntimeContext;
using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;

struct FilterDescriptor {
  bool applied_filter_mask{false};
  jitlib::JITValuePointer filter_i64_mask{nullptr};
  jitlib::JITValuePointer start_index{nullptr};
};

class CodegenContext {
 public:
  CodegenContext() : jit_func_(nullptr) {}

  // we can reuse the compiled func, but create RuntimeCtx for every driver.
  RuntimeCtxPtr generateRuntimeCTX(const CiderAllocatorPtr& allocator) const;

  // bare column map
  // output column id to input column id;
  std::unordered_map<int, int> bare_output_input_map_;

  void setJITFunction(jitlib::JITFunctionPointer& jit_func) {
    CHECK(nullptr == jit_func_);
    jit_func_ = jit_func;
  }

  jitlib::JITFunctionPointer getJITFunction() { return jit_func_; }

  // FIXME (bigPYJ1151)
  [[deprecated]] void setJITModule(jitlib::JITModulePointer jit_module) {
    jit_module_ = std::move(jit_module);
  }

  void setFilterMask(jitlib::JITValuePointer& mask, jitlib::JITValuePointer& start_index);

  bool isAppliedFilterMask() { return filter_desc_.applied_filter_mask; }

  std::pair<jitlib::JITValuePointer&, jitlib::JITValuePointer&> getFilterMask() {
    return {filter_desc_.filter_i64_mask, filter_desc_.start_index};
  }

  template <typename FuncT>
  void appendDeferFunc(FuncT&& func) {
    defer_func_list_.emplace_back(func);
  }

  void clearDeferFunc() { defer_func_list_.clear(); }

  const std::vector<std::function<void()>>& getDeferFunc() const {
    return defer_func_list_;
  }

  void setInputLength(jitlib::JITValuePointer& len) { input_len_.replace(len); }

  jitlib::JITValuePointer& getInputLength() { return input_len_; }

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

  void setOutputBatch(jitlib::JITValuePointer& array_array) {
    output_arrow_array_.replace(array_array);
    return;
  }

  jitlib::JITValuePointer& getOutputBatch() { return output_arrow_array_; }

  jitlib::JITValuePointer registerStringHeap();

  // TBD: HashTable (GroupBy, Join), other objects registration.
  jitlib::JITValuePointer registerBuffer(
      const int32_t capacity,
      const std::string& name = "",
      const BufferInitializer& initializer = [](Buffer* buf) {},
      bool output_raw_buffer = true);

  jitlib::JITValuePointer registerBuffer(
      const int32_t capacity,
      const std::vector<AggExprsInfo>& info,
      const std::string& name = "",
      const BufferInitializer& initializer = [](Buffer* buf) {},
      bool output_raw_buffer = true);

  jitlib::JITValuePointer registerHashTable(const std::string& name = "");
  jitlib::JITValuePointer registerBuildTable(const std::string& name = "");
  jitlib::JITValuePointer registerCiderSet(const std::string& name,
                                           const SQLTypeInfo& type,
                                           CiderSetPtr& c_set);

  void setHashTable(const std::shared_ptr<processor::JoinHashTable>& join_hash_table);

  void setBuildTable(BatchSharedPtr& batch);
  void setCodegenOptions(CodegenOptions codegen_options) {
    codegen_options_ = codegen_options;
  }

  void setHasOuterJoin(bool has_outer_join) { has_outer_join_ = has_outer_join; }

  bool getHasOuterJoin() { return has_outer_join_; }

  // registers a set of trim characters for TrimStringOper, to be used at runtime
  // returns an index used for retrieving the charset at runtime
  int registerTrimStringOperCharMap(const std::string& trim_chars);

 private:
  int64_t acquireContextID() { return id_counter_++; }
  int64_t getNextContextID() const { return id_counter_; }
  jitlib::JITValuePointer getBufferContentPtr(
      int64_t id,
      bool output_raw_buffer = false,
      const std::string& raw_buffer_func_name = "");

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
  jitlib::JITValuePointer output_arrow_array_;
  std::pair<BuildTableDescriptorPtr, jitlib::JITValuePointer> buildtable_descriptor_;

  jitlib::JITFunctionPointer jit_func_;
  jitlib::JITModulePointer jit_module_;
  jitlib::JITValuePointer input_len_{nullptr};
  FilterDescriptor filter_desc_;
  std::vector<std::function<void()>> defer_func_list_{};

  int64_t id_counter_{0};
  CodegenOptions codegen_options_;
  bool has_outer_join_ = false;

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

void setArrowArrayChild(jitlib::JITValuePointer& arrow_array,
                        int64_t index,
                        jitlib::JITValuePointer& child_array);
void clearArrowArrayChild(jitlib::JITValuePointer& arrow_array, int64_t index);
void copyArrowArrayChild(jitlib::JITValuePointer& arrow_array,
                         int64_t index,
                         jitlib::JITValuePointer& child_array);

jitlib::JITValuePointer getArrowArrayDictionary(jitlib::JITValuePointer& arrow_array);

jitlib::JITValuePointer allocateArrowArrayBuffer(jitlib::JITValuePointer& arrow_array,
                                                 int64_t index,
                                                 jitlib::JITValuePointer& bytes);

jitlib::JITValuePointer allocateArrowArrayBuffer(jitlib::JITValuePointer& arrow_array,
                                                 int64_t index,
                                                 jitlib::JITValuePointer& len,
                                                 SQLTypes type);

void bitBufferMemcpy(jitlib::JITValuePointer& dst,
                     jitlib::JITValuePointer& src,
                     jitlib::JITValuePointer& bit_num);

void bitBufferAnd(jitlib::JITValuePointer& output,
                  jitlib::JITValuePointer& a,
                  jitlib::JITValuePointer& b,
                  jitlib::JITValuePointer& bit_num);

void convertByteBoolToBit(jitlib::JITValuePointer& byte,
                          jitlib::JITValuePointer& bit,
                          jitlib::JITValuePointer& len);
}  // namespace codegen_utils
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CODEGENCONTEXT_H
