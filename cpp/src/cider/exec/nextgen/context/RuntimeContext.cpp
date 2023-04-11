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

#include "exec/nextgen/context/RuntimeContext.h"

#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/context/ContextDescriptors.h"
#include "exec/nextgen/context/StringHeap.h"
#include "exec/nextgen/operators/extractor/AggExtractorBuilder.h"
#include "exec/nextgen/utils/FunctorUtils.h"
#include "util/ArrowArrayBuilder.h"

namespace cider::exec::nextgen::context {
void RuntimeContext::addBatch(const BatchDescriptorPtr& descriptor) {
  batch_holder_.emplace_back(descriptor, nullptr);
}

void RuntimeContext::addBuffer(const BufferDescriptorPtr& descriptor) {
  buffer_holder_.emplace_back(descriptor, nullptr);
}

void RuntimeContext::addHashTable(const HashTableDescriptorPtr& descriptor) {
  hashtable_holder_ = descriptor;
}

void RuntimeContext::addBuildTable(const BuildTableDescriptorPtr& descriptor) {
  buildtable_holder_ = descriptor;
}

void RuntimeContext::addCiderSet(const CiderSetDescriptorPtr& descriptor) {
  cider_set_holder_.emplace_back(descriptor, nullptr);
}

void RuntimeContext::instantiate(const CiderAllocatorPtr& allocator) {
  // Instantiation of batches.
  for (auto& batch_desc : batch_holder_) {
    if (nullptr == batch_desc.second) {
      batch_desc.second = std::make_unique<Batch>(batch_desc.first->type, allocator);
      runtime_ctx_pointers_[batch_desc.first->ctx_id] = batch_desc.second.get();
    }
  }

  // Instantiation of buffers.
  for (auto& buffer_desc : buffer_holder_) {
    if (nullptr == buffer_desc.second) {
      buffer_desc.second = std::make_unique<Buffer>(
          buffer_desc.first->capacity, allocator, buffer_desc.first->initializer_);
      runtime_ctx_pointers_[buffer_desc.first->ctx_id] = buffer_desc.second.get();
    }
  }

  // Instantiation of hashtable.
  if (hashtable_holder_ != nullptr) {
    runtime_ctx_pointers_[hashtable_holder_->ctx_id] =
        hashtable_holder_->hash_table.get();
  }

  // Instantiation of buildtable.
  if (buildtable_holder_ != nullptr) {
    runtime_ctx_pointers_[buildtable_holder_->ctx_id] =
        buildtable_holder_->build_table.get();
  }

  string_heap_ptr_ = std::make_shared<StringHeap>(allocator);

  for (auto& cider_set_desc : cider_set_holder_) {
    if (nullptr == cider_set_desc.second) {
      runtime_ctx_pointers_[cider_set_desc.first->ctx_id] =
          cider_set_desc.first->cider_set.get();
    }
  }
}

// TODO: batch and buffer should be self-managed
void RuntimeContext::resetBatch(const CiderAllocatorPtr& allocator) {
  if (!batch_holder_.empty()) {
    auto& [descriptor, batch] = batch_holder_.front();
    batch->reset(descriptor->type, allocator);
  }
}

void RuntimeContext::destroyStringHeap() {
  string_heap_ptr_->destroy();
}

void allocateBatchMem(ArrowArray* array,
                      int64_t length,
                      bool is_struct = true,
                      int64_t value_size = 0) {
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);
  // null buffer (size rounded up)
  holder->allocBuffer(0, (length + 7) / 8);
  auto child_null_buffer = holder->getBufferAs<int8_t>(0);
  memset(child_null_buffer, 0xFF, (length + 7) / 8);

  // value buffer
  if (!is_struct) {
    holder->allocBuffer(1, value_size * length);
  }

  // set length
  array->length = length;
}

Batch* RuntimeContext::getNonGroupByAggOutputBatch() {
  AggExprsInfoVector& info =
      reinterpret_cast<AggBufferDescriptor*>(buffer_holder_.back().first.get())->info_;
  int8_t* buf = buffer_holder_.back().second->getBuffer();
  Batch* batch = batch_holder_.front().second.get();

  // allocate mem
  // row struct
  auto arrow_array = batch->getArray();
  auto arrow_schema = batch->getSchema();
  allocateBatchMem(arrow_array, 1);

  // child value
  for (size_t i = 0; i < arrow_array->n_children; i++) {
    auto child_array = arrow_array->children[i];
    allocateBatchMem(child_array, 1, false, info[i].sql_type_info_.get_size());
  }

  for (size_t i = 0; i < info.size(); ++i) {
    operators::NextgenAggExtractorBuilder::buildNextgenAggExtractor(buf, info[i])
        ->extract({buf}, arrow_array->children[i]);
  }

  // for partial avg, recombinate the sum&count columns as a new struct column
  auto partial_avg_count = std::count_if(
      info.begin(), info.end(), [](const AggExprsInfo i) { return i.is_partial_; });
  // assert sum&count in partial avg appear in pairs and adjoin
  if (partial_avg_count > 0 && partial_avg_count % 2 == 0) {
    ArrowArrayBuilder builder;
    auto row_num = arrow_array->length;
    builder.setRowNum(row_num);
    for (auto i = 0; i < info.size(); i++) {
      if (info[i].is_partial_ && info[i + 1].is_partial_) {
        auto schema_and_array =
            ArrowArrayBuilder()
                .setRowNum(row_num)
                .addStructColumn(arrow_schema->children[i], arrow_array->children[i])
                .addStructColumn(arrow_schema->children[i + 1],
                                 arrow_array->children[i + 1])
                .build();
        i++;
        builder.addStructColumn(std::get<0>(schema_and_array),
                                std::get<1>(schema_and_array));
      } else {
        builder.addStructColumn(arrow_schema->children[i], arrow_array->children[i]);
      }
    }
    auto schema_and_array = builder.build();
    batch = new Batch(*std::get<0>(schema_and_array), *std::get<1>(schema_and_array));
  }

  return batch;
}

Batch* RuntimeContext::getOutputBatch() {
  if (batch_holder_.empty()) {
    return nullptr;
  }
  auto batch = batch_holder_.front().second.get();
  auto arrow_array = batch->getArray();
  // FIXME (bigPYJ1151): This is a workaround for output struct array length setting.
  arrow_array->length = arrow_array->children[0]->length;
  auto arrow_schema = batch->getSchema();

  auto set_null_count_function =
      utils::RecursiveFunctor{[](auto&& set_null_count_function,
                                 ArrowArray* arrow_array,
                                 ArrowSchema* arrow_schema) -> void {
        if (arrow_array->buffers[0]) {
          auto length = arrow_array->length;
          arrow_array->null_count =
              length -
              CiderBitUtils::countSetBits(
                  reinterpret_cast<const uint8_t*>(arrow_array->buffers[0]), length);
        }
        for (size_t i = 0; i < arrow_schema->n_children; ++i) {
          set_null_count_function(arrow_array->children[i], arrow_schema->children[i]);
        }
      }};
  set_null_count_function(arrow_array, arrow_schema);
  return batch;
}

void RuntimeContext::setTrimStringOperCharMaps(const TrimCharMapsPtr& maps) {
  trim_char_maps_ = maps;
}

const int8_t* RuntimeContext::getTrimStringOperCharMapById(int id) const {
  return trim_char_maps_->at(id).data();
}

}  // namespace cider::exec::nextgen::context
