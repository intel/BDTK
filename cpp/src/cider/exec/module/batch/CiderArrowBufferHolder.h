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

#ifndef CIDER_ARROW_BUFFER_HOLDER_H
#define CIDER_ARROW_BUFFER_HOLDER_H

#include <vector>

#include "cider/CiderAllocator.h"

struct ArrowSchema;
struct ArrowArray;

class CiderArrowArrayBufferHolder {
 public:
  CiderArrowArrayBufferHolder(size_t buffer_num,
                              size_t children_num,
                              std::shared_ptr<CiderAllocator> allocator,
                              bool dict);
  ~CiderArrowArrayBufferHolder();

  const void** getBufferPtrs() { return const_cast<const void**>(buffers_.data()); }

  template <typename T>
  T* getBufferAs(size_t index) {
    return reinterpret_cast<T*>(buffers_[index]);
  }

  // (re-) Allocate the buffer.
  void allocBuffer(size_t index, size_t bytes);

  ArrowArray** getChildrenPtrs() { return children_ptr_.data(); }

  ArrowArray* getDictPtr();

  size_t getBufferSizeAt(size_t index);

 private:
  void releaseBuffer(size_t index);

  std::vector<void*> buffers_;
  std::vector<size_t> buffers_bytes_;  // Used for allocator.
  std::vector<ArrowArray*> children_ptr_;
  std::vector<ArrowArray> children_and_dict_;
  std::shared_ptr<CiderAllocator> allocator_;
  const bool has_dict_;
};

class CiderArrowSchemaBufferHolder {
 public:
  CiderArrowSchemaBufferHolder(size_t children_num, bool dict);

  ArrowSchema** getChildrenPtrs() { return children_ptr_.data(); }

  ArrowSchema* getDictPtr();

  // Buffer required to generate a decimal format.
  std::string& getFormatBuffer() { return format_buffer_; }

 private:
  std::vector<ArrowSchema*> children_ptr_;
  std::vector<ArrowSchema> children_and_dict_;
  const bool has_dict_;
  std::string format_buffer_;
};

#endif
