/*
 * Copyright (c) 2022 Intel Corporation.
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
#ifndef QUERYENGINE_INVALUESBITMAP_H
#define QUERYENGINE_INVALUESBITMAP_H

#include <llvm/IR/Value.h>
#include "util/memory/BufferProvider.h"

#include <cstdint>
#include <stdexcept>
#include <vector>

class Executor;

class InValuesBitmap {
 public:
  InValuesBitmap(const std::vector<int64_t>& values,
                 const int64_t null_val,
                 const Data_Namespace::MemoryLevel memory_level,
                 const int device_count,
                 BufferProvider* buffer_provider);
  ~InValuesBitmap();

  llvm::Value* codegen(llvm::Value* needle, Executor* executor) const;

  std::unique_ptr<CodegenColValues> codegen(llvm::Value* needle,
                                            llvm::Value* null,
                                            Executor* executor) const;

  bool isEmpty() const;

  bool hasNull() const;

 private:
  std::vector<int8_t*> bitsets_;
  bool rhs_has_null_;
  int64_t min_val_;
  int64_t max_val_;
  const int64_t null_val_;
  const Data_Namespace::MemoryLevel memory_level_;
  const int device_count_;
  BufferProvider* buffer_provider_;
};

#endif  // QUERYENGINE_INVALUESBITMAP_H
