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
#include "exec/nextgen/context/Batch.h"

#include <functional>

#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/utils/FunctorUtils.h"

namespace cider::exec::nextgen::context {

void Batch::reset(const SQLTypeInfo& type, const CiderAllocatorPtr& allocator) {
  release();

  schema_ = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);

  auto builder = utils::RecursiveFunctor{
      [&allocator](auto&& builder, ArrowSchema* schema, ArrowArray* array) -> void {
        array->length = 0;
        array->null_count = 0;
        array->offset = 0;

        array->n_buffers = CiderBatchUtils::getBufferNum(schema);
        array->n_children = schema->n_children;
        CiderArrowArrayBufferHolder* root_holder = new CiderArrowArrayBufferHolder(
            array->n_buffers, schema->n_children, allocator, schema->dictionary);
        array->buffers = root_holder->getBufferPtrs();
        array->children = root_holder->getChildrenPtrs();
        array->dictionary = root_holder->getDictPtr();
        array->private_data = root_holder;
        array->release = CiderBatchUtils::ciderArrowArrayReleaser;

        for (size_t i = 0; i < schema->n_children; ++i) {
          builder(schema->children[i], array->children[i]);
        }
      }};

  builder(&schema_, &array_);
}
}  // namespace cider::exec::nextgen::context
