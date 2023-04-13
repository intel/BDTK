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
#include "type/data/sqltypes.h"

namespace cider::exec::nextgen::context {

void Batch::reset(const SQLTypeInfo& type,
                  const CiderAllocatorPtr& allocator,
                  const ArrowArray* input_array,
                  const ArrowSchema* input_schema) {
  release();

  auto builder = utils::RecursiveFunctor{[&allocator, &input_schema, &input_array, this](
                                             auto&& builder,
                                             ArrowSchema* schema,
                                             ArrowArray* array,
                                             const SQLTypeInfo& info,
                                             int child_level) -> void {
    CiderArrowSchemaBufferHolder* holder =
        new CiderArrowSchemaBufferHolder(info.getChildrenNum(),
                                         false);  // TODO: Dictionary support is TBD;
    schema->format =
        CiderBatchUtils::convertCiderTypeToArrowType(info, holder->getFormatBuffer());
    schema->n_children = info.getChildrenNum();
    schema->children = holder->getChildrenPtrs();
    schema->dictionary = holder->getDictPtr();
    schema->release = CiderBatchUtils::ciderArrowSchemaReleaser;
    schema->private_data = holder;

    // array
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
      if (input_schema && input_array && child_level == 0 &&
          bare_output_input_map_.count(i)) {
        // Caution:
        // for some input columns, we copy child array/schema content and manage it's
        // buffers, dictionary.
        auto input_col_id = bare_output_input_map_[i];
        *schema->children[i] = *input_schema->children[input_col_id];
        *array->children[i] = *input_array->children[input_col_id];

        // make input release not release this child
        input_schema->children[input_col_id] = nullptr;
        // we set input_array children to nullptr in LazyNode (we
        // can't do it here cause query_func may read these field)
        continue;
      }
      builder(
          schema->children[i], array->children[i], info.getChildAt(i), child_level + 1);
    }
  }};

  builder(&schema_, &array_, type, 0);
}
}  // namespace cider::exec::nextgen::context
