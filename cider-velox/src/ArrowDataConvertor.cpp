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

#include "ArrowDataConvertor.h"
#include <memory>
#include "ArrowConvertorUtils.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

ArrowDataConvertor::ArrowDataConvertor(std::shared_ptr<CiderAllocator> allocator)
    : allocator_(allocator) {}

int8_t* toCiderWithArrow(VectorPtr& child,
                         int idx,
                         int num_rows,
                         std::shared_ptr<CiderAllocator> allocator) {
  // velox to arrow
  ArrowArray arrowArray;
  exportToArrow(child, arrowArray);
  ArrowSchema arrowSchema;
  exportToArrow(child, arrowSchema);
  // arrow to cider
  int8_t* column = convertToCider(arrowSchema, arrowArray, num_rows, allocator);
  arrowArray.release(&arrowArray);
  arrowSchema.release(&arrowSchema);
  return column;
}

CiderBatch ArrowDataConvertor::convertToCider(RowVectorPtr input,
                                              int num_rows,
                                              std::chrono::microseconds* timer) {
  RowVector* row = input.get();
  auto* rowVector = row->as<RowVector>();
  auto size = rowVector->childrenSize();
  std::vector<const int8_t*> table_ptr;
  // auto col_buffer_ptr = &col_buffer;
  for (auto idx = 0; idx < size; idx++) {
    VectorPtr& child = rowVector->childAt(idx);
    switch (child->encoding()) {
      case VectorEncoding::Simple::FLAT:
        table_ptr.push_back(toCiderWithArrow(child, idx, num_rows, allocator_));
        break;
      case VectorEncoding::Simple::LAZY: {
        // For LazyVector, we will load it here and use as TypeVector to use.
        auto tic = std::chrono::system_clock::now();
        auto vec = (std::dynamic_pointer_cast<LazyVector>(child))->loadedVectorShared();
        auto toc = std::chrono::system_clock::now();
        if (timer) {
          *timer += std::chrono::duration_cast<std::chrono::microseconds>(toc - tic);
        }
        table_ptr.push_back(toCiderWithArrow(vec, idx, num_rows, allocator_));
        break;
      }
      default:
        VELOX_NYI(" {} conversion is not supported yet", child->encoding());
    }
  }
  return CiderBatch(num_rows, table_ptr);
}

VectorPtr toVeloxVectorWithArrow(ArrowArray& arrowArray,
                                 ArrowSchema& arrowSchema,
                                 const int8_t* data_buffer,
                                 ::substrait::Type col_type,
                                 int num_rows,
                                 memory::MemoryPool* pool,
                                 std::shared_ptr<CiderAllocator> allocator,
                                 int32_t dimen = 0) {
  convertToArrow(arrowArray, arrowSchema, data_buffer, col_type, num_rows, allocator);
  auto result = importFromArrowAsViewer(arrowSchema, arrowArray, pool);
  return result;
}

RowVectorPtr ArrowDataConvertor::convertToRowVector(const CiderBatch& input,
                                                    const CiderTableSchema& schema,
                                                    memory::MemoryPool* pool) {
  std::shared_ptr<const RowType> rowType;
  std::vector<VectorPtr> columns;
  std::vector<TypePtr> types;
  std::vector<std::string> col_names = schema.getColumnNames();
  int num_rows = input.row_num();
  int num_cols = schema.getColumnCount();
  types.reserve(num_cols);
  columns.reserve(num_cols);
  for (int i = 0; i < num_cols; i++) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    columns.push_back(toVeloxVectorWithArrow(arrowArray,
                                             arrowSchema,
                                             input.column(i),
                                             schema.getColumnTypeById(i),
                                             num_rows,
                                             pool,
                                             allocator_));
    types.push_back(importFromArrow(arrowSchema));
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
}
}  // namespace facebook::velox::plugin
