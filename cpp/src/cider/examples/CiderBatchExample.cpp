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

#include <iostream>
#include <memory>
#include "cider/CiderAllocator.h"
#include "cider/CiderBatch.h"
#include "cider/CiderTableSchema.h"
#include "exec/plan/parser/TypeUtils.h"

class CiderUserAllocator : public CiderAllocator {
 public:
  CiderUserAllocator() {}

  CiderUserAllocator(const CiderUserAllocator&) {}

  int8_t* allocate(size_t buffer_size) override {
    int8_t* t = (int8_t*)malloc(buffer_size);
    std::cout << "Use CiderUserAllocator to allocate at address " << static_cast<void*>(t)
              << " with size " << buffer_size << std::endl;
    return t;
  }

  void deallocate(int8_t* p, size_t buffer_size) override {
    if (p) {
      free(p);
      std::cout << "Use CiderUserAllocator to deallocate at address "
                << static_cast<void*>(p) << " wiht size " << buffer_size << std::endl;
    }
  }
};

CiderBatch TestCiderBatchMoveAssignment() {
  std::vector<size_t> column_size{4, 8, 2, 1, 4};
  auto alloc = std::make_shared<CiderUserAllocator>();
  CiderBatch column_batch(10, column_size, alloc);
  std::cout << column_batch.toString() << std::endl;
  return column_batch;
}

int main(int argc, char** argv) {
  constexpr uint32_t kNumRows = 10;
  constexpr uint32_t kRowSize = 19;
  std::vector<size_t> column_size{4, 8, 2, 1, 4};
  std::vector<std::string> column_names{"c0", "c1", "c2", "c3", "c4"};
  std::vector<::substrait::Type> column_types{CREATE_SUBSTRAIT_TYPE(I32),
                                              CREATE_SUBSTRAIT_TYPE(Fp64),
                                              CREATE_SUBSTRAIT_TYPE(I16),
                                              CREATE_SUBSTRAIT_TYPE(Bool),
                                              CREATE_SUBSTRAIT_TYPE(Fp32)};
  std::shared_ptr<CiderTableSchema> schema =
      std::make_shared<CiderTableSchema>(column_names, column_types);

  auto alloc = std::make_shared<CiderUserAllocator>();
  {
    // Test for row based CiderBatch
    std::cout << "Test for row batch:" << std::endl;
    CiderBatch row_batch(kNumRows, kRowSize, alloc);
    std::cout << row_batch.toString() << std::endl;
  }
  std::cout << "Row batch memory lifecycle end, should see deallocate\n" << std::endl;

  {
    std::cout << "Test for align allocator:" << std::endl;
    auto alignAlloc = std::make_shared<AlignAllocator<64>>(alloc);
    int8_t* p = alignAlloc->allocate(kRowSize);
    p[0] = 9;
    std::cout << "Alloc address: " << static_cast<void*>(p)
              << ", value: " << int32_t(p[0]) << std::endl;
    alignAlloc->deallocate(p, kRowSize);
  }

  // Test for row based CiderBatch with zero row size
  std::cout << "Test for row batch with zero row size: shouldn't see allocate"
            << std::endl;
  CiderBatch row_batch_with_zero_row_size(kNumRows, 0, alloc);
  std::cout << row_batch_with_zero_row_size.toString() << std::endl;

  // Test for row based CiderBatch with zero row num
  std::cout << "Test for row batch with zero num rows: shouldn't see allocate"
            << std::endl;
  CiderBatch row_batch_with_zero_num_rows(0, kRowSize, alloc);
  std::cout << row_batch_with_zero_num_rows.toString() << std::endl;

  {
    // Test for column based CiderBatch, get column size from vector
    std::cout << "Test for column batch:" << std::endl;
    CiderBatch column_batch_with_size(kNumRows, column_size, alloc);
    std::cout << column_batch_with_size.toString() << std::endl;
  }
  std::cout << "Column batch memory lifecycle end, should see deallocate\n" << std::endl;

  // Test for column based CiderBatch, get column size from vector
  std::cout << "Test for column batch:" << std::endl;
  CiderBatch column_batch_with_size(kNumRows, column_size, alloc);
  std::cout << column_batch_with_size.toString() << std::endl;
  std::cout << "Column batch memory lifecycle will be end when exit example program, "
               "shouldn't see deallocate now\n"
            << std::endl;

  // Test for column based CiderBatch, get column size from schema
  std::cout << "Test for column batch with schema:" << std::endl;
  CiderBatch column_batch_with_schema(kNumRows, schema, alloc);
  std::cout << column_batch_with_schema.toString() << std::endl;
  std::cout << "Column batch memory lifecycle will be end when exit example program, "
               "shouldn't see deallocate now\n"
            << std::endl;

  std::cout << "Test for move assignment return from function:" << std::endl;
  const auto& move_batch = TestCiderBatchMoveAssignment();
  std::cout << move_batch.toString() << std::endl;
  std::cout << "Column batch memory lifecycle will be end when exit example program, "
               "shouldn't see deallocate now\n"
            << std::endl;

  // Test for empty CiderBatch
  std::cout << "Test for empty batch:" << std::endl;
  CiderBatch empty_batch = CiderBatch();
  std::cout << empty_batch.toString() << std::endl;

  std::cout << "Test for empty batch resize" << std::endl;
  CiderBatch resize_empty_batch;
  std::cout << "before empty batch resize empty:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(10, 19);
  std::cout << "after empty batch resize empty:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::cout << "before empty batch resize capacity:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(20, 19);
  std::cout << "after empty batch resize capacity:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::cout << "before empty batch resize capacity: should no allocate" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(10, 19);
  std::cout << "after empty batch resize capacity: should no allocate" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::cout << "before empty batch resize row size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(20, 29);
  std::cout << "after empty batch resize row size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::cout << "before empty batch resize row size: should no allocate" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(20, 19);
  std::cout << "after empty batch resize row size: should no allocate" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::vector<size_t> column_type_size{4, 8, 4};
  std::cout << "before empty batch resize for column type size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(20, column_type_size);
  std::cout << "after empty batch resize for column type size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;

  std::cout << "before empty batch resize with same column type size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  resize_empty_batch.resize(20, column_type_size);
  std::cout << "after empty batch resize with same column type size: nothing happy"
            << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;

  std::cout << "before empty batch resize with different column type size:" << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;
  std::vector<size_t> new_column_type_size{8, 4, 4};
  resize_empty_batch.resize(20, new_column_type_size);
  std::cout << "after empty batch resize with different column type size: should realloc"
            << std::endl;
  std::cout << resize_empty_batch.toString() << std::endl;

  std::cout << "Test for move assignment:" << std::endl;
  std::cout << "before move assignment:" << std::endl;
  std::cout << column_batch_with_schema.toString() << std::endl;
  column_batch_with_schema = std::move(column_batch_with_size);
  std::cout << "after move assignment:" << std::endl;
  std::cout << column_batch_with_schema.toString() << std::endl;
  std::cout << "Column batch memory lifecycle end, should see deallocate\n" << std::endl;

  // Test for user provided buffer
  std::cout << "Test for user provided buffer:" << std::endl;
  std::vector<const int8_t*> user_buffers;
  for (size_t i = 0; i < column_size.size(); i++) {
    const int8_t* user_buffer = (const int8_t*)malloc(kNumRows * column_size[i]);
    user_buffers.push_back(user_buffer);
  }
  CiderBatch batch_with_user_buffer(kNumRows, user_buffers);
  std::cout << batch_with_user_buffer.toString() << std::endl;

  std::cout << "Test for move assignment user provided buffer:" << std::endl;
  std::cout << "before move assignment:" << std::endl;
  std::cout << column_batch_with_size.toString() << std::endl;
  column_batch_with_size = std::move(batch_with_user_buffer);
  std::cout << "after move assignment:" << std::endl;
  std::cout << column_batch_with_size.toString() << std::endl;

  return 0;
}
