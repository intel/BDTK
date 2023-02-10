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

#include "cider/batch/CiderBatch.h"
#include "ArrowABI.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "tests/utils/ArrowArrayBuilder.h"

CiderBatch::CiderBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator)
    : arrow_schema_(schema), ownership_(true), reallocate_(true), allocator_(allocator) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  arrow_array_ = CiderBatchUtils::allocateArrowArray();
  arrow_array_->n_buffers = CiderBatchUtils::getBufferNum(arrow_schema_);
  arrow_array_->n_children = arrow_schema_->n_children;
  CiderArrowArrayBufferHolder* root_holder =
      new CiderArrowArrayBufferHolder(arrow_array_->n_buffers,
                                      arrow_schema_->n_children,
                                      allocator_,
                                      arrow_schema_->dictionary);
  arrow_array_->buffers = root_holder->getBufferPtrs();
  arrow_array_->children = root_holder->getChildrenPtrs();
  arrow_array_->dictionary = root_holder->getDictPtr();
  arrow_array_->private_data = root_holder;
  arrow_array_->release = CiderBatchUtils::ciderArrowArrayReleaser;
}

CiderBatch::CiderBatch(ArrowSchema* schema,
                       ArrowArray* array,
                       std::shared_ptr<CiderAllocator> allocator)
    : arrow_schema_(schema)
    , arrow_array_(array)
    , ownership_(true)
    , reallocate_(false)
    , allocator_(allocator) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  CHECK(arrow_array_);
  CHECK(arrow_array_->release);
  CHECK(allocator_);
}

CiderBatch::~CiderBatch() {
  releaseArrowEntries();
#ifdef CIDER_BATCH_CIDER_IMPL
  destroy();  // TODO: Remove
#endif
}

CiderBatch::CiderBatch(const CiderBatch& rh) {
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = false;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;
}

CiderBatch& CiderBatch::operator=(const CiderBatch& rh) {
  if (&rh == this) {
    return *this;
  }
  releaseArrowEntries();

  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = false;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  return *this;
}

CiderBatch::CiderBatch(CiderBatch&& rh) noexcept {
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = rh.ownership_;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  rh.arrow_array_ = nullptr;
  rh.arrow_schema_ = nullptr;
  rh.ownership_ = false;
  rh.reallocate_ = false;
#ifdef CIDER_BATCH_CIDER_IMPL
  moveFrom(&rh);  // TODO: Remove
#endif
}

CiderBatch& CiderBatch::operator=(CiderBatch&& rh) noexcept {
  if (this == &rh) {
    return *this;
  }
  releaseArrowEntries();
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = rh.ownership_;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  rh.arrow_array_ = nullptr;
  rh.arrow_schema_ = nullptr;
  rh.ownership_ = false;
  rh.reallocate_ = false;

#ifdef CIDER_BATCH_CIDER_IMPL
  moveFrom(&rh);  // TODO: Remove
#endif
  return *this;
}

size_t CiderBatch::getBufferNum() const {
  CHECK(arrow_array_);
  return arrow_array_->n_buffers;
}

size_t CiderBatch::getChildrenNum() const {
  CHECK(arrow_schema_);
  return arrow_schema_->n_children;
}

SQLTypes CiderBatch::getCiderType() const {
  CHECK(arrow_schema_);
  return CiderBatchUtils::convertArrowTypeToCiderType(arrow_schema_->format);
}

const char* CiderBatch::getArrowFormatString() const {
  return getArrowSchema().format;
}

// TODO: Dictionary support is TBD.
std::unique_ptr<CiderBatch> CiderBatch::getChildAt(size_t index) {
  CHECK(!isMoved());
  CHECK_LT(index, arrow_schema_->n_children);
  ArrowSchema* child_schema = arrow_schema_->children[index];
  ArrowArray* child_array = arrow_array_->children[index];

  if (!child_schema || !child_schema->release) {
    // Child has been moved.
    return nullptr;
  }

  if (child_array->release == nullptr) {
    // Lazy allocate child array.
    child_array->n_buffers = CiderBatchUtils::getBufferNum(child_schema);
    child_array->n_children = child_schema->n_children;
    CiderArrowArrayBufferHolder* holder =
        new CiderArrowArrayBufferHolder(child_array->n_buffers,
                                        child_schema->n_children,
                                        allocator_,
                                        child_schema->dictionary);
    child_array->buffers = holder->getBufferPtrs();
    child_array->children = holder->getChildrenPtrs();
    child_array->dictionary = holder->getDictPtr();
    child_array->private_data = holder;
    child_array->release = CiderBatchUtils::ciderArrowArrayReleaser;
  }

  auto child_batch =
      CiderBatchUtils::createCiderBatch(allocator_, child_schema, child_array);
  child_batch->ownership_ = false;  // Only root batch has ownership.
  child_batch->reallocate_ =
      true;  // ArrowArray allocated from Cider could (re-)allocate buffer.

  return child_batch;
}

bool CiderBatch::resizeBatch(int64_t size, bool default_not_null) {
  if (getNulls()) {
    if (!resizeNulls(size, default_not_null)) {
      return false;
    }
  }
  if (!resizeData(size)) {
    return false;
  }
  return true;
}

bool CiderBatch::resizeNulls(int64_t size, bool default_not_null) {
  CHECK(!isMoved());
  if (!permitBufferAllocate()) {
    return false;
  }

  ArrowArray array = getArrowArray();
  auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(array.private_data);

  size_t bytes = ((size + 7) >> 3);
  size_t null_index = getNullVectorIndex();
  bool first_time = !array.buffers[null_index];

  array_holder->allocBuffer(null_index, bytes);
  uint8_t* null_vector = array_holder->getBufferAs<uint8_t>(null_index);

  if (default_not_null) {
    // TODO: Optimize
    for (size_t i = (first_time ? 0 : getLength()); i < size; ++i) {
      CiderBitUtils::setBitAt(null_vector, i);
    }
  } else {
    // TODO: Optimize
    for (size_t i = (first_time ? 0 : getLength()); i < size; ++i) {
      CiderBitUtils::clearBitAt(null_vector, i);
    }
  }
  size_t not_null_num = CiderBitUtils::countSetBits(null_vector, size);
  setNullCount(size - not_null_num);

  return true;
}

uint8_t* CiderBatch::getMutableNulls() {
  CHECK(!isMoved());
  ArrowArray array = getArrowArray();
  const void* nulls = array.buffers[getNullVectorIndex()];
  if (!nulls) {
    if (resizeNulls(getLength(), true)) {
      return reinterpret_cast<uint8_t*>(
          const_cast<void*>(array.buffers[getNullVectorIndex()]));
    }
  }
  return nullptr;
}

const uint8_t* CiderBatch::getNulls() const {
  CHECK(!isMoved());
  ArrowArray array = getArrowArray();

  if (!array.buffers) {
    // usually should not happen, but just in case
    CIDER_THROW(CiderRuntimeException, "Arrow Array has no buffer.");
  }

  return reinterpret_cast<const uint8_t*>(array.buffers[getNullVectorIndex()]);
}

void CiderBatch::releaseArrowEntries() {
  if (ownership_) {
    if (arrow_schema_) {
      if (arrow_schema_->release) {
        arrow_schema_->release(arrow_schema_);
      }
      CiderBatchUtils::freeArrowSchema(arrow_schema_);
      arrow_schema_ = nullptr;
    }
    if (arrow_array_) {
      if (arrow_array_->release) {
        arrow_array_->release(arrow_array_);
      }
      // CiderBatchUtils::freeArrowArray(arrow_array_);
      arrow_array_ = nullptr;
    }
  }
}

// to be deprecated, just for test not nullable data.
void CiderBatch::convertToArrowRepresentation() {
  CHECK(!arrow_array_ && !arrow_schema_);
  arrow_array_ = CiderBatchUtils::allocateArrowArray();
  arrow_schema_ = CiderBatchUtils::allocateArrowSchema();

  arrow_array_->length = row_num();
  arrow_array_->n_children = column_num();
  arrow_array_->buffers = nullptr;  // ?
  arrow_array_->n_buffers = 0;      // ?
  arrow_array_->private_data = nullptr;
  arrow_array_->children = (ArrowArray**)std::malloc(sizeof(ArrowArray) * column_num());
  arrow_array_->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

  arrow_schema_->format = "+s";
  arrow_schema_->dictionary = nullptr;
  arrow_schema_->n_children = column_num();
  arrow_schema_->children =
      (ArrowSchema**)std::malloc(sizeof(ArrowSchema*) * column_num());
  arrow_schema_->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;

  for (int i = 0; i < column_num(); i++) {
    arrow_array_->children[i] = new ArrowArray();
    arrow_array_->children[i]->length = row_num();
    arrow_array_->children[i]->n_children = 0;
    void* null_buf = std::malloc(row_num() / 8 + 1);
    std::memset(null_buf, 0xFF, row_num() / 8 + 1);
    arrow_array_->children[i]->private_data = nullptr;
    arrow_array_->children[i]->dictionary = nullptr;
    arrow_array_->children[i]->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

    arrow_schema_->children[i] = new ArrowSchema();
    // todo: velox-plugin does not provide schema.
    arrow_schema_->children[i]->format = "";
    arrow_schema_->children[i]->n_children = 0;
    arrow_schema_->children[i]->children = nullptr;
    arrow_schema_->children[i]->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;

    // (Kunshang)To be removed. temp code to pass ut.
    // CiderStringTest::CiderStringTestArrow
    if (schema_->getColumnTypeById(i).has_varchar()) {
      arrow_array_->children[i]->n_buffers = 3;
      arrow_array_->children[i]->buffers = (const void**)std::malloc(sizeof(void*) * 3);
      arrow_array_->children[i]->buffers[0] = null_buf;

      arrow_schema_->children[i]->format = "";
      // 10 string row 0-9
      int32_t* offset_buf = new int[11]{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
      char* data_buf(
          "000000000011111111112222222222333333333344444444445555555555666666666677777777"
          "7788888888889999999999");
      arrow_array_->children[i]->buffers[1] = offset_buf;
      arrow_array_->children[i]->buffers[2] = data_buf;
    } else {
      arrow_array_->children[i]->buffers = (const void**)std::malloc(sizeof(void*) * 2);
      // FIXME: fill actual null
      arrow_array_->children[i]->buffers[0] = null_buf;
      arrow_array_->children[i]->buffers[1] = table_ptr_[i];
      arrow_array_->children[i]->n_buffers = 2;
    }
  }
}

void CiderBatch::move(ArrowSchema& schema, ArrowArray& array) {
  CHECK(!isMoved());

  schema = *arrow_schema_;
  array = *arrow_array_;
  arrow_schema_->release = nullptr;
  arrow_array_->release = nullptr;
}

std::pair<ArrowSchema*, ArrowArray*> CiderBatch::move() {
  CHECK(!isMoved());
  ArrowSchema* schema = CiderBatchUtils::allocateArrowSchema();
  ArrowArray* array = CiderBatchUtils::allocateArrowArray();

  *schema = *arrow_schema_;
  *array = *arrow_array_;

  arrow_schema_->release = nullptr;
  arrow_array_->release = nullptr;

  return {schema, array};
}

CiderBatch::SchemaReleaser CiderBatch::getSchemaReleaser() const {
  return arrow_schema_->release;
}

CiderBatch::ArrayReleaser CiderBatch::getArrayReleaser() const {
  return arrow_array_->release;
}

void* CiderBatch::getSchemaPrivate() const {
  return arrow_schema_->private_data;
}

void* CiderBatch::getArrayPrivate() const {
  return arrow_array_->private_data;
}

const void** CiderBatch::getBuffersPtr() const {
  CHECK(!isMoved());
  return arrow_array_->buffers;
}

const void** CiderBatch::getChildrenArrayPtr() const {
  CHECK(!isMoved());
  return const_cast<const void**>(reinterpret_cast<void**>(arrow_array_->children));
}

const void* CiderBatch::arrow_column(int32_t col_id) const {
  CHECK(!isMoved());
  const void* buf = arrow_array_->children[col_id]->buffers[1];
  return buf;
}

void CiderBatch::setNullCount(int64_t null_num) {
  CHECK(!isMoved());
  arrow_array_->null_count = null_num;
}

int64_t CiderBatch::getNullCount() const {
  CHECK(!isMoved());
  return arrow_array_->null_count;
}

void CiderBatch::setLength(int64_t length) {
  CHECK(!isMoved());
  arrow_array_->length = length;
}

int64_t CiderBatch::getLength() const {
  CHECK(!isMoved());
  return arrow_array_->length;
}

bool CiderBatch::isMoved() const {
  CHECK((arrow_array_ && arrow_schema_) || (!arrow_array_ && !arrow_schema_));
  if (arrow_schema_ && arrow_array_) {
    CHECK((arrow_schema_->release && arrow_array_->release) ||
          (!arrow_schema_->release && !arrow_array_->release));
    return !arrow_array_->release;
  }
  return true;
}

bool CiderBatch::containsNull() const {
  CHECK(!isMoved());
  return getNulls() && getNullCount();
}

#define PRINT_BY_TYPE(C_TYPE)                                                      \
  {                                                                                \
    ss << "column type: " << #C_TYPE << " ";                                       \
    C_TYPE* buf = (C_TYPE*)(array->buffers[1]);                                    \
    const uint8_t* null_buf = reinterpret_cast<const uint8_t*>(array->buffers[0]); \
    bool has_null_buf = false;                                                     \
    if (null_buf != nullptr) {                                                     \
      has_null_buf = true;                                                         \
    }                                                                              \
    for (int j = 0; j < length; j++) {                                             \
      if (has_null_buf && !CiderBitUtils::isBitSetAt(null_buf, j)) {               \
        ss << "NULL"                                                               \
           << "\t";                                                                \
      } else {                                                                     \
        ss << buf[j] << "\t";                                                      \
      }                                                                            \
    }                                                                              \
    break;                                                                         \
  }

std::string CiderBatch::toStringForArrow() const {
  std::stringstream ss;
  ss << "row num: " << this->arrow_array_->length
     << ", column num: " << this->arrow_array_->n_children << ".\n";

  for (auto i = 0; i < this->arrow_array_->n_children; i++) {
    if (this->arrow_array_->children[i] != nullptr) {
      printByTypeForArrow(ss,
                          this->arrow_schema_->children[i]->format,
                          this->arrow_array_->children[i],
                          this->arrow_array_->length);
    } else {
    }
    ss << '\n';
  }
  return ss.str();
}

void CiderBatch::printByTypeForArrow(std::stringstream& ss,
                                     const char* type,
                                     const ArrowArray* array,
                                     int64_t length) const {
  switch (type[0]) {
    case 'b':
    case 'c':
      PRINT_BY_TYPE(int8_t);
    case 's':
      PRINT_BY_TYPE(int16_t);
    case 'i':
      PRINT_BY_TYPE(int32_t);
    case 'l':
      PRINT_BY_TYPE(int64_t);
    case 'f':
      PRINT_BY_TYPE(float);
    case 'g':
      PRINT_BY_TYPE(double);
    case 'u':
      ss << "column type: String ";
      for (int i = 0; i < length; i++) {
        ss << CiderBatchUtils::extractUtf8ArrowArrayAt(array, i) << " ";
      }
      ss << std::endl;
      break;
    default:
      CIDER_THROW(CiderCompileException, "Not supported type to print value!");
  }
}

namespace CiderBatchUtils {

std::unique_ptr<CiderBatch> createCiderBatch(std::shared_ptr<CiderAllocator> allocator,
                                             ArrowSchema* schema,
                                             ArrowArray* array) {
  CHECK(schema);
  CHECK(schema->release);

  const char* format = schema->format;
  switch (format[0]) {
    // Scalar Types
    case 'b':
      return ScalarBatch<bool>::Create(schema, allocator, array);
    case 'c':
      return ScalarBatch<int8_t>::Create(schema, allocator, array);
    case 's':
      return ScalarBatch<int16_t>::Create(schema, allocator, array);
    case 'i':
      return ScalarBatch<int32_t>::Create(schema, allocator, array);
    case 'l':
      return ScalarBatch<int64_t>::Create(schema, allocator, array);
    case 'f':
      return ScalarBatch<float>::Create(schema, allocator, array);
    case 'g':
      return ScalarBatch<double>::Create(schema, allocator, array);
    case 'd':
      return ScalarBatch<__int128_t>::Create(schema, allocator, array);
    case '+':
      // Complex Types
      switch (format[1]) {
        // Struct Type
        case 's':
          return StructBatch::Create(schema, allocator, array);
      }
    case 't':
      // date32 [days]
      if (format[1] == 'd' && format[2] == 'D') {
        return ScalarBatch<int32_t>::Create(schema, allocator, array);
      }
      // time64 [microseconds]
      if (format[1] == 't' && format[2] == 'u') {
        return ScalarBatch<int64_t>::Create(schema, allocator, array);
      }
      // timestamp [microseconds]
      if (format[1] == 's' && format[2] == 'u') {
        return ScalarBatch<int64_t>::Create(schema, allocator, array);
      }
      break;
    case 'u':
      return VarcharBatch::Create(schema, allocator, array);
    default:
      CIDER_THROW(CiderCompileException,
                  std::string("Unsupported data type to create CiderBatch: ") + format);
  }
}

#define GENERATE_AND_ADD_BOOL_COLUMN(C_TYPE)                                            \
  {                                                                                     \
    std::vector<C_TYPE> col_data;                                                       \
    std::vector<bool> null_data;                                                        \
    col_data.reserve(table_row_num);                                                    \
    null_data.reserve(table_row_num);                                                   \
    C_TYPE* buf = (C_TYPE*)table_ptr[table_ptr_idx];                                    \
    uint8_t* null_buf = (uint8_t*)table_ptr[table_ptr_idx + column_num];                \
    for (auto j = 0; j < table_row_num; ++j) {                                          \
      C_TYPE value = buf[j];                                                            \
      bool is_not_null = null_buf[j];                                                   \
      col_data.push_back(value);                                                        \
      null_data.push_back(!is_not_null);                                                \
    }                                                                                   \
    builder = builder.addBoolColumn<C_TYPE>(names[table_ptr_idx], col_data, null_data); \
    break;                                                                              \
  }

#define GENERATE_AND_ADD_COLUMN(C_TYPE)                                             \
  {                                                                                 \
    std::vector<C_TYPE> col_data;                                                   \
    std::vector<bool> null_data;                                                    \
    col_data.reserve(table_row_num);                                                \
    null_data.reserve(table_row_num);                                               \
    C_TYPE* buf = (C_TYPE*)table_ptr[table_ptr_idx];                                \
    uint8_t* null_buf = (uint8_t*)table_ptr[table_ptr_idx + column_num];            \
    for (auto j = 0; j < table_row_num; ++j) {                                      \
      C_TYPE value = buf[j];                                                        \
      bool is_null = (null_buf[j] == 0);                                            \
      col_data.push_back(value);                                                    \
      null_data.push_back(is_null);                                                 \
    }                                                                               \
    builder =                                                                       \
        builder.addColumn<C_TYPE>(names[table_ptr_idx], type, col_data, null_data); \
    break;                                                                          \
  }

int convertToArrowStruct(ArrowArrayBuilder& builder,
                         const ::substrait::Type type,
                         const int8_t** table_ptr,
                         int table_ptr_idx,
                         int table_row_num,
                         int column_num,
                         std::vector<std::string> names) {
  if (!type.has_struct_()) {
    switch (type.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
        GENERATE_AND_ADD_BOOL_COLUMN(bool)
      case ::substrait::Type::KindCase::kI8:
        GENERATE_AND_ADD_COLUMN(int8_t)
      case ::substrait::Type::KindCase::kI16:
        GENERATE_AND_ADD_COLUMN(int16_t)
      case ::substrait::Type::KindCase::kI32:
        GENERATE_AND_ADD_COLUMN(int32_t)
      case ::substrait::Type::KindCase::kI64:
        GENERATE_AND_ADD_COLUMN(int64_t)
      case ::substrait::Type::KindCase::kFp32:
        GENERATE_AND_ADD_COLUMN(float)
      case ::substrait::Type::KindCase::kFp64:
        GENERATE_AND_ADD_COLUMN(double)
      default:
        CIDER_THROW(CiderCompileException, "Type arrow convert not supported.");
    }
    return 1;
  }

  ArrowArrayBuilder subBuilder;
  int table_ptr_num = 0;
  for (auto subType : type.struct_().types()) {
    auto struct_col_num = convertToArrowStruct(
        subBuilder, subType, table_ptr, table_ptr_idx, table_row_num, column_num, names);
    table_ptr_idx += struct_col_num;
    table_ptr_num += struct_col_num;
  }
  auto schema_and_array = subBuilder.build();
  builder.addStructColumn(std::get<0>(schema_and_array), std::get<1>(schema_and_array));
  return table_ptr_num;
}

#undef GENERATE_AND_ADD_COLUMN
#undef GENERATE_AND_ADD_BOOL_COLUMN

CiderBatch convertToArrow(const CiderBatch& output_batch) {
  std::shared_ptr<CiderTableSchema> table_schema = output_batch.schema();
  auto column_num = table_schema->getFlattenColCount();
  auto arrow_colum_num = output_batch.column_num();
  CHECK_EQ(column_num * 2, arrow_colum_num);
  auto table_row_num = output_batch.row_num();
  ArrowArrayBuilder builder;
  builder = builder.setRowNum(table_row_num);
  const auto& types = table_schema->getColumnTypes();
  const auto& names = table_schema->getFlattenColNames();
  const int8_t** table_ptr = output_batch.table();
  int table_ptr_idx = 0;
  // Every scalar column is flatten stored in table_ptr, while some types in schema are
  // nested. The loop below will enter the struct type and construct ArrowArray
  // recursively.
  for (auto type : types) {
    table_ptr_idx += convertToArrowStruct(
        builder, type, table_ptr, table_ptr_idx, table_row_num, column_num, names);
  }
  auto schema_and_array = builder.build();
  CiderBatch result(std::get<0>(schema_and_array),
                    std::get<1>(schema_and_array),
                    std::make_shared<CiderDefaultAllocator>());
  return result;
}

}  // namespace CiderBatchUtils
