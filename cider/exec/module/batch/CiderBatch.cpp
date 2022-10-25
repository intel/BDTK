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

#include "cider/batch/CiderBatch.h"
#include "ArrowABI.h"

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
  destroy();  // TODO: Remove
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

  moveFrom(&rh);  // TODO: Remove
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

  moveFrom(&rh);  // TODO: Remove

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

  ArrowArray* array = getArrowArray();
  auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);

  size_t bytes = ((size + 7) >> 3);
  size_t null_index = getNullVectorIndex();
  bool first_time = !array->buffers[null_index];

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
  ArrowArray* array = getArrowArray();
  const void* nulls = array->buffers[getNullVectorIndex()];
  if (!nulls) {
    if (resizeNulls(getLength(), true)) {
      return reinterpret_cast<uint8_t*>(
          const_cast<void*>(array->buffers[getNullVectorIndex()]));
    }
  }
  return nullptr;
}

const uint8_t* CiderBatch::getNulls() const {
  CHECK(!isMoved());
  ArrowArray* array = getArrowArray();

  if (!array->buffers) {
    // usually should not happen, but just in case
    CIDER_THROW(CiderRuntimeException, "Arrow Array has no buffer.");
  }

  return reinterpret_cast<const uint8_t*>(array->buffers[getNullVectorIndex()]);
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
      CiderBatchUtils::freeArrowArray(arrow_array_);
      arrow_array_ = nullptr;
    }
  }
}

// to be deprecated, just for test not nullable data.
void CiderBatch::convertToArrowRepresentation() {
  auto column_num = schema_->getColumnCount();
  CHECK(!arrow_array_ && !arrow_schema_);
  arrow_array_ = CiderBatchUtils::allocateArrowArray();
  arrow_schema_ = CiderBatchUtils::allocateArrowSchema();

  arrow_array_->length = row_num();
  arrow_array_->n_children = column_num;
  arrow_array_->buffers = nullptr;  // ?
  arrow_array_->n_buffers = 0;      // ?
  arrow_array_->private_data = nullptr;
  arrow_array_->children = (ArrowArray**)std::malloc(sizeof(ArrowArray) * column_num);
  arrow_array_->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

  arrow_schema_->format = "+s";
  arrow_schema_->dictionary = nullptr;
  arrow_schema_->n_children = column_num;
  arrow_schema_->children = (ArrowSchema**)std::malloc(sizeof(ArrowSchema*) * column_num);
  arrow_schema_->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;

  for (int i = 0; i < column_num; i++) {
    arrow_array_->children[i] = new ArrowArray();
    arrow_array_->children[i]->length = row_num();
    arrow_array_->children[i]->n_children = 0;
    arrow_array_->children[i]->buffers = (const void**)std::malloc(sizeof(void*) * 2);
    // FIXME: fill actual null
    void* null_buf = std::malloc(row_num() / 8 + 1);
    std::memset(null_buf, 0xFF, row_num() / 8 + 1);
    arrow_array_->children[i]->buffers[0] = null_buf;
    arrow_array_->children[i]->buffers[1] = table_ptr_[i];
    arrow_array_->children[i]->n_buffers = 2;
    arrow_array_->children[i]->private_data = nullptr;
    arrow_array_->children[i]->dictionary = nullptr;
    arrow_array_->children[i]->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

    arrow_schema_->children[i] = new ArrowSchema();
    // todo: velox-plugin does not provide schema.
    arrow_schema_->children[i]->format = "";
    arrow_schema_->children[i]->n_children = 0;
    arrow_schema_->children[i]->children = nullptr;
    arrow_schema_->children[i]->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;
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
