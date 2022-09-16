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

#include "include/cider/batch/CiderBatch.h"
#include "ArrowABI.h"

CiderBatch::CiderBatch(ArrowSchema* schema)
    : arrow_schema_(schema), ownership_(true), reallocate_(true) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  arrow_array_ = CiderBatchUtils::allocateArrowArray();
  arrow_array_->n_buffers = CiderBatchUtils::getBufferNum(arrow_schema_);
  arrow_array_->n_children = arrow_schema_->n_children;
  CiderArrowArrayBufferHolder* root_holder = new CiderArrowArrayBufferHolder(
      arrow_array_->n_buffers, arrow_schema_->n_children, arrow_schema_->dictionary);
  arrow_array_->buffers = root_holder->getBufferPtrs();
  arrow_array_->children = root_holder->getChildrenPtrs();
  arrow_array_->dictionary = root_holder->getDictPtr();
  arrow_array_->private_data = root_holder;
  arrow_array_->release = CiderBatchUtils::ciderArrowArrayReleaser;
}

CiderBatch::CiderBatch(ArrowSchema* schema, ArrowArray* array)
    : arrow_schema_(schema), arrow_array_(array), ownership_(true), reallocate_(false) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  CHECK(arrow_array_);
  CHECK(arrow_array_->release);
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

  return *this;
}

CiderBatch::CiderBatch(CiderBatch&& rh) noexcept {
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = rh.ownership_;
  this->reallocate_ = rh.reallocate_;

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

  rh.arrow_array_ = nullptr;
  rh.arrow_schema_ = nullptr;
  rh.ownership_ = false;
  rh.reallocate_ = false;

  moveFrom(&rh);  // TODO: Remove

  return *this;
}

size_t CiderBatch::getBufferNum() const {
  return arrow_array_->n_buffers;
}

size_t CiderBatch::getChildrenNum() const {
  return arrow_schema_->n_children;
}

SQLTypes CiderBatch::getCiderType() const {
  return CiderBatchUtils::convertArrowTypeToCiderType(arrow_schema_->format);
}

// TODO: Dictionary support is TBD.
std::unique_ptr<CiderBatch> CiderBatch::getChildAt(size_t index) {
  CHECK(arrow_schema_ && arrow_array_);
  CHECK_LT(index, arrow_schema_->n_children);
  ArrowSchema* child_schema = arrow_schema_->children[index];
  ArrowArray* child_array = arrow_array_->children[index];

  if (isMoved()) {
    // Child has been moved.
    return nullptr;
  }

  if (child_array->release == nullptr) {
    // Lazy allocate child array.
    child_array->n_buffers = CiderBatchUtils::getBufferNum(child_schema);
    child_array->n_children = child_schema->n_children;
    CiderArrowArrayBufferHolder* holder = new CiderArrowArrayBufferHolder(
        child_array->n_buffers, child_schema->n_children, child_schema->dictionary);
    child_array->buffers = holder->getBufferPtrs();
    child_array->children = holder->getChildrenPtrs();
    child_array->dictionary = holder->getDictPtr();
    child_array->private_data = holder;
    child_array->release = CiderBatchUtils::ciderArrowArrayReleaser;
  }

  auto child_batch = CiderBatchUtils::createCiderBatch(child_schema, child_array);
  child_batch->ownership_ = false;  // Only root batch has ownership.
  child_batch->reallocate_ =
      true;  // ArrowArray allocated from Cider could (re-)allocate buffer.

  return child_batch;
}

uint8_t* CiderBatch::getMutableNulls() {
  CHECK(!isMoved());
  ArrowArray* array = getArrowArray();

  return reinterpret_cast<uint8_t*>(const_cast<void*>(array->buffers[0]));
}

const uint8_t* CiderBatch::getNulls() const {
  CHECK(!isMoved());
  ArrowArray* array = getArrowArray();

  return reinterpret_cast<const uint8_t*>(array->buffers[0]);
}

bool CiderBatch::resizeNullVector(size_t index, size_t size, bool default_not_null) {
  if (!permitBufferAllocate()) {
    return false;
  }

  ArrowSchema* schema = getArrowSchema();
  ArrowArray* array = getArrowArray();

  auto schema_holder =
      reinterpret_cast<CiderArrowSchemaBufferHolder*>(schema->private_data);
  auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);

  if (schema_holder->needNullVector()) {
    size_t bytes = ((size + 7) >> 3);
    array_holder->allocBuffer(0, bytes);

    uint8_t* null_vector = array_holder->getBufferAs<uint8_t>(index);

    // TODO: Optimize
    for (size_t i = array->length; i < size; ++i) {
      if (default_not_null) {
        CiderBitUtils::setBitAt(null_vector, i);
      } else {
        CiderBitUtils::clearBitAt(null_vector, i);
      }
    }
    if (!default_not_null) {
      array->null_count += size - array->length;
    }
  }

  return true;
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
  CHECK((arrow_schema_ && arrow_schema_) || (!arrow_array_ && !arrow_schema_));
  if (arrow_schema_ && arrow_array_) {
    CHECK((arrow_schema_->release && arrow_array_->release) ||
          (!arrow_schema_->release && !arrow_array_->release));
    return !arrow_array_->release;
  }
  return true;
}

bool CiderBatch::isNullable() const {
  CHECK(!isMoved());
  CiderArrowSchemaBufferHolder* holder =
      reinterpret_cast<CiderArrowSchemaBufferHolder*>(arrow_schema_->private_data);

  return holder->needNullVector();
}
