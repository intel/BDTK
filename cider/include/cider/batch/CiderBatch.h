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

#ifndef CIDER_CIDERBATCH_H
#define CIDER_CIDERBATCH_H

#include "../CiderAllocator.h"
#include "../CiderTableSchema.h"
#include "../CiderTypes.h"
#include "CiderBatchUtils.h"
#include "cider/CiderException.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "util/CiderBitUtils.h"

#define CIDER_BATCH_ARROW_IMPL
#define CIDER_BATCH_CIDER_IMPL

/// \class CiderBatch
/// \brief This class will be data/table format interface/protocol
class CiderBatch {
#ifdef CIDER_BATCH_ARROW_IMPL
 public:
  // Both constructors will take over the ownership of ArrowSchema & ArrowArray and
  // buffers they hold, so ArrowSchema and ArrowArray should allocated from
  // CiderBatchUtils. Never construct distinct CiderBatches by this method with same
  // schema and array.
  explicit CiderBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator);
  // In this case, CiderBatch references the memory allocated by the Caller, therefore
  // resize is not allowed.
  explicit CiderBatch(ArrowSchema* schema,
                      ArrowArray* array,
                      std::shared_ptr<CiderAllocator> allocator);

  virtual ~CiderBatch();

  CiderBatch(const CiderBatch& rh);
  CiderBatch(CiderBatch&& rh) noexcept;
  CiderBatch& operator=(const CiderBatch& rh);
  CiderBatch& operator=(CiderBatch&& rh) noexcept;

  size_t getBufferNum() const;
  size_t getChildrenNum() const;
  SQLTypes getCiderType() const;
  const char* getArrowFormatString() const;

  bool isRootOwner() const { return ownership_; }

  bool isMoved() const;

  bool containsNull() const;

  std::unique_ptr<CiderBatch> getChildAt(size_t index);

  void convertToArrowRepresentation();

  // Move ArrowSchema and ArrowArray out of the tree. The ownership will be moved to the
  // caller. Typical usage:
  /*
    CiderBatch.move(array, schema);
  */
  void move(ArrowSchema& schema, ArrowArray& array);
  std::pair<ArrowSchema*, ArrowArray*> move();

  template <typename T>
  T* asMutable() {
    CHECK(!isMoved());
    static_assert(std::is_base_of_v<CiderBatch, T>);
    return dynamic_cast<T*>(this);
  }

  template <typename T>
  const T* as() const {
    CHECK(!isMoved());
    static_assert(std::is_base_of_v<CiderBatch, T>);
    return dynamic_cast<const T*>(this);
  }

  // TODO: Change to pure virtual function.
  // CiderBatch doesn't contain null vector by default until getMutableNulls is called.
  bool resizeBatch(int64_t size, bool default_not_null = false);

  // This function has a side effect that the null vector will be allocated if there is no
  // null vector exists.
  virtual uint8_t* getMutableNulls();

  virtual const uint8_t* getNulls() const;

  void setNullCount(int64_t null_num);

  int64_t getNullCount() const;

  void setLength(int64_t length);

  int64_t getLength() const;

  const void** getChildrenArrayPtr() const;

  std::string toStringForArrow() const;

 protected:
  using SchemaReleaser = void (*)(struct ArrowSchema*);
  using ArrayReleaser = void (*)(struct ArrowArray*);

  // This function will not resize nulls.
  // TODO: Change to pure virtual function.
  virtual bool resizeData(int64_t size) {
    CIDER_THROW(CiderUnsupportedException, fmt::format("size is {}", size));
  }
  bool resizeNulls(int64_t size, bool default_not_null);

  virtual size_t getNullVectorIndex() const { return 0; }

  bool permitBufferAllocate() const { return reallocate_; }

  ArrowSchema* getArrowSchema() const { return arrow_schema_; }

  ArrowArray* getArrowArray() const { return arrow_array_; }

  SchemaReleaser getSchemaReleaser() const;
  ArrayReleaser getArrayReleaser() const;
  void* getSchemaPrivate() const;
  void* getArrayPrivate() const;
  const void** getBuffersPtr() const;

 private:
  void releaseArrowEntries();

  ArrowSchema* arrow_schema_{nullptr};
  ArrowArray* arrow_array_{nullptr};
  bool ownership_{false};  // Whether need to release the tree of schema_ and array_.
  bool reallocate_{
      false};  // Whether permitted to (re-)allocate memory to buffers of array_.

  void printByTypeForArrow(std::stringstream& ss,
                           const char* type,
                           const ArrowArray* array,
                           int64_t length) const;
#endif

#ifdef CIDER_BATCH_CIDER_IMPL
 public:
  /// \brief Constructs CiderBatch that will use row memory layout with self memory
  /// manager. It will allocate row_num * row_size memory internally and the allocated
  /// memory will be released when leave the scope or manually call the move construct.
  ///
  /// \param row_num the row capacity of the CiderBatch
  /// \param row_size the size of a row in bytes
  /// \param allocator the optional memory allocator from user
  /// \param schema the optional param that represent the schema used by the CiderBatch
  /// \param align the optional alignment used for memory allocate
  explicit CiderBatch(int64_t row_num,
                      size_t row_size,
                      std::shared_ptr<CiderAllocator> allocator = nullptr,
                      const std::shared_ptr<CiderTableSchema> schema = nullptr,
                      uint32_t align = kDefaultAlignment)
      : row_num_(row_num)
      , row_capacity_(row_num)
      , allocator_(allocator)
      , schema_(schema)
      , align_(align) {
    if (allocator_ == nullptr) {
      allocator_ = std::make_shared<CiderDefaultAllocator>();
    }
    if (row_num > 0 && row_size > 0) {
      size_t row_buffer_size = alignSize(row_capacity_ * row_size, align_);
      auto row_buffer =
          reinterpret_cast<const int8_t*>(allocator_->allocate(row_buffer_size));
      table_ptr_.push_back(row_buffer);
      column_type_size_.push_back(row_size);
    }
  }

  /// \brief Constructs CiderBatch that will use column memory layout with self memory
  /// manager. The column size will be got form the parameter size vector. It will
  /// allocate row_num * total column size memory internally and the allocated memory
  /// will be released when leave the scope or manually call the move constructor or move
  /// assignment.
  ///
  /// \param row_num the row capacity of the CiderBatch
  /// \param column_type_size the vector that store the size of each column in the
  /// \param allocator the optional memory allocator from user
  /// \param schema the optional param that represent the schema used by the
  /// \param align the optional alignment used for memory allocate
  explicit CiderBatch(int64_t row_num,
                      const std::vector<size_t> column_type_size,
                      std::shared_ptr<CiderAllocator> allocator = nullptr,
                      const std::shared_ptr<CiderTableSchema> schema = nullptr,
                      uint32_t align = kDefaultAlignment)
      : row_num_(row_num)
      , row_capacity_(row_num)
      , column_type_size_(std::move(column_type_size))
      , allocator_(allocator)
      , schema_(schema)
      , align_(align) {
    if (allocator_ == nullptr) {
      allocator_ = std::make_shared<CiderDefaultAllocator>();
    }
    for (size_t i = 0; i < column_type_size_.size(); i++) {
      size_t column_buffer_size = alignSize(row_capacity_ * column_type_size_[i], align_);
      auto column_buffer =
          reinterpret_cast<const int8_t*>(allocator_->allocate(column_buffer_size));
      table_ptr_.push_back(column_buffer);
    }
  }

  /// \brief Constructs CiderBatch that will use column memory layout with self memory
  /// manager. The column size will be got form the parameter schema. It will allocate
  /// row_num * total column size memory internally and the allocated memory will be
  /// released when leave the scope or manually call the move constructor or move
  /// assignment.
  ///
  /// \param row_num the row capacity of the CiderBatch
  /// \param schema the schema used by the CiderBatch
  /// \param allocator the optional memory allocator from user
  /// \param align the optional alignment used for memory allocate
  explicit CiderBatch(int64_t row_num,
                      const std::shared_ptr<CiderTableSchema> schema,
                      std::shared_ptr<CiderAllocator> allocator = nullptr,
                      uint32_t align = kDefaultAlignment)
      : row_num_(row_num)
      , row_capacity_(row_num)
      , allocator_(allocator)
      , schema_(schema)
      , align_(align) {
    if (allocator_ == nullptr) {
      allocator_ = std::make_shared<CiderDefaultAllocator>();
    }
    if (schema_) {
      for (size_t i = 0; i < schema_->getColumnCount(); i++) {
        int column_type_size = schema_->GetColumnTypeSize(i);
        size_t column_buffer_size = alignSize(row_capacity_ * column_type_size, align_);
        auto column_buffer =
            reinterpret_cast<const int8_t*>(allocator_->allocate(column_buffer_size));
        table_ptr_.push_back(column_buffer);
        column_type_size_.push_back(column_type_size);
      }
    }
  }

  /// \brief Constructs CiderBatch that will use the user provided buffers
  /// It is user's responsibility to release the user provided buffers.
  ///
  /// \param row_num the row capacity of the CiderBatch
  /// \param table_ptr the vector that store the address of buffers proviede by user
  /// \param schema the optional param that represent the schema used by the CiderBatch
  explicit CiderBatch(int64_t row_num,
                      const std::vector<const int8_t*> table_ptr,
                      const std::shared_ptr<CiderTableSchema> schema = nullptr)
      : row_num_(row_num)
      , row_capacity_(row_num)
      , table_ptr_(std::move(table_ptr))
      , schema_(schema)
      , is_use_self_memory_manager_(false) {}

  /// \brief Constructs CiderBatch that will use the user provided allocator
  ///
  /// \param allocator the allocator provided by user
  explicit CiderBatch(std::shared_ptr<CiderAllocator> allocator) : allocator_(allocator) {
    if (allocator_ == nullptr) {
      allocator_ = std::make_shared<CiderDefaultAllocator>();
    }
  }

  CiderBatch() : schema_(nullptr), align_(kDefaultAlignment) {
    allocator_ = std::make_shared<CiderDefaultAllocator>();
  }

  /// \brief Resize the CiderBatch to the new row capacity
  ///
  /// \param new_row_capacity the new row capacity of the of the CiderBatch.
  /// \param new_row_size the new row size of the of the CiderBatch.
  void resize(const int64_t new_row_capacity, const int64_t new_row_size) {
    if (is_use_self_memory_manager_) {
      if (table_ptr_.empty() || (new_row_capacity > row_capacity_) ||
          (new_row_size > static_cast<int64_t>(column_type_size_[0]))) {
        destroy();
        size_t row_buffer_size = alignSize(new_row_capacity * new_row_size, align_);
        auto row_buffer =
            reinterpret_cast<const int8_t*>(allocator_->allocate(row_buffer_size));
        row_capacity_ = new_row_capacity;
        row_num_ = new_row_capacity;
        table_ptr_.push_back(row_buffer);
        column_type_size_.push_back(new_row_size);
      }
    }
  }

  /// \brief Resize the CiderBatch to the new row capacity or new row size
  ///
  /// \param new_row_capacity the new row capacity of the of the CiderBatch.
  /// \param new_row_size the new row size of the of the CiderBatch.
  void resize(const int64_t new_row_capacity,
              const std::vector<size_t> new_column_type_size) {
    if (is_use_self_memory_manager_) {
      if (table_ptr_.empty() || (new_row_capacity > row_capacity_) ||
          new_column_type_size != column_type_size_) {
        destroy();
        for (auto i : new_column_type_size) {
          size_t column_buffer_size = alignSize(new_row_capacity * i, align_);
          auto column_buffer =
              reinterpret_cast<const int8_t*>(allocator_->allocate(column_buffer_size));
          table_ptr_.push_back(column_buffer);
        }
        row_capacity_ = new_row_capacity;
        row_num_ = new_row_capacity;
        column_type_size_ = new_column_type_size;
      }
    }
  }

  const int8_t* column(int32_t col_id) const { return table_ptr_[col_id]; }

  const int8_t** table() const {
    return table_ptr_.empty() ? nullptr : const_cast<const int8_t**>(table_ptr_.data());
  }

  // same as column(), return the pointer to the child array's data buffer regardless of
  // its type
  const void* arrow_column(int32_t col_id) const;

  int64_t row_num() const { return row_num_; }

  size_t column_type_size(int32_t col_id) const { return column_type_size_[col_id]; }

  void set_row_num(int64_t row_num) { row_num_ = row_num; }

  int64_t row_capacity() const { return row_capacity_; }

  int64_t column_num() const { return table_ptr_.size(); }

  void set_null_vecs(std::vector<std::vector<bool>>& null_vecs) {
    null_vecs_ = null_vecs;
  }

  std::vector<std::vector<bool>> get_null_vecs() const { return null_vecs_; }

  std::string toString() const {
    std::stringstream ss;
    ss << "rowNum: " << row_num_ << "\nrowCapacity: " << row_capacity_ << "\ntablePtrs:[";
    for (size_t i = 0; i < table_ptr_.size(); ++i) {
      auto addr_num = reinterpret_cast<std::uintptr_t>(table_ptr_[i]);
      ss << std::hex << addr_num << std::dec << ",";
    }
    ss << "]\n"
       << "columnSizes:[";
    for (size_t i = 0; i < column_type_size_.size(); ++i) {
      ss << column_type_size_[i] << ",";
    }
    ss << "]\n"
       << "tableSchema: " << schema_ << "\n"
       << "align: " << align_ << "\n"
       << "self_memory_manager: " << is_use_self_memory_manager_ << "\n";
    return ss.str();
  }

#define PRINT_BY_TYPE(C_TYPE)                \
  {                                          \
    ss << "column type: " << #C_TYPE << " "; \
    C_TYPE* buf = (C_TYPE*)table_ptr_[loc];  \
    for (int j = 0; j < row_num_; j++) {     \
      ss << buf[j] << "\t";                  \
    }                                        \
    break;                                   \
  }

  // Only for debug usage.
  std::string toValueString() const {
    std::stringstream ss;
    ss << "row num: " << row_num_ << ", column num: " << column_num() << ".\n";
    // don't have an valid schema
    if (schema_ == nullptr) {
      for (int loc = 0; loc < column_type_size_.size(); loc++) {
        switch (column_type_size_[loc]) {
          case 4:
            PRINT_BY_TYPE(int32_t)
          case 8:
            PRINT_BY_TYPE(int64_t)
          default:
            CIDER_THROW(CiderCompileException, "Not supported type size to print value!");
        }
        ss << "\n";
      }
    } else {
      auto types = schema_->getColumnTypes();
      for (auto loc = 0; loc < types.size(); loc++) {
        // print by Type kind
        printByType(ss, types[loc], loc);
        ss << "\n";
      }
    }
    return ss.str();
  }

  void printByType(std::stringstream& ss, substrait::Type type, int64_t loc) const {
    switch (type.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
      case ::substrait::Type::KindCase::kI8:
        PRINT_BY_TYPE(int8_t)
      case ::substrait::Type::KindCase::kI16:
        PRINT_BY_TYPE(int16_t)
      case ::substrait::Type::KindCase::kI32:
        PRINT_BY_TYPE(int32_t)
      case ::substrait::Type::KindCase::kI64:
      case ::substrait::Type::KindCase::kTimestamp:
      case ::substrait::Type::KindCase::kTime:
      case ::substrait::Type::KindCase::kDate:
        PRINT_BY_TYPE(int64_t)
      case ::substrait::Type::KindCase::kFp32:
        PRINT_BY_TYPE(float)
      case ::substrait::Type::KindCase::kFp64:
      case ::substrait::Type::KindCase::kDecimal:
        PRINT_BY_TYPE(double)
      case ::substrait::Type::KindCase::kFixedChar:
      case ::substrait::Type::KindCase::kVarchar:
      case ::substrait::Type::KindCase::kString: {
        ss << "column type: CiderByteArray ";
        CiderByteArray* buf = (CiderByteArray*)table_ptr_[loc];
        for (int j = 0; j < row_num_; j++) {
          ss << CiderByteArray::toString(buf[j]) << "\t";
        }
        break;
      }
      case ::substrait::Type::KindCase::kStruct: {
        ss << "column type: Struct "
           << "\n";
        auto structSize = type.struct_().types_size();
        for (int typeId = 0; typeId < structSize; typeId++) {
          printByType(ss, type.struct_().types(typeId), loc + typeId);
          ss << "\n";
        }
        break;
      }
      default:
        CIDER_THROW(CiderCompileException, "Not supported type to print value!");
    }
  }

  const std::shared_ptr<CiderTableSchema> schema() const { return schema_; }

  void set_schema(const std::shared_ptr<CiderTableSchema> schema) { schema_ = schema; }

 private:
  int64_t row_num_ = 0;
  int64_t row_capacity_ = 0;
  std::vector<size_t> column_type_size_{};
  std::vector<const int8_t*> table_ptr_{};  // underlayer format is Modular SQL format
  std::shared_ptr<CiderAllocator> allocator_;
  std::shared_ptr<CiderTableSchema> schema_ = nullptr;
  bool is_use_self_memory_manager_ = true;
  uint32_t align_ = kDefaultAlignment;
  std::vector<std::vector<bool>>
      null_vecs_;  // only use to pass and mark null data value to the duckdb engine
  static constexpr uint32_t kDefaultAlignment = 64;

  size_t alignSize(size_t size, uint32_t align) const noexcept {
    return (((static_cast<uint32_t>(size)) + (align)-1) & ~((align)-1));
  }

  void destroy() {
    if (is_use_self_memory_manager_) {
      for (size_t i = 0; i < table_ptr_.size(); i++) {
        if (table_ptr_[i]) {
          size_t buffer_size = alignSize(row_capacity_ * column_type_size_[i], align_);
          allocator_->deallocate(const_cast<int8_t*>(table_ptr_[i]), buffer_size);
          table_ptr_[i] = nullptr;
        }
      }
      table_ptr_.clear();
      column_type_size_.clear();
      null_vecs_.clear();
    }
  }

  void moveFrom(CiderBatch* other) {
    destroy();
    row_num_ = other->row_num_;
    row_capacity_ = other->row_capacity_;
    column_type_size_ = other->column_type_size_;
    table_ptr_ = std::move(other->table_ptr_);
    allocator_ = other->allocator_;
    schema_ = other->schema_;
    is_use_self_memory_manager_ = other->is_use_self_memory_manager_;
    align_ = other->align_;
    null_vecs_ = other->null_vecs_;
  }
#endif
};

#endif  // CIDER_CIDERBATCH_H
