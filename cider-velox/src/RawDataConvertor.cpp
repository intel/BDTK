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

#include "RawDataConvertor.h"
#include <cmath>
#include <cstdint>
#include "TypeConversions.h"
#include "cider/batch/CiderBatch.h"
#include "substrait/type.pb.h"
#include "velox/buffer/Buffer.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::plugin {
template <TypeKind kind>
int8_t* toCiderImpl(VectorPtr& child, int idx, int num_rows) {
  using T = typename TypeTraits<kind>::NativeType;
  auto childVal = child->asFlatVector<T>();
  auto* rawValues = childVal->mutableRawValues();
  if (child->mayHaveNulls()) {
    T nullValue = getNullValue<T>();
    auto nulls = child->rawNulls();
    for (auto pos = 0; pos < num_rows; pos++) {
      if (bits::isBitNull(nulls, pos)) {
        rawValues[pos] = nullValue;
      }
    }
  }
  // memcpy(column, rawValues, sizeof(T) * num_rows);
  return reinterpret_cast<int8_t*>(rawValues);
}

template <TypeKind kind>
int8_t* toCiderImplWithDictEncoding(VectorPtr& child, int idx, int num_rows) {
  using T = typename TypeTraits<kind>::NativeType;
  auto dict = dynamic_cast<const DictionaryVector<T>*>(child.get());
  // TODO(YantingTao1315): new allocator API will be used in the future.
  T* column = (T*)std::malloc(sizeof(T) * num_rows);
  for (auto i = 0; i < num_rows; i++) {
    if (dict->isNullAt(i)) {
      T nullValue = getNullValue<T>();
      column[i] = nullValue;
    } else {
      column[i] = dict->valueAt(i);
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

template <>
int8_t* toCiderImpl<TypeKind::BOOLEAN>(VectorPtr& child, int idx, int num_rows) {
  auto childVal = child->asFlatVector<bool>();
  uint64_t* rawValues = childVal->mutableRawValues<uint64_t>();
  int8_t* column = (int8_t*)std::malloc(sizeof(int8_t) * num_rows);
  auto nulls = child->rawNulls();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, pos)) {
      column[pos] = inline_int_null_value<int8_t>();
    } else {
      column[pos] = bits::isBitSet(rawValues, pos) ? true : false;
    }
  }
  return column;
}

template <>
int8_t* toCiderImpl<TypeKind::VARCHAR>(VectorPtr& child, int idx, int num_rows) {
  auto childVal = child->asFlatVector<StringView>();
  auto* rawValues = childVal->mutableRawValues();
  CiderByteArray* column =
      (CiderByteArray*)std::malloc(sizeof(CiderByteArray) * num_rows);
  auto nulls = child->rawNulls();
  for (auto i = 0; i < num_rows; i++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, i)) {
      column[i].len = 0;
      column[i].ptr = nullptr;
    } else {
      column[i].len = rawValues[i].size();
      // TODO: new allocator API will be used in the future.
      column[i].ptr = (uint8_t*)std::malloc(sizeof(uint8_t) * column[i].len);
      std::memcpy((void*)column[i].ptr, rawValues[i].data(), column[i].len);
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

template <>
int8_t* toCiderImplWithDictEncoding<TypeKind::VARCHAR>(VectorPtr& child,
                                                       int idx,
                                                       int num_rows) {
  auto dict = dynamic_cast<const DictionaryVector<StringView>*>(child.get());
  CiderByteArray* column =
      (CiderByteArray*)std::malloc(sizeof(CiderByteArray) * num_rows);
  for (auto i = 0; i < num_rows; i++) {
    if (dict->isNullAt(i)) {
      column[i].len = 0;
      column[i].ptr = nullptr;
    } else {
      auto stringViewTemp = dict->valueAt(i);
      column[i].len = stringViewTemp.size();
      // TODO(YantingTao1315): new allocator API will be used in the future.
      column[i].ptr = (uint8_t*)std::malloc(sizeof(uint8_t) * column[i].len);
      std::memcpy((void*)column[i].ptr, stringViewTemp.data(), column[i].len);
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

template <>
int8_t* toCiderImplWithDictEncoding<TypeKind::VARBINARY>(VectorPtr& child,
                                                         int idx,
                                                         int num_rows) {
  VELOX_NYI(" {} conversion is not supported with dictionary encoding",
            child->typeKind());
}

template <>
int8_t* toCiderImpl<TypeKind::VARBINARY>(VectorPtr& child, int idx, int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
int8_t* toCiderImplWithDictEncoding<TypeKind::INTERVAL_DAY_TIME>(VectorPtr& child,
                                                                 int idx,
                                                                 int num_rows) {
  VELOX_NYI(" {} conversion is not supported with dictionary encoding",
            child->typeKind());
}

template <>
int8_t* toCiderImpl<TypeKind::INTERVAL_DAY_TIME>(VectorPtr& child,
                                                 int idx,
                                                 int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet");
}

static constexpr int64_t kNanoSecsPerSec = 1000000000;
static constexpr int64_t kMicroSecsPerSec = 1000000;
static constexpr int64_t kMilliSecsPerSec = 1000;
static constexpr int64_t kSecsPerSec = 1;

template <>
int8_t* toCiderImplWithDictEncoding<TypeKind::TIMESTAMP>(VectorPtr& child,
                                                         int idx,
                                                         int num_rows) {
  VELOX_NYI(" {} conversion is not supported with dictionary encoding",
            child->typeKind());
}

template <>
int8_t* toCiderImpl<TypeKind::TIMESTAMP>(VectorPtr& child, int idx, int num_rows) {
  auto childVal = child->asFlatVector<Timestamp>();
  auto* rawValues = childVal->mutableRawValues();
  int64_t* column = (int64_t*)std::malloc(sizeof(int64_t) * num_rows);
  auto nulls = child->rawNulls();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, pos)) {
      column[pos] = std::numeric_limits<int64_t>::min();
    } else {
      // convert to microseconds to align with ::substrait microseconds
      // precision
      column[pos] = rawValues[pos].toMicros();
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

template <>
int8_t* toCiderImplWithDictEncoding<TypeKind::DATE>(VectorPtr& child,
                                                    int idx,
                                                    int num_rows) {
  VELOX_NYI(" {} conversion is not supported yet with dictionary encoding",
            child->typeKind());
}

template <>
int8_t* toCiderImpl<TypeKind::DATE>(VectorPtr& child, int idx, int num_rows) {
  auto childVal = child->asFlatVector<Date>();
  auto* rawValues = childVal->mutableRawValues();
  int64_t* column = (int64_t*)std::malloc(sizeof(int64_t) * num_rows);
  auto nulls = child->rawNulls();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (child->mayHaveNulls() && bits::isBitNull(nulls, pos)) {
      column[pos] = std::numeric_limits<int64_t>::min();
    } else {
      column[pos] = rawValues[pos].days();
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

int8_t* toCiderResult(VectorPtr& child, int idx, int num_rows) {
  switch (child->encoding()) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::LAZY:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          toCiderImpl, child->typeKind(), child, idx, num_rows);
    case VectorEncoding::Simple::DICTIONARY:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          toCiderImplWithDictEncoding, child->typeKind(), child, idx, num_rows);
    default:
      VELOX_NYI(" {} conversion is not supported yet", child->encoding());
  }
}

CiderBatch RawDataConvertor::convertToCider(RowVectorPtr input,
                                            int num_rows,
                                            std::chrono::microseconds* timer) {
  RowVector* row = input.get();
  auto* rowVector = row->as<RowVector>();
  auto size = rowVector->childrenSize();
  std::vector<const int8_t*> table_ptr;
  for (auto idx = 0; idx < size; idx++) {
    VectorPtr& child = rowVector->childAt(idx);
    switch (child->encoding()) {
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::DICTIONARY:
        table_ptr.push_back(toCiderResult(child, idx, num_rows));
        break;
      case VectorEncoding::Simple::LAZY: {
        // For LazyVector, we will load it here and use as TypeVector to use.
        auto tic = std::chrono::system_clock::now();
        auto vec = (std::dynamic_pointer_cast<LazyVector>(child))->loadedVectorShared();
        auto toc = std::chrono::system_clock::now();
        if (timer) {
          *timer += std::chrono::duration_cast<std::chrono::microseconds>(toc - tic);
        }
        table_ptr.push_back(toCiderResult(vec, idx, num_rows));
        break;
      }
      default:
        VELOX_NYI(" {} conversion is not supported yet", child->encoding());
    }
  }
  return CiderBatch(num_rows, table_ptr);
}

struct BufferReleaser {
  void addRef() const {}
  void release() const {}
};

template <TypeKind kind>
VectorPtr toVeloxImpl(const TypePtr& vType,
                      const int8_t* data_buffer,
                      int num_rows,
                      memory::MemoryPool* pool,
                      int32_t /*unused*/) {
  using T = typename TypeTraits<kind>::NativeType;
  BufferReleaser releaser;
  BufferPtr buffer =
      BufferView<BufferReleaser&>::create(reinterpret_cast<const uint8_t*>(data_buffer),
                                          num_rows * vType->cppSizeInBytes(),
                                          releaser);
  auto result = std::make_shared<FlatVector<T>>(pool,
                                                vType,
                                                BufferPtr(nullptr),
                                                num_rows,
                                                buffer,
                                                std::vector<BufferPtr>(),
                                                SimpleVectorStats<T>(),
                                                std::nullopt,
                                                0);

  // auto result = BaseVector::create(vType, num_rows, pool);
  // auto flatResult = result->as<FlatVector<T>>();
  // auto rawValues = flatResult->mutableRawValues();
  const T* srcValues = reinterpret_cast<const T*>(data_buffer);
  // memcpy(rawValues, srcValues, num_rows * sizeof(T));
  T nullValue = getNullValue<T>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == nullValue) {
      result->setNull(pos, true);
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::BOOLEAN>(const TypePtr& vType,
                                         const int8_t* data_buffer,
                                         int num_rows,
                                         memory::MemoryPool* pool,
                                         int32_t /*unused*/) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<bool>>();
  auto rawValues = flatResult->mutableRawValues<uint64_t>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (data_buffer[pos] == inline_int_null_value<int8_t>()) {
      result->setNull(pos, true);
    } else {
      bits::setBit(rawValues, pos, static_cast<bool>(data_buffer[pos]));
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::VARCHAR>(const TypePtr& vType,
                                         const int8_t* data_buffer,
                                         int num_rows,
                                         memory::MemoryPool* pool,
                                         int32_t /*unused*/) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<StringView>>();
  const CiderByteArray* srcValues = reinterpret_cast<const CiderByteArray*>(data_buffer);
  auto rawValues = flatResult->mutableRawValues<uint64_t>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos].ptr == nullptr) {
      result->setNull(pos, true);
    } else {
      flatResult->set(pos,
                      StringView(reinterpret_cast<const char*>(srcValues[pos].ptr),
                                 srcValues[pos].len));
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::INTERVAL_DAY_TIME>(const TypePtr& vType,
                                                   const int8_t* data_buffer,
                                                   int num_rows,
                                                   memory::MemoryPool* pool,
                                                   int32_t /*unused*/) {
  VELOX_NYI(" {} conversion is not supported yet");
}

template <>
VectorPtr toVeloxImpl<TypeKind::VARBINARY>(const TypePtr& vType,
                                           const int8_t* data_buffer,
                                           int num_rows,
                                           memory::MemoryPool* pool,
                                           int32_t /*unused*/) {
  VELOX_NYI(" '{}' conversion is not supported yet", vType->toString());
}

std::tuple<int64_t, int64_t> calculateScale(int32_t dimen) {
  switch (dimen) {
    case CIDER_DIMEN::SECOND:
      return {kSecsPerSec, kNanoSecsPerSec};
    case CIDER_DIMEN::MILLISECOND:
      return {kMilliSecsPerSec, kMicroSecsPerSec};
    case CIDER_DIMEN::MICROSECOND:
      return {kMicroSecsPerSec, kMilliSecsPerSec};
    case CIDER_DIMEN::NANOSECOND:
      return {kNanoSecsPerSec, kSecsPerSec};
    default:
      VELOX_UNREACHABLE("Unknown dimension");
  }
}

template <>
VectorPtr toVeloxImpl<TypeKind::TIMESTAMP>(const TypePtr& vType,
                                           const int8_t* data_buffer,
                                           int num_rows,
                                           memory::MemoryPool* pool,
                                           int32_t dimen) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<Timestamp>>();
  const int64_t* srcValues = reinterpret_cast<const int64_t*>(data_buffer);
  auto [scaleSecond, scaleNano] = calculateScale(dimen);  // NOLINT
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == std::numeric_limits<int64_t>::min()) {
      result->setNull(pos, true);
    } else {
      auto timeValue = srcValues[pos];
      flatResult->set(
          pos, Timestamp(timeValue / scaleSecond, (timeValue % scaleSecond) * scaleNano));
    }
  }
  return result;
}

template <>
VectorPtr toVeloxImpl<TypeKind::DATE>(const TypePtr& vType,
                                      const int8_t* data_buffer,
                                      int num_rows,
                                      memory::MemoryPool* pool,
                                      int32_t /*unused*/) {
  auto result = BaseVector::create(vType, num_rows, pool);
  auto flatResult = result->as<FlatVector<Date>>();
  const int64_t* srcValues = reinterpret_cast<const int64_t*>(data_buffer);
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == std::numeric_limits<int64_t>::min()) {
      result->setNull(pos, true);
    } else {
      auto value = srcValues[pos];
      flatResult->set(pos, Date(value));
    }
  }
  return result;
}

VectorPtr toVeloxDecimalImpl(const TypePtr& vType,
                             const int8_t* data_buffer,
                             int num_rows,
                             memory::MemoryPool* pool) {
  BufferReleaser releaser;
  BufferPtr buffer =
      BufferView<BufferReleaser&>::create(reinterpret_cast<const uint8_t*>(data_buffer),
                                          num_rows * vType->cppSizeInBytes(),
                                          releaser);
  auto result = std::make_shared<FlatVector<double>>(pool,
                                                     vType,
                                                     BufferPtr(nullptr),
                                                     num_rows,
                                                     buffer,
                                                     std::vector<BufferPtr>(),
                                                     SimpleVectorStats<double>(),
                                                     std::nullopt,
                                                     0);

  const int64_t* srcValues = reinterpret_cast<const int64_t*>(data_buffer);
  int64_t nullValue = getNullValue<int64_t>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == nullValue) {
      result->setNull(pos, true);
    }
  }
  return result;
}

VectorPtr toVeloxVector(const TypePtr& vType,
                        const ::substrait::Type sType,
                        const int8_t* data_buffer,
                        int num_rows,
                        memory::MemoryPool* pool,
                        int32_t dimen = CIDER_DIMEN::MICROSECOND) {
  if (sType.kind_case() == ::substrait::Type::KindCase::kDecimal) {
    return toVeloxDecimalImpl(vType, data_buffer, num_rows, pool);
  } else {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        toVeloxImpl, vType->kind(), vType, data_buffer, num_rows, pool, dimen);
  }
}

RowVectorPtr RawDataConvertor::convertToRowVector(const CiderBatch& input,
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
  int inputColIndex = 0;

  for (int i = 0; i < num_cols; i++) {
    ::substrait::Type sType = schema.getColumnTypeById(i);
    types.push_back(getVeloxType(sType));
    auto currentData = input.column(i);
    auto columNum = input.column_num();
    if (sType.kind_case() == substrait::Type::kStruct) {
      // TODO : (ZhangJie) Support nested struct.
      // For the case, struct[sum, count].
      auto structSize = sType.struct_().types_size();
      std::vector<VectorPtr> columnStruct;
      columnStruct.reserve(structSize);

      for (int typeId = 0; typeId < structSize; typeId++) {
        columnStruct.emplace_back(toVeloxVector(types[i]->childAt(typeId),
                                                sType.struct_().types(typeId),
                                                input.column(inputColIndex + typeId),
                                                num_rows,
                                                pool));
      }

      columns.push_back(std::make_shared<RowVector>(
          pool, types[i], BufferPtr(nullptr), num_rows, columnStruct));

      inputColIndex = inputColIndex + structSize;
    } else {
      columns.push_back(
          toVeloxVector(types[i], sType, input.column(inputColIndex), num_rows, pool));
      inputColIndex = inputColIndex + 1;
    }
  }
  rowType = std::make_shared<RowType>(move(col_names), move(types));
  return std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), num_rows, columns);
}
}  // namespace facebook::velox::plugin
