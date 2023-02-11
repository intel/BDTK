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

#include "DuckDbQueryRunner.h"
#include <memory>
#include <utility>
#include "DuckDbArrowAdaptor.h"
#include "Utils.h"
#include "cider/CiderTypes.h"
#include "exec/plan/parser/TypeUtils.h"
#include "util/Logger.h"
#include "cider/CiderAllocator.h"
#include "util/CiderBitUtils.h"

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

template <typename T>
::duckdb::Value duckValueAt(const int8_t* buf, int64_t offset) {
  return ::duckdb::Value(*(T*)(buf + sizeof(T) * offset));
}

template <>
::duckdb::Value duckValueAt<std::string>(const int8_t* buf, int64_t offset) {
  const CiderByteArray* ptr = (const CiderByteArray*)(buf);
  std::string copy((char*)ptr[offset].ptr, ptr[offset].len);
  return ::duckdb::Value(copy);
}

::duckdb::Value duckDateValueAt(const int8_t* buf, int64_t offset) {
  int64_t epochSeconds = *(int64_t*)(buf + sizeof(int64_t) * offset);
  int32_t epochDays = epochSeconds / kSecondsInOneDay;
  auto date = ::duckdb::Value::DATE(::duckdb::Date::EpochDaysToDate(epochDays));
  return date;
}

::duckdb::Value duckTimeValueAt(const int8_t* buf, int64_t offset) {
  int64_t micros = *(int64_t*)(buf + sizeof(int64_t) * offset);
  auto time = ::duckdb::Value::TIME(duckdb::dtime_t(micros));
  return time;
}

::duckdb::Value duckTimestampValueAt(const int8_t* buf, int64_t offset) {
  int64_t micros = *(int64_t*)(buf + sizeof(int64_t) * offset);
  auto timestamp =
      ::duckdb::Value::TIMESTAMP(::duckdb::Timestamp::FromEpochMicroSeconds(micros));
  return timestamp;
}

#define GEN_DUCK_VALUE_FUNC                                                  \
  [&]() {                                                                    \
    switch (s_type.kind_case()) {                                            \
      case ::substrait::Type::KindCase::kBool:                               \
      case ::substrait::Type::KindCase::kI8: {                               \
        return duckValueAt<int8_t>(current_batch->column(k), j);             \
      }                                                                      \
      case ::substrait::Type::KindCase::kI16: {                              \
        return duckValueAt<int16_t>(current_batch->column(k), j);            \
      }                                                                      \
      case ::substrait::Type::KindCase::kI32: {                              \
        return duckValueAt<int32_t>(current_batch->column(k), j);            \
      }                                                                      \
      case ::substrait::Type::KindCase::kI64: {                              \
        return duckValueAt<int64_t>(current_batch->column(k), j);            \
      }                                                                      \
      case ::substrait::Type::KindCase::kFp32: {                             \
        return duckValueAt<float>(current_batch->column(k), j);              \
      }                                                                      \
      case ::substrait::Type::KindCase::kFp64: {                             \
        return duckValueAt<double>(current_batch->column(k), j);             \
      }                                                                      \
      case ::substrait::Type::KindCase::kDate: {                             \
        return duckDateValueAt(current_batch->column(k), j);                 \
      }                                                                      \
      case ::substrait::Type::KindCase::kTime: {                             \
        return duckTimeValueAt(current_batch->column(k), j);                 \
      }                                                                      \
      case ::substrait::Type::KindCase::kTimestamp: {                        \
        return duckTimestampValueAt(current_batch->column(k), j);            \
      }                                                                      \
      case ::substrait::Type::KindCase::kFixedChar:                          \
      case ::substrait::Type::KindCase::kVarchar:                            \
      case ::substrait::Type::KindCase::kString: {                           \
        return duckValueAt<std::string>(current_batch->column(k), j);        \
      }                                                                      \
      default:                                                               \
        CIDER_THROW(CiderException, "not supported type to gen duck value"); \
    }                                                                        \
  }

::duckdb::Value duckValueVarcharAt(const int8_t* data_buffer,
                                   const int32_t* offset_buffer,
                                   int64_t offset) {
  auto len = offset_buffer[offset + 1] - offset_buffer[offset];
  char copy[len + 1];
  memcpy(&copy, &data_buffer[offset_buffer[offset]], len);
  copy[len] = '\0';
  return ::duckdb::Value(std::string(copy));
}

#define GEN_DUCK_DB_VALUE_FROM_ARROW_ARRAY_AND_SCHEMA_FUNC                               \
  [&]() {                                                                                \
    switch (child_schema->format[0]) {                                                   \
      case 'b': {                                                                        \
        return ::duckdb::Value(CiderBitUtils::isBitSetAt(                                \
            static_cast<const uint8_t*>(child_array->buffers[1]), row_idx));             \
      }                                                                                  \
      case 'c': {                                                                        \
        return duckValueAt<int8_t>(static_cast<const int8_t*>(child_array->buffers[1]),  \
                                   row_idx);                                             \
      }                                                                                  \
      case 's': {                                                                        \
        return duckValueAt<int16_t>(static_cast<const int8_t*>(child_array->buffers[1]), \
                                    row_idx);                                            \
      }                                                                                  \
      case 'i': {                                                                        \
        return duckValueAt<int32_t>(static_cast<const int8_t*>(child_array->buffers[1]), \
                                    row_idx);                                            \
      }                                                                                  \
      case 'l': {                                                                        \
        return duckValueAt<int64_t>(static_cast<const int8_t*>(child_array->buffers[1]), \
                                    row_idx);                                            \
      }                                                                                  \
      case 'f': {                                                                        \
        return duckValueAt<float>(static_cast<const int8_t*>(child_array->buffers[1]),   \
                                  row_idx);                                              \
      }                                                                                  \
      case 'g': {                                                                        \
        return duckValueAt<double>(static_cast<const int8_t*>(child_array->buffers[1]),  \
                                   row_idx);                                             \
      }                                                                                  \
      case 't': {                                                                        \
        if (child_schema->format[1] == 'd' && child_schema->format[2] == 'D') {          \
          return duckValueAt<int32_t>(                                                   \
              static_cast<const int8_t*>(child_array->buffers[1]), row_idx);             \
        } else if (child_schema->format[1] == 't' && child_schema->format[2] == 'u') {   \
          return duckValueAt<int64_t>(                                                   \
              static_cast<const int8_t*>(child_array->buffers[1]), row_idx);             \
        } else if (child_schema->format[1] == 's' && child_schema->format[2] == 'u') {   \
          return duckValueAt<int64_t>(                                                   \
              static_cast<const int8_t*>(child_array->buffers[1]), row_idx);             \
        }                                                                                \
        CIDER_THROW(CiderException, "not supported time type to gen duck value");        \
      }                                                                                  \
      case 'u': {                                                                        \
        if (child_array->dictionary) {                                                   \
          int32_t offset = ((int32_t*)(child_array->buffers[1]))[row_idx];               \
          return duckValueVarcharAt(                                                     \
              static_cast<const int8_t*>(child_array->dictionary->buffers[2]),           \
              static_cast<const int32_t*>(child_array->dictionary->buffers[1]),          \
              offset);                                                                   \
        } else {                                                                         \
          return duckValueVarcharAt(                                                     \
              static_cast<const int8_t*>(child_array->buffers[2]),                       \
              static_cast<const int32_t*>(child_array->buffers[1]),                      \
              row_idx);                                                                  \
        }                                                                                \
      }                                                                                  \
      default:                                                                           \
        CIDER_THROW(CiderException, "not supported type to gen duck value");             \
    }                                                                                    \
  }

void DuckDbQueryRunner::createTableAndInsertArrowData(const std::string& table_name,
                                                      const std::string& create_ddl,
                                                      const ArrowArray& array,
                                                      const ArrowSchema& schema) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + table_name);

  auto res = con.Query(create_ddl);
  if (!res->success) {
    LOG(ERROR) << res->error;
  }

  for (int row_idx = 0; row_idx < array.length; ++row_idx) {
    ::duckdb::Appender appender(con, table_name);
    appender.BeginRow();
    for (int col_idx = 0; col_idx < array.n_children; ++col_idx) {
      auto child_array = array.children[col_idx];
      auto child_schema = schema.children[col_idx];
      const uint8_t* valid_bitmap = static_cast<const uint8_t*>(child_array->buffers[0]);
      if (!valid_bitmap || CiderBitUtils::isBitSetAt(valid_bitmap, row_idx)) {
        ::duckdb::Value value = GEN_DUCK_DB_VALUE_FROM_ARROW_ARRAY_AND_SCHEMA_FUNC();
        appender.Append(value);
      } else {
        appender.Append(nullptr);
      }
    }
    appender.EndRow();
  }
}

std::unique_ptr<::duckdb::MaterializedQueryResult> DuckDbQueryRunner::runSql(
    const std::string& sql) {
  ::duckdb::Connection con(db_);
  auto duck_result = con.Query(sql);
  CHECK(duck_result->success) << "DuckDB query failed: " << duck_result->error << "\n"
                              << sql;
  return duck_result;
}

namespace {
size_t getTypeSize(const ::duckdb::LogicalType& type) {
  switch (type.id()) {
    case ::duckdb::LogicalTypeId::TINYINT:
    case ::duckdb::LogicalTypeId::UTINYINT:
    case ::duckdb::LogicalTypeId::BOOLEAN:
      return 1;
    case ::duckdb::LogicalTypeId::SMALLINT:
    case ::duckdb::LogicalTypeId::USMALLINT:
      return 2;
    case ::duckdb::LogicalTypeId::INTEGER:
    case ::duckdb::LogicalTypeId::UINTEGER:
    case ::duckdb::LogicalTypeId::FLOAT:
      return 4;
    case ::duckdb::LogicalTypeId::BIGINT:
    case ::duckdb::LogicalTypeId::UBIGINT:
    case ::duckdb::LogicalTypeId::DOUBLE:
    case ::duckdb::LogicalTypeId::DECIMAL:
    case ::duckdb::LogicalTypeId::HUGEINT:
    case ::duckdb::LogicalTypeId::DATE:
    case ::duckdb::LogicalTypeId::TIME:
    case ::duckdb::LogicalTypeId::TIMESTAMP:
      return 8;
    case ::duckdb::LogicalTypeId::CHAR:
    case ::duckdb::LogicalTypeId::VARCHAR:
      return 16;  // sizeof(CiderByteArray)
    default:
      CIDER_THROW(CiderCompileException, "Unsupported type: " + type.ToString());
  }
}

substrait::Type convertToSubstraitType(const ::duckdb::LogicalType& type) {
  switch (type.id()) {
    case ::duckdb::LogicalTypeId::BOOLEAN:
      return CREATE_SUBSTRAIT_TYPE(Bool);
    case ::duckdb::LogicalTypeId::TINYINT:
      return CREATE_SUBSTRAIT_TYPE(I8);
    case ::duckdb::LogicalTypeId::SMALLINT:
      return CREATE_SUBSTRAIT_TYPE(I16);
    case ::duckdb::LogicalTypeId::INTEGER:
      return CREATE_SUBSTRAIT_TYPE(I32);
    case ::duckdb::LogicalTypeId::BIGINT:
    case ::duckdb::LogicalTypeId::HUGEINT:
      return CREATE_SUBSTRAIT_TYPE(I64);
    case ::duckdb::LogicalTypeId::FLOAT:
      return CREATE_SUBSTRAIT_TYPE(Fp32);
    case ::duckdb::LogicalTypeId::DOUBLE:
      return CREATE_SUBSTRAIT_TYPE(Fp64);
    case ::duckdb::LogicalTypeId::DATE:
      return CREATE_SUBSTRAIT_TYPE(Date);
    case ::duckdb::LogicalTypeId::TIME:
      return CREATE_SUBSTRAIT_TYPE(Time);
    case ::duckdb::LogicalTypeId::TIMESTAMP:
      return CREATE_SUBSTRAIT_TYPE(Timestamp);
    case ::duckdb::LogicalTypeId::CHAR:
      return CREATE_SUBSTRAIT_TYPE(FixedChar);
    case ::duckdb::LogicalTypeId::VARCHAR:
      return CREATE_SUBSTRAIT_TYPE(Varchar);
    case ::duckdb::LogicalTypeId::DECIMAL:
      return CREATE_SUBSTRAIT_TYPE(Decimal);
    default:
      CIDER_THROW(CiderCompileException, "Unsupported type: " + type.ToString());
  }
}

bool isDuckDbTimingType(const ::duckdb::LogicalType& type) {
  return type.id() == ::duckdb::LogicalTypeId::DATE ||
         type.id() == ::duckdb::LogicalTypeId::TIME ||
         type.id() == ::duckdb::LogicalTypeId::TIMESTAMP;
}

bool isPrimitiveType(const ::duckdb::LogicalType& type) {
  return type.IsNumeric() || type.id() == ::duckdb::LogicalTypeId::BOOLEAN;
}

bool isStringType(const ::duckdb::LogicalType& type) {
  return type.id() == ::duckdb::LogicalTypeId::VARCHAR ||
         type.id() == ::duckdb::LogicalTypeId::CHAR;
}

bool isDuckDb2CiderBatchSupportType(const ::duckdb::LogicalType& type) {
  return isPrimitiveType(type) || isDuckDbTimingType(type) || isStringType(type);
}
}  // namespace

void updateChildrenNullCnt(struct ArrowArray* array) {
  int64_t length = array->length;
  for (int i = 0; i < array->n_children; i++) {
    int null_count = 0;
    auto child = array->children[i];
    const uint8_t* validity_map = reinterpret_cast<const uint8_t*>(child->buffers[0]);
    if (validity_map) {
      child->null_count = length - CiderBitUtils::countSetBits(validity_map, length);
    }
  }
}

std::vector<
    std::pair<std::unique_ptr<struct ArrowArray>, std::unique_ptr<struct ArrowSchema>>>
DuckDbResultConvertor::fetchDataToArrow(
    std::unique_ptr<::duckdb::MaterializedQueryResult>& result) {
  std::vector<
      std::pair<std::unique_ptr<struct ArrowArray>, std::unique_ptr<struct ArrowSchema>>>
      arrow_vector;
  for (;;) {
    std::unique_ptr<struct ArrowArray> arrow_array =
        std::make_unique<struct ArrowArray>();
    std::unique_ptr<struct ArrowSchema> arrow_schema =
        std::make_unique<struct ArrowSchema>();
    auto chunk = result->Fetch();
    if (!chunk || (chunk->size() == 0)) {
      return arrow_vector;
    }
    chunk->ToArrowArray(arrow_array.get());
    updateChildrenNullCnt(arrow_array.get());
    std::string config_timezone{"UTC"};
    duckdb::QueryResult::ToArrowSchema(
        arrow_schema.get(), result->types, result->names, config_timezone);
    arrow_vector.push_back(
        std::make_pair(std::move(arrow_array), std::move(arrow_schema)));
  }
  return arrow_vector;
}
