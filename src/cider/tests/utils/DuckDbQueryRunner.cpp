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

#include <memory>
#include <utility>
#include "DuckDbArrowAdaptor.h"
#include "DuckDbQueryRunner.h"
#include "Utils.h"
#include "cider/CiderTypes.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "exec/plan/parser/TypeUtils.h"
#include "util/Logger.h"

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

void DuckDbQueryRunner::createTableAndInsertData(
    const std::string& table_name,
    const std::string& create_ddl,
    const std::vector<std::vector<int32_t>>& table_data,
    bool use_tpch_schema,
    const std::vector<std::vector<bool>>& null_data) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + table_name);
  std::string create_table_sql;
  if (use_tpch_schema) {
    create_table_sql = TpcHTableCreator::createTpcHTables(table_name);
  } else {
    create_table_sql = create_ddl;
  }

  auto res = con.Query(create_table_sql);
  if (!res->success) {
    LOG(ERROR) << res->error;
  }
  if (null_data.empty()) {
    appendTableData(con, table_name, table_data);
  } else {
    appendNullableTableData(con, table_name, table_data, null_data);
  }
}

void DuckDbQueryRunner::appendTableData(
    ::duckdb::Connection& con,
    const std::string& table_name,
    const std::vector<std::vector<int32_t>>& table_data) {
  const int col_num = table_data.size();
  CHECK_GT(col_num, 0);
  const int row_num = table_data[0].size();

  for (int i = 0; i < row_num; ++i) {
    ::duckdb::Appender appender(con, table_name);
    appender.BeginRow();
    for (int j = 0; j < col_num; ++j) {
      int32_t value = table_data[j][i];
      appender.Append(value);
    }
    appender.EndRow();
  }
}

void DuckDbQueryRunner::appendNullableTableData(
    ::duckdb::Connection& con,
    const std::string& table_name,
    const std::vector<std::vector<int32_t>>& table_data,
    const std::vector<std::vector<bool>>& null_data) {
  const int col_num = table_data.size();
  CHECK_GT(col_num, 0);
  CHECK_EQ(col_num, null_data.size());
  const int row_num = table_data[0].size();

  for (int i = 0; i < row_num; ++i) {
    ::duckdb::Appender appender(con, table_name);
    appender.BeginRow();
    for (int j = 0; j < col_num; ++j) {
      if (null_data.empty() || null_data[j].empty() ||  // this column is not nullable
          null_data[j][i]) {                            // ture is null, false is non-null
        int32_t value = table_data[j][i];
        appender.Append(value);
      } else {
        appender.Append(nullptr);
      }
    }
    appender.EndRow();
  }
}

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

template <typename T>
::duckdb::Value duckDbValueAtScalarBatch(const ScalarBatch<T>* batch, int64_t offset) {
  if (!batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr. Maybe check your casting?");
  }

  auto data_buffer = batch->getRawData();
  return ::duckdb::Value(data_buffer[offset]);
}

template <>
::duckdb::Value duckDbValueAtScalarBatch<bool>(const ScalarBatch<bool>* batch,
                                               int64_t offset) {
  if (!batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr. Maybe check your casting?");
  }

  auto data_buffer = batch->getRawData();
  return ::duckdb::Value(CiderBitUtils::isBitSetAt(data_buffer, offset));
}

::duckdb::Value duckDbValueAtVarcharBatch(const VarcharBatch* batch, int64_t offset) {
  auto data_buffer = batch->getRawData();
  auto offset_buffer = batch->getRawOffset();
  auto len = offset_buffer[offset + 1] - offset_buffer[offset];
  char copy[len + 1];
  memcpy(&copy, &data_buffer[offset_buffer[offset]], len);
  copy[len] = '\0';
  return ::duckdb::Value(std::string(copy));
}

#define GEN_DUCK_DB_VALUE_FROM_ARROW_FUNC                                               \
  [&]() {                                                                               \
    switch (child_type) {                                                               \
      case SQLTypes::kBOOLEAN:                                                          \
        return duckDbValueAtScalarBatch<bool>(child->as<ScalarBatch<bool>>(), row_idx); \
      case SQLTypes::kTINYINT:                                                          \
        return duckDbValueAtScalarBatch<int8_t>(child->as<ScalarBatch<int8_t>>(),       \
                                                row_idx);                               \
      case SQLTypes::kSMALLINT:                                                         \
        return duckDbValueAtScalarBatch<int16_t>(child->as<ScalarBatch<int16_t>>(),     \
                                                 row_idx);                              \
      case SQLTypes::kINT:                                                              \
        return duckDbValueAtScalarBatch<int32_t>(child->as<ScalarBatch<int32_t>>(),     \
                                                 row_idx);                              \
      case SQLTypes::kBIGINT:                                                           \
        return duckDbValueAtScalarBatch<int64_t>(child->as<ScalarBatch<int64_t>>(),     \
                                                 row_idx);                              \
      case SQLTypes::kFLOAT:                                                            \
        return duckDbValueAtScalarBatch<float>(child->as<ScalarBatch<float>>(),         \
                                               row_idx);                                \
      case SQLTypes::kDOUBLE:                                                           \
        return duckDbValueAtScalarBatch<double>(child->as<ScalarBatch<double>>(),       \
                                                row_idx);                               \
      case SQLTypes::kVARCHAR:                                                          \
        return duckDbValueAtVarcharBatch(child->as<VarcharBatch>(), row_idx);           \
      case SQLTypes::kDATE:                                                             \
        return duckDbValueAtScalarBatch<int32_t>(child->as<ScalarBatch<int32_t>>(),     \
                                                 row_idx);                              \
      case SQLTypes::kTIME:                                                             \
        return duckDbValueAtScalarBatch<int64_t>(child->as<ScalarBatch<int64_t>>(),     \
                                                 row_idx);                              \
      case SQLTypes::kTIMESTAMP:                                                        \
        return duckDbValueAtScalarBatch<int64_t>(child->as<ScalarBatch<int64_t>>(),     \
                                                 row_idx);                              \
      default:                                                                          \
        CIDER_THROW(CiderUnsupportedException,                                          \
                    "Unsupported type for converting to duckdb values.");               \
    }                                                                                   \
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

#define GEN_DUCK_DB_VALUE_FROM_ARROW_ARRAY_AND_SCHEMA_FUNC                               \
  [&]() {                                                                                \
    switch (child_schema->format[0]) {                                                   \
      case 'b': {                                                                        \
        return ::duckdb::Value(CiderBitUtils::isBitSetAt(                              \
            static_cast<const uint8_t*>(child_array->buffers[1]), row_idx));              \
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
        }                                                                                \
      }                                                                                  \
      default:                                                                           \
        CIDER_THROW(CiderException, "not supported type to gen duck value");             \
    }                                                                                    \
  }

void DuckDbQueryRunner::createTableAndInsertData(
    const std::string& table_name,
    const std::string& create_ddl,
    const std::vector<std::shared_ptr<CiderBatch>>& data) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + table_name);

  auto res = con.Query(create_ddl);
  if (!res->success) {
    LOG(ERROR) << res->error;
  }

  for (auto i = 0; i < data.size(); ++i) {
    auto& current_batch = data[i];
    int row_num = current_batch->row_num();
    int col_num = current_batch->column_num();
    std::vector<std::vector<bool>> null_data = current_batch->get_null_vecs();
    for (int j = 0; j < row_num; ++j) {
      ::duckdb::Appender appender(con, table_name);
      appender.BeginRow();
      for (int k = 0; k < col_num; ++k) {
        if (null_data.empty() || null_data[k].empty() ||  // this column is not nullable
            !null_data[k][j]) {  // ture is null, false is non-null
          auto& s_type = current_batch->schema()->getColumnTypeById(k);
          auto value = GEN_DUCK_VALUE_FUNC();
          appender.Append(value);
        } else {
          appender.Append(nullptr);
        }
      }
      appender.EndRow();
    }
  }
}

void DuckDbQueryRunner::createTableAndInsertData(
    const std::string& table_name,
    const std::string& create_ddl,
    const std::shared_ptr<CiderBatch>& data) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + table_name);

  auto res = con.Query(create_ddl);
  if (!res->success) {
    LOG(ERROR) << res->error;
  }

  auto& current_batch = data;
  int row_num = current_batch->row_num();
  int col_num = current_batch->column_num();
  std::vector<std::vector<bool>> null_data = current_batch->get_null_vecs();
  for (int j = 0; j < row_num; ++j) {
    ::duckdb::Appender appender(con, table_name);
    appender.BeginRow();
    for (int k = 0; k < col_num; ++k) {
      if (null_data[k].empty() ||  // this column is not nullable
          !null_data[k][j]) {
        auto& s_type = current_batch->schema()->getColumnTypeById(k);
        auto value = GEN_DUCK_VALUE_FUNC();
        appender.Append(value);
      } else {
        appender.Append(nullptr);
      }
    }
    appender.EndRow();
  }
}

void DuckDbQueryRunner::createTableAndInsertArrowData(
    const std::string& table_name,
    const std::string& create_ddl,
    const std::vector<std::shared_ptr<CiderBatch>>& data) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + table_name);

  auto res = con.Query(create_ddl);
  if (!res->success) {
    LOG(ERROR) << res->error;
  }

  for (auto i = 0; i < data.size(); ++i) {
    auto& current_batch = data[i];
    int row_num = current_batch->getLength();
    int col_num = current_batch->getChildrenNum();
    for (int row_idx = 0; row_idx < row_num; ++row_idx) {
      ::duckdb::Appender appender(con, table_name);
      appender.BeginRow();
      for (int col_idx = 0; col_idx < col_num; ++col_idx) {
        auto child = current_batch->getChildAt(col_idx);
        auto child_type = child->getCiderType();
        const uint8_t* valid_bitmap = child->getNulls();
        if (!valid_bitmap || CiderBitUtils::isBitSetAt(valid_bitmap, row_idx)) {
          ::duckdb::Value value = GEN_DUCK_DB_VALUE_FROM_ARROW_FUNC();
          appender.Append(value);
        } else {
          appender.Append(nullptr);
        }
      }
      appender.EndRow();
    }
  }
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

template <typename T>
void addColumnDataToCiderBatch(std::unique_ptr<duckdb::DataChunk>& dataChunk,
                               int i,
                               int row_num,
                               int8_t* col_buf) {
  T type_null_value = std::numeric_limits<T>::min();
  for (int j = 0; j < row_num; j++) {
    T value = dataChunk->GetValue(i, j).IsNull()
                  ? type_null_value
                  : dataChunk->GetValue(i, j).GetValue<T>();
    std::memcpy(col_buf + j * sizeof(T), &value, sizeof(T));
  }
}

template <>
void addColumnDataToCiderBatch<CiderDateType>(
    std::unique_ptr<duckdb::DataChunk>& dataChunk,
    int i,
    int row_num,
    int8_t* col_buf) {
  int64_t date_null_value = std::numeric_limits<int64_t>::min();
  for (int j = 0; j < row_num; j++) {
    int64_t value =
        dataChunk->GetValue(i, j).IsNull()
            ? date_null_value
            : static_cast<int64_t>(dataChunk->GetValue(i, j).GetValue<int32_t>()) *
                  kSecondsInOneDay;
    std::memcpy(col_buf + j * sizeof(int64_t), &value, sizeof(int64_t));
  }
}

template <>
void addColumnDataToCiderBatch<CiderByteArray>(
    std::unique_ptr<duckdb::DataChunk>& dataChunk,
    int i,
    int row_num,
    int8_t* col_buf) {
  CiderByteArray* buf = (CiderByteArray*)col_buf;
  for (int j = 0; j < row_num; j++) {
    if (dataChunk->GetValue(i, j).IsNull()) {
      std::memset(col_buf + j * sizeof(CiderByteArray), 0, sizeof(CiderByteArray));
    } else {
      std::string value = dataChunk->GetValue(i, j).GetValue<std::string>();
      // memory leak risk. user should manually free.
      uint8_t* value_buf =
          reinterpret_cast<uint8_t*>(allocator->allocate(value.length()));
      std::memcpy(value_buf, value.c_str(), value.length());
      buf[j].len = value.length();
      buf[j].ptr = value_buf;
    }
  }
}

void DuckDbResultConvertor::updateChildrenNullCounts(CiderBatch& batch) {
  for (int i = 0; i < batch.getChildrenNum(); ++i) {
    auto child = batch.getChildAt(i);
    const uint8_t* validity_map = child->getNulls();
    int null_count = 0;
    if (validity_map) {
      for (int j = 0; j < batch.getLength(); ++j) {
        if (!CiderBitUtils::isBitSetAt(validity_map, j)) {
          // 0 stands for a null value
          ++null_count;
        }
      }
    } else {
      CHECK_EQ(child->getNullCount(), 0);
    }
    child->setNullCount(null_count);
  }
}

CiderBatch DuckDbResultConvertor::fetchOneArrowFormattedBatch(
    std::unique_ptr<duckdb::DataChunk>& chunk,
    std::vector<std::string>& names) {
  int col_num = chunk->ColumnCount();
  int row_num = chunk->size();

  // Construct ArrowSchema using methods from duckdb
  std::vector<::duckdb::LogicalType> types = chunk->GetTypes();
  auto arrow_schema = CiderBatchUtils::allocateArrowSchema();
  DuckDbArrowSchemaAdaptor::duckdbResultSchemaToArrowSchema(arrow_schema, types, names);

  // Construct ArrowArray
  auto arrow_array = CiderBatchUtils::allocateArrowArray();
  chunk->ToArrowArray(arrow_array);

  // Build CiderBatch
  auto batch = StructBatch::Create(
      arrow_schema, std::make_shared<CiderDefaultAllocator>(), arrow_array);

  // ToArrowArray() will not compute null_count, so we do it manually here
  updateChildrenNullCounts(*batch);

  chunk->Destroy();
  return std::move(*batch);
}

CiderBatch DuckDbResultConvertor::fetchOneBatch(
    std::unique_ptr<duckdb::DataChunk>& chunk) {
  const int col_num = chunk->ColumnCount();
  const int row_num = chunk->size();
  auto types = chunk->GetTypes();
  std::vector<const int8_t*> table_ptr;
  std::vector<::substrait::Type> batch_types;
  std::vector<std::string> batch_names;
  std::vector<size_t> column_size;
  for (int i = 0; i < col_num; i++) {
    ::duckdb::LogicalType type = types[i];
    batch_types.emplace_back(std::move(convertToSubstraitType(type)));
    batch_names.emplace_back("");
    if (!isDuckDb2CiderBatchSupportType(type)) {
      CIDER_THROW(CiderCompileException,
                  "Non numberic type " + type.ToString() + " not supported yet");
    }
    // allocate memory according to type.
    column_size.push_back(getTypeSize(type));
  }
  auto schema = std::make_shared<CiderTableSchema>(batch_names, batch_types, "");
  CiderBatch ciderBatch(row_num, column_size, nullptr, schema);

  for (int i = 0; i < col_num; i++) {
    ::duckdb::LogicalType type = types[i];
    if (!isDuckDb2CiderBatchSupportType(type)) {
      CIDER_THROW(CiderCompileException,
                  "Non numberic type " + type.ToString() + " not supported yet");
    }
    // allocate memory according to type.
    auto type_size = getTypeSize(type);
    int8_t* col_buf = const_cast<int8_t*>(ciderBatch.column(i));
    switch (type.id()) {
      case ::duckdb::LogicalTypeId::HUGEINT:
      case ::duckdb::LogicalTypeId::BIGINT:
      case ::duckdb::LogicalTypeId::TIMESTAMP:
      case ::duckdb::LogicalTypeId::TIME:
        addColumnDataToCiderBatch<int64_t>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::INTEGER:
        addColumnDataToCiderBatch<int32_t>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::SMALLINT:
        addColumnDataToCiderBatch<int16_t>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::BOOLEAN:
      case ::duckdb::LogicalTypeId::TINYINT:
        addColumnDataToCiderBatch<int8_t>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::FLOAT:
        addColumnDataToCiderBatch<float>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::DOUBLE:
      case ::duckdb::LogicalTypeId::DECIMAL:
        addColumnDataToCiderBatch<double>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::DATE:
        addColumnDataToCiderBatch<CiderDateType>(chunk, i, row_num, col_buf);
        break;
      case ::duckdb::LogicalTypeId::CHAR:
      case ::duckdb::LogicalTypeId::VARCHAR:
        addColumnDataToCiderBatch<CiderByteArray>(chunk, i, row_num, col_buf);
        break;
      default:
        CIDER_THROW(CiderCompileException,
                    "Unsupported type convertor: " + type.ToString());
    }
  }
  chunk->Destroy();
  return std::move(ciderBatch);
}

std::vector<std::shared_ptr<CiderBatch>> DuckDbResultConvertor::fetchDataToCiderBatch(
    std::unique_ptr<::duckdb::MaterializedQueryResult>& result) {
  std::vector<std::shared_ptr<CiderBatch>> batch_res;
  for (;;) {
    auto chunk = result->Fetch();
    if (!chunk || (chunk->size() == 0)) {
      if (batch_res.empty()) {
        batch_res.push_back(std::make_shared<CiderBatch>());
      }
      return batch_res;
    }
    batch_res.push_back(std::make_shared<CiderBatch>(fetchOneBatch(chunk)));
  }
  return batch_res;
}

std::vector<std::shared_ptr<CiderBatch>>
DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(
    std::unique_ptr<::duckdb::MaterializedQueryResult>& result) {
  auto names = result->names;
  std::vector<std::shared_ptr<CiderBatch>> batch_res;
  for (;;) {
    auto chunk = result->Fetch();
    if (!chunk || (chunk->size() == 0)) {
      if (batch_res.empty()) {
        auto arrow_array = CiderBatchUtils::allocateArrowArray();
        arrow_array->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

        auto arrow_schema = CiderBatchUtils::allocateArrowSchema();
        arrow_schema->format = "+s";
        arrow_schema->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;

        batch_res.push_back(std::make_shared<CiderBatch>(*(new CiderBatch(
            arrow_schema, arrow_array, std::make_shared<CiderDefaultAllocator>()))));
      }
      return batch_res;
    }
    batch_res.push_back(
        std::make_shared<CiderBatch>(fetchOneArrowFormattedBatch(chunk, names)));
  }
  return batch_res;
}

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
