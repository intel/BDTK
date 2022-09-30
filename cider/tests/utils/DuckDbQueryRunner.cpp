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

#include "DuckDbQueryRunner.h"
#include "Utils.h"
#include "cider/CiderTypes.h"
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
          auto s_type = current_batch->schema()->getColumnTypeById(k);
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
        auto s_type = current_batch->schema()->getColumnTypeById(k);
        auto value = GEN_DUCK_VALUE_FUNC();
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
