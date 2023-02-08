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

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

namespace {

substrait::Type convertToSubstraitType(const arrow::Type::type tag) {
  switch (tag) {
    case arrow::Type::type::BOOL:
      return CREATE_SUBSTRAIT_TYPE(Bool);
    case arrow::Type::type::INT8:
      return CREATE_SUBSTRAIT_TYPE(I8);
    case arrow::Type::type::INT16:
      return CREATE_SUBSTRAIT_TYPE(I16);
    case arrow::Type::type::INT32:
      return CREATE_SUBSTRAIT_TYPE(I32);
    case arrow::Type::type::INT64:
      return CREATE_SUBSTRAIT_TYPE(I64);
    case arrow::Type::type::FLOAT:
      return CREATE_SUBSTRAIT_TYPE(Fp32);
    case arrow::Type::type::DOUBLE:
      return CREATE_SUBSTRAIT_TYPE(Fp64);
    case arrow::Type::type::STRING:
      return CREATE_SUBSTRAIT_TYPE(Varchar);
    case arrow::Type::type::DATE32:
      return CREATE_SUBSTRAIT_TYPE(Date);
    case arrow::Type::type::TIME64:
      return CREATE_SUBSTRAIT_TYPE(Time);
    case arrow::Type::type::TIMESTAMP:
      return CREATE_SUBSTRAIT_TYPE(Timestamp);
    default:
      CIDER_THROW(CiderRuntimeException, "Failed to convert arrow type: " + tag);
  }
}

void constructArrowArray(ArrowArrayBuilder& builder,
                         std::shared_ptr<arrow::ChunkedArray> chunk_arrays,
                         const std::string& name) {
  for (int i = 0; i < chunk_arrays->num_chunks(); i++) {
    auto arrow_array = chunk_arrays->chunk(i);
    auto tag = arrow_array->type_id();
    int64_t buffer_num = arrow_array->data()->buffers.size();
    switch (buffer_num) {
      case 2:
        builder.addColumn(name,
                          convertToSubstraitType(tag),
                          arrow_array->null_bitmap_data(),
                          arrow_array->data()->buffers[1]->data());
        break;
      case 3:
        builder.addColumn(name,
                          convertToSubstraitType(tag),
                          arrow_array->null_bitmap_data(),
                          arrow_array->data()->buffers[1]->data(),
                          arrow_array->data()->buffers[2]->data());
        break;
      default:
        CIDER_THROW(CiderRuntimeException,
                    "Failed to extract array buffer, buffe_num is : " + buffer_num);
    }
  }
}

}  // namespace

class ArrowDataReader {
 public:
  ArrowDataReader(const std::string& file_name,
                  const std::unordered_set<std::string>& col_names = {})
      : file_name_{file_name}, col_names_{col_names} {};
  std::shared_ptr<CiderBatch> read() {
    std::shared_ptr<arrow::Table> raw_table = constructArrowTable();
    // Combine chunks for workaround large volume dataset.
    std::shared_ptr<arrow::Table> table =
        std::move(raw_table->CombineChunks()).ValueOrDie();
    int64_t rows = table->num_rows();
    int64_t columns = table->num_columns();
    auto builder = ArrowArrayBuilder().setRowNum(rows);
    bool col_selected = col_names_.size() > 0 ? true : false;
    for (int i = 0; i < table->num_columns(); i++) {
      const std::string col_name = table->schema()->field(i)->name();
      if (col_selected && col_names_.find(col_name) == col_names_.end()) {
        continue;
      }
      std::shared_ptr<arrow::ChunkedArray> field = table->column(i);
      constructArrowArray(builder, field, col_name);
    }
    auto schema_and_array = builder.build();
    return std::make_shared<CiderBatch>(std::get<0>(schema_and_array),
                                        std::get<1>(schema_and_array),
                                        std::make_shared<CiderDefaultAllocator>());
  }
  virtual std::shared_ptr<arrow::Table> constructArrowTable() = 0;

 protected:
  const std::string file_name_;
  const std::unordered_set<std::string> col_names_;
};

class CSVToArrowDataReader : public ArrowDataReader {
 public:
  CSVToArrowDataReader(const std::string& file_name,
                       const std::unordered_set<std::string>& col_names = {})
      : ArrowDataReader(file_name, col_names) {}
  std::shared_ptr<arrow::Table> constructArrowTable() {
    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto open_file = arrow::io::ReadableFile::Open(file_name_);
    if (!open_file.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to open file " + file_name_);
    }
    std::shared_ptr<arrow::io::InputStream> input = std::move(open_file).ValueOrDie();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    // Instantiate TableReader from input stream and options
    auto maybe_reader = arrow::csv::TableReader::Make(
        io_context, input, read_options, parse_options, convert_options);
    if (!maybe_reader.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to read file " + file_name_);
    }
    std::shared_ptr<arrow::csv::TableReader> reader =
        std::move(maybe_reader).ValueOrDie();
    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to load data in " + file_name_);
    }
    return std::move(maybe_table).ValueOrDie();
  }
};

class ParquetToArrowDataReader : public ArrowDataReader {
 public:
  ParquetToArrowDataReader(const std::string& file_name,
                           const std::unordered_set<std::string>& col_names = {})
      : ArrowDataReader(file_name, col_names) {}

  std::shared_ptr<arrow::Table> constructArrowTable() {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto open_file = arrow::io::ReadableFile::Open(file_name_);
    if (!open_file.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to open file " + file_name_);
    }
    std::shared_ptr<arrow::io::RandomAccessFile> input =
        std::move(open_file).ValueOrDie();
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    auto status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    if (!status.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to read file " + file_name_);
    }
    std::shared_ptr<arrow::Table> table;
    status = arrow_reader->ReadTable(&table);
    if (!status.ok()) {
      CIDER_THROW(CiderRuntimeException, "Failed to load data in " + file_name_);
    }
    return table;
  }
};
