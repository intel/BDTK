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

#include <arrow/api.h>
#include <arrow/compute/kernel.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/stl.h>
#include <arrow/util/iterator.h>
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

void constructBuilder(substrait::Type s_type,
                      ArrowArrayBuilder& builder,
                      std::shared_ptr<arrow::ChunkedArray> field,
                      const std::string& name) {
  for (int j = 0; j < field->num_chunks(); j++) {
    auto chunk = field->chunk(j);
    builder.addColumn(
        name, s_type, chunk->null_bitmap_data(), chunk->data()->buffers[1]->data());
  }
}

void constructStringBuilder(substrait::Type s_type,
                            ArrowArrayBuilder& builder,
                            std::shared_ptr<arrow::ChunkedArray> field,
                            const std::string& name) {
  for (int j = 0; j < field->num_chunks(); j++) {
    auto chunk = field->chunk(j);
    builder.addColumn(name,
                      s_type,
                      chunk->null_bitmap_data(),
                      chunk->data()->buffers[1]->data(),
                      chunk->data()->buffers[2]->data());
  }
}

void convertToCiderBatch(ArrowArrayBuilder& builder,
                         std::shared_ptr<arrow::ChunkedArray> field,
                         const std::string& name) {
  auto tag = field->type()->id();
  switch (tag) {
    case arrow::Type::type::INT32:
      return constructBuilder(CREATE_SUBSTRAIT_TYPE(I32), builder, field, name);
    case arrow::Type::type::INT64:
      return constructBuilder(CREATE_SUBSTRAIT_TYPE(I64), builder, field, name);
    case arrow::Type::type::FLOAT:
      return constructBuilder(CREATE_SUBSTRAIT_TYPE(Fp32), builder, field, name);
    case arrow::Type::type::DOUBLE:
      return constructBuilder(CREATE_SUBSTRAIT_TYPE(Fp64), builder, field, name);
    case arrow::Type::type::STRING:
      return constructStringBuilder(CREATE_SUBSTRAIT_TYPE(Varchar), builder, field, name);
  }
}

std::shared_ptr<CiderBatch> readFromCsv(std::string csv_file,
                                        std::vector<std::string>& col_name) {
  std::shared_ptr<arrow::io::InputStream> input;
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto open_file = arrow::io::ReadableFile::Open(csv_file);
  if (open_file.ok()) {
    input = std::move(open_file).ValueOrDie();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    // Instantiate TableReader from input stream and options
    auto reader = arrow::csv::TableReader::Make(
        io_context, input, read_options, parse_options, convert_options);
    if (reader.ok()) {
      std::shared_ptr<arrow::csv::TableReader> tableReader =
          std::move(reader).ValueOrDie();
      auto t_reader = tableReader->Read();
      if (t_reader.ok()) {
        std::shared_ptr<arrow::Table> raw_table = std::move(t_reader).ValueOrDie();
        // Combine chunks for workaround large volume dataset.
        auto table = std::move(raw_table->CombineChunks()).ValueOrDie();
        int64_t rows = table->num_rows();
        int64_t columns = table->num_columns();
        auto builder = ArrowArrayBuilder().setRowNum(rows);
        for (int i = 0; i < table->num_columns(); i++) {
          std::shared_ptr<arrow::ChunkedArray> field = table->column(i);
          convertToCiderBatch(builder, field, col_name[i]);
        }
        auto schema_and_array = builder.build();
        return std::make_shared<CiderBatch>(std::get<0>(schema_and_array),
                                            std::get<1>(schema_and_array),
                                            std::make_shared<CiderDefaultAllocator>());
      }
    }
  }
}
