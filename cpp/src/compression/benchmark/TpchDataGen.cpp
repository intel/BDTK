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

#include <iostream>
#include <string>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/tpch_node.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

namespace {

arrow::Status GenerateTpchLinitemForParquet(const std::string& filename,
                                            const std::string& codec,
                                            int level,
                                            double scale_factor) {
  if (setenv("GZIP_BACKEND", codec.c_str(), 1) != 0) {
    std::cout << "Set `GZIP_BACKEND` failed, use default backend" << std::endl;
  }

  ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(filename));

  parquet::WriterProperties::Builder builder;
  builder.compression(parquet::Compression::GZIP);
  builder.compression_level(level);
  std::shared_ptr<parquet::WriterProperties> props = builder.build();

  arrow::compute::ExecContext ctx(arrow::default_memory_pool(),
                                  arrow::internal::GetCpuThreadPool());
  ARROW_ASSIGN_OR_RAISE(auto plan, arrow::compute::ExecPlan::Make(&ctx));
  ARROW_ASSIGN_OR_RAISE(
      auto gen, arrow::compute::internal::TpchGen::Make(plan.get(), scale_factor));
  arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto table_node, ((gen->Lineitem)({})));
  arrow::compute::Declaration sink("sink",
                                   {arrow::compute::Declaration::Input(table_node)},
                                   arrow::compute::SinkNodeOptions{&sink_gen});
  ARROW_RETURN_NOT_OK(sink.AddToPlan(plan.get()));
  auto fut = StartAndCollect(plan.get(), sink_gen);
  ARROW_ASSIGN_OR_RAISE(auto lineitem, fut.MoveResult());

  auto schema = table_node->output_schema();
  std::cout << "schema:\n" << schema->ToString() << std::endl;
  std::cout << std::endl;

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (auto& batch : lineitem) {
    ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema));
    batches.push_back(rb);
  }
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make(batches));
  ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, 64 * 1024 * 1024, props));

  return arrow::Status::OK();
}

class ParquetReader {
 public:
  explicit ParquetReader(const std::string& file_name) {
    PARQUET_ASSIGN_OR_THROW(
        parquet_file_,
        arrow::io::ReadableFile::Open(file_name, arrow::default_memory_pool()));
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(parquet_file_, arrow::default_memory_pool(), &reader_));
  }

  void read() {
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader_->ReadTable(&table));
  }

  void dump() {
    auto parquet_file_reader = reader_->parquet_reader();
    auto metadata = parquet_file_reader->metadata();
    std::cout << "total row:" << metadata->num_rows() << std::endl;
    std::cout << "total col:" << metadata->num_columns() << std::endl;
    for (auto rg_index = 0; rg_index < metadata->num_row_groups(); rg_index++) {
      std::cout << "In RowGroup:" << rg_index << std::endl;
      auto rg = metadata->RowGroup(rg_index);
      std::cout << "rg row:" << rg->num_rows() << std::endl;
      std::cout << "rg col:" << rg->num_columns() << std::endl;
      std::cout << "rg total size:" << rg->total_byte_size() << std::endl;
      std::cout << "rg compr size:" << rg->total_compressed_size() << std::endl;
      for (auto column_index = 0; column_index < rg->num_columns(); column_index++) {
        auto col = metadata->RowGroup(rg_index)->ColumnChunk(column_index);
        std::cout << "col uncompress size:" << col->total_uncompressed_size()
                  << std::endl;
        std::cout << "col   compress size:" << col->total_compressed_size() << std::endl;
      }
    }
  }

 private:
  std::shared_ptr<arrow::io::ReadableFile> parquet_file_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
};

void print_usage() {
  std::cout << R"(Usage:
    '--file'  or '-f' to specify the name for parquet file
    '--codec' or '-c' to specify the backend codec
    '--scale' or '-s' to specify the scale factor for TPCH datagen
    '--level' or '-l' to specify the compression level
    '--help'  or '-h' to print the help)"
            << std::endl;
}
}  // namespace

int main(int argc, char** argv) {
  int level = 1;
  double scale_factor = 1.0;
  std::string file = "lineitem.igzip";
  std::string codec = "igzip";

  for (auto i = 0; i < argc; i++) {
    if ((strcmp(argv[i], "--file") == 0) || (strcmp(argv[i], "-f") == 0)) {
      file = argv[i + 1];
    } else if ((strcmp(argv[i], "--codec") == 0) || (strcmp(argv[i], "-c") == 0)) {
      codec = argv[i + 1];
    } else if ((strcmp(argv[i], "--scale") == 0) || (strcmp(argv[i], "-s") == 0)) {
      scale_factor = atof(argv[i + 1]);
    } else if ((strcmp(argv[i], "--level") == 0) || (strcmp(argv[i], "-l") == 0)) {
      level = atoi(argv[i + 1]);
    } else if ((strcmp(argv[i], "--help") == 0) || (strcmp(argv[i], "-h") == 0)) {
      print_usage();
      return 0;
    }
  }

  std::cout << "file = " << file << std::endl;
  std::cout << "codec = " << codec << std::endl;
  std::cout << "scale factor = " << scale_factor << std::endl;
  std::cout << "compression level = " << level << std::endl;
  std::cout << std::endl;

  auto status = GenerateTpchLinitemForParquet(file, codec, level, scale_factor);
  if (status.ok()) {
    ParquetReader reader(file);
    reader.dump();
  }

  return 0;
}
