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

#include "benchmark/benchmark.h"

#include <iostream>
#include <string>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

class ParquetBenchamarkReader {
 public:
  ParquetBenchamarkReader(const std::string& file_name = std::string("tpch")) {
    PARQUET_ASSIGN_OR_THROW(
        parquet_file_,
        arrow::io::ReadableFile::Open(file_name, arrow::default_memory_pool()));
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(parquet_file_, arrow::default_memory_pool(), &reader_));
    readMetadata();
  }

  int64_t getTotalByteSize() const { return total_byte_size_; }

  int64_t getTotalCompressedSize() const { return total_compressed_size_; }

  void read() {
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader_->ReadTable(&table));
  }

  void readMetadata() {
    total_byte_size_ = 0;
    total_compressed_size_ = 0;
    auto parquet_file_reader = reader_->parquet_reader();
    auto metadata = parquet_file_reader->metadata();
    for (int rg_index = 0; rg_index < metadata->num_row_groups(); rg_index++) {
      auto rg = metadata->RowGroup(rg_index);
      total_byte_size_ += rg->total_byte_size();
      total_compressed_size_ += rg->total_compressed_size();
    }
  }

 private:
  std::shared_ptr<arrow::io::ReadableFile> parquet_file_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  int64_t total_compressed_size_;
  int64_t total_byte_size_;
};

static void BM_ParquetRead(::benchmark::State& state) {
  ParquetBenchamarkReader reader;
  state.counters["ratio"] = static_cast<double>(reader.getTotalByteSize()) /
                            static_cast<double>(reader.getTotalCompressedSize());

  for (auto _ : state) {
    state.PauseTiming();
    state.ResumeTiming();
    reader.read();
  }

  state.SetBytesProcessed(state.iterations() * reader.getTotalCompressedSize());
}

BENCHMARK(BM_ParquetRead);
