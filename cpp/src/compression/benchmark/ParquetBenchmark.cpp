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

#include "benchmark/benchmark.h"

#include <pthread.h>
#include <chrono>
#include <iostream>
#include <string>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

namespace {
class ParquetReader {
 public:
  explicit ParquetReader(const std::string& file_name) {
    PARQUET_ASSIGN_OR_THROW(
        parquet_file_,
        arrow::io::ReadableFile::Open(file_name, arrow::default_memory_pool()));
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(parquet_file_, arrow::default_memory_pool(), &reader_));
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

int SetCurrentThreadAffinity(int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void BM_ParquetRead(benchmark::State& state, const std::string& file_name) {
  SetCurrentThreadAffinity(state.thread_index() + 1);

  double total_seconds = 0;
  for (auto _ : state) {
    state.PauseTiming();
    ParquetReader reader(file_name);
    state.ResumeTiming();

    auto start = std::chrono::steady_clock::now();
    reader.read();
    auto end = std::chrono::steady_clock::now();
    total_seconds +=
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
  }

  ParquetReader reader(file_name);
  reader.readMetadata();

  state.SetBytesProcessed(state.iterations() * reader.getTotalByteSize());

  state.counters["bw"] =
      benchmark::Counter(state.iterations() * reader.getTotalByteSize() / total_seconds,
                         benchmark::Counter::kAvgThreads,
                         benchmark::Counter::OneK::kIs1024);

  double ratio = static_cast<double>(reader.getTotalByteSize()) /
                 static_cast<double>(reader.getTotalCompressedSize());
  state.counters["ratio"] = benchmark::Counter(
      ratio, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
}

void print_usage() {
  std::cout << R"(Usage:
    '--iter'   or '-i' to specify the number of iterations to run the benchmark
    '--thread' or '-t' to specify the number of threads to run the benchmark
    '--file'   or '-f' to specify the name of parquet file to run the benchmark
    '--codec'  or '-c' to specify the backend codec to run the benchmark
    '--help'   or '-h' to print the help)"
            << std::endl;
  return;
}

}  // namespace

int main(int argc, char** argv) {
  int iterations = 1;
  int threads = 1;
  std::string file = "lineitem.igzip";
  std::string codec = "igzip";

  for (auto i = 0; i < argc; i++) {
    if ((strcmp(argv[i], "--iter") == 0) || (strcmp(argv[i], "-i") == 0)) {
      iterations = atoi(argv[i + 1]);
    } else if ((strcmp(argv[i], "--thread") == 0) || (strcmp(argv[i], "-t") == 0)) {
      threads = atoi(argv[i + 1]);
    } else if ((strcmp(argv[i], "--file") == 0) || (strcmp(argv[i], "-f") == 0)) {
      file = argv[i + 1];
    } else if ((strcmp(argv[i], "--codec") == 0) || (strcmp(argv[i], "-c") == 0)) {
      codec = argv[i + 1];
    } else if ((strcmp(argv[i], "--help") == 0) || (strcmp(argv[i], "-h") == 0)) {
      print_usage();
      return 0;
    }
  }

  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "threads    = " << threads << std::endl;
  std::cout << "file       = " << file << std::endl;
  std::cout << "codec      = " << codec << std::endl;

  if (setenv("GZIP_BACKEND", codec.c_str(), 1) != 0) {
    std::cout << "Set `GZIP_BACKEND` failed, use default backend" << std::endl;
  }

  benchmark::RegisterBenchmark("ParquetReaderBenchmark", BM_ParquetRead, file)
      ->Iterations(iterations)
      ->Threads(threads)
      ->MeasureProcessCPUTime()
      ->UseRealTime()
      ->ReportAggregatesOnly(false)
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();

  return 0;
}
