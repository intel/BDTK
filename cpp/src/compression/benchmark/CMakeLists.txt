# Copyright(c) 2022-2023 Intel Corporation.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

project(icl-benchmark)

find_package(Arrow REQUIRED)
if(${Arrow_FOUND})
  message(STATUS "Arrow library found: ${ARROW_STATIC_LIB}")
endif()

find_package(ArrowTesting REQUIRED)
if(${ArrowTesting_FOUND})
  message(STATUS "ArrowTesting library found: ${ARROW_TESTING_STATIC_LIB}")
endif()

find_package(Parquet REQUIRED)
if(${Parquet_FOUND})
  message(STATUS "Parquet library found: ${PARQUET_STATIC_LIB}")
endif()

set(COMPRESSION_BENCHMARK_SRCS BenchmarkMain.cpp CompressionBenchmark.cpp)
add_executable(CompressionBenchmark ${COMPRESSION_BENCHMARK_SRCS})
target_link_libraries(CompressionBenchmark benchmark::benchmark gtest
                      arrow_shared)

set(PARQUET_BENCHMARK_SRCS BenchmarkMain.cpp ParquetBenchmark.cpp)
add_executable(ParquetBenchmark ${PARQUET_BENCHMARK_SRCS})
target_link_libraries(ParquetBenchmark benchmark::benchmark gtest
                      parquet_shared arrow_shared arrow_testing_shared)
