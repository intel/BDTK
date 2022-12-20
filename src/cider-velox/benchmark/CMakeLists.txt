# Copyright (c) 2022 Intel Corporation.
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

include_directories(
  ${PROJECT_SOURCE_DIR}/velox/third_party/googletest/googletest/include)

set(Boost_USE_MULTITHREADED TRUE)
find_package(
  Boost
  1.66.0
  REQUIRED
  program_options
  context
  filesystem
  regex
  thread
  system
  date_time
  atomic)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
find_package(gflags COMPONENTS shared)
find_library(GMock gmock)
find_library(GLOG glog)
find_library(FMT fmt)
find_library(EVENT event)
find_library(DOUBLE_CONVERSION double-conversion)
find_library(LZ4 lz4)
find_library(LZO lzo2)
find_library(RE2 re2 REQUIRED)
find_library(ZSTD zstd)
find_package(ZLIB)
find_library(SNAPPY snappy)
find_package(folly CONFIG REQUIRED)
find_library(LLVM_LIB LLVM)

set(FOLLY_WITH_DEPENDENCIES
    ${FOLLY_LIBRARIES} ${Boost_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARIES}
    ${EVENT} ${SNAPPY} ${CMAKE_DL_LIBS})
set(FOLLY_BENCHMARK Folly::follybenchmark)
set(FOLLY ${FOLLY_LIBRARIES})
set(VELOX_BENCHMARKS_DEPENDENCIES
    velox_aggregates
    velox_exec
    velox_dwio_common
    velox_dwio_common_exception
    velox_dwio_parquet_reader
    velox_dwio_type_fbhive
    velox_dwrf_test_utils
    velox_hive_connector
    velox_exception
    velox_memory
    velox_process
    velox_serialization
    velox_encode
    velox_type
    velox_caching
    velox_vector
    velox_vector_fuzzer
    velox_vector_test_lib
    gtest
    gtest_main
    ${FOLLY_WITH_DEPENDENCIES}
    ${FOLLY_BENCHMARK}
    ${FMT})
set(CIDER_BENCHMARKS_SUPPLEMENT_DEPENDENCIES
    velox_plugin
    velox_substrait_convertor
    velox_functions_test_lib
    velox_aggregates_test_lib
    velox_functions_lib
    velox_functions_prestosql
    velox_serialization
    velox_functions_json
    velox_arrow_bridge
    ${Boost_ATOMIC_LIBRARIES}
    ${Boost_CONTEXT_LIBRARIES}
    ${Boost_DATE_TIME_LIBRARIES}
    ${Boost_FILESYSTEM_LIBRARIES}
    ${Boost_PROGRAM_OPTIONS_LIBRARIES}
    ${Boost_REGEX_LIBRARIES}
    ${Boost_THREAD_LIBRARIES}
    ${Boost_SYSTEM_LIBRARIES}
    ${DOUBLE_CONVERSION}
    ${gflags_LIBRARIES}
    ${GLOG}
    ${FILESYSTEM}
    ${LLVM_LIB}
    cider
    velox_plan_transformer
    cider_plan_transformer
    cider_plan_parser)

# add_executable(tpch_benchmark_velox_plugin TpchBenchmarkCompare.cpp)

# target_link_libraries(
#   tpch_benchmark_velox_plugin ${VELOX_BENCHMARKS_DEPENDENCIES}
#   ${CIDER_BENCHMARKS_SUPPLEMENT_DEPENDENCIES})

# add_executable(CiderOperatorBenchmark CiderOperatorBenchmark.cpp)
# target_link_libraries(CiderOperatorBenchmark ${VELOX_BENCHMARKS_DEPENDENCIES}
#                       ${CIDER_BENCHMARKS_SUPPLEMENT_DEPENDENCIES})

# add_executable(CiderDataTypesBenchmark CiderDataTypesBenchmark.cpp)
# target_link_libraries(CiderDataTypesBenchmark ${VELOX_BENCHMARKS_DEPENDENCIES}
#                       ${CIDER_BENCHMARKS_SUPPLEMENT_DEPENDENCIES})

add_subdirectory(expression)