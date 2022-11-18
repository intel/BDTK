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
set(FOLLY_WITH_DEPENDENCIES
    ${FOLLY_LIBRARIES} ${Boost_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARIES}
    ${EVENT} ${SNAPPY} ${CMAKE_DL_LIBS})

set(FOLLY ${FOLLY_LIBRARIES})
set(FOLLY_BENCHMARK Folly::follybenchmark)

find_library(LLVM_LIB LLVM)

add_library(velox_plugin_test_util CiderPlanBuilder.cpp)

add_library(
  plan_transformer_test_util
  ${CMAKE_CURRENT_SOURCE_DIR}/planTransformerTest/utils/PlanTansformerTestUtil.cpp
)

set(VELOX_PLUGIN_TEST_LIBS
    velox_plugin
    velox_substrait_convertor
    velox_substrait_plan_converter
    velox_plugin_test_util
    plan_transformer_test_util
    velox_functions_test_lib
    velox_exec
    velox_dwio_common
    velox_aggregates
    velox_aggregates_test_lib
    velox_functions_lib
    velox_functions_prestosql
    velox_hive_connector
    velox_type
    velox_serialization
    velox_exec_test_util
    velox_functions_json
    velox_vector
    velox_vector_test_lib
    velox_arrow_bridge
    velox_memory
    velox_dwio_common_exception
    ${Boost_ATOMIC_LIBRARIES}
    ${Boost_CONTEXT_LIBRARIES}
    ${Boost_DATE_TIME_LIBRARIES}
    ${Boost_FILESYSTEM_LIBRARIES}
    ${Boost_PROGRAM_OPTIONS_LIBRARIES}
    ${Boost_REGEX_LIBRARIES}
    ${Boost_THREAD_LIBRARIES}
    ${Boost_SYSTEM_LIBRARIES}
    ${FOLLY_WITH_DEPENDENCIES}
    ${FOLLY_BENCHMARK}
    ${DOUBLE_CONVERSION}
    ${gflags_LIBRARIES}
    ${GLOG}
    ${FMT}
    ${FILESYSTEM}
    ${LLVM_LIB}
    cider
    cider_plan_parser
    velox_plan_transformer
    cider_plan_transformer
    gtest
    gtest_main)

set(TEST_ARGS "")

add_executable(CiderPlanNodeTest CiderPlanNodeTest.cpp)
target_link_libraries(CiderPlanNodeTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderPlanNodeTest CiderPlanNodeTest ${TEST_ARGS})

add_executable(CiderDataConvertorTest DataConvertorTest.cpp ArrowUtilsTest.cpp)
target_link_libraries(CiderDataConvertorTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderDataConvertorTest CiderDataConvertorTest ${TEST_ARGS})

add_executable(CiderOperatorTest CiderOperatorTest.cpp)
target_link_libraries(CiderOperatorTest ${VELOX_PLUGIN_TEST_LIBS}
                      velox_plan_transformer)
add_test(CiderOperatorTest CiderOperatorTest ${TEST_ARGS})

add_executable(CiderPatternTest CiderPatternTest.cpp)
target_link_libraries(CiderPatternTest ${VELOX_PLUGIN_TEST_LIBS}
                      velox_plan_transformer)
add_test(CiderPatternTest CiderPatternTest ${TEST_ARGS})

add_executable(AggWithRandomDataTest AggWithRandomDataTest.cpp)
target_link_libraries(AggWithRandomDataTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(AggWithRandomDataTest AggWithRandomDataTest ${TEST_ARGS})

add_executable(CiderDataTypeTest CiderDataTypeTest.cpp)
target_link_libraries(CiderDataTypeTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderDataTypeTest CiderDataTypeTest ${TEST_ARGS})

add_executable(CiderOperatorCompareOpTest CiderOperatorCompareOpTest.cpp)
target_link_libraries(CiderOperatorCompareOpTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderOperatorCompareOpTest CiderOperatorCompareOpTest ${TEST_ARGS})

add_executable(CiderFilterProjectTest CiderFilterProjectTest.cpp)
target_link_libraries(CiderFilterProjectTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderFilterProjectTest CiderFilterProjectTest ${TEST_ARGS})

add_executable(CiderScalarFunctionTest CiderScalarFunctionTest.cpp)
target_link_libraries(CiderScalarFunctionTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderScalarFunctionTest CiderScalarFunctionTest ${TEST_ARGS})

add_executable(CiderOperatorAggOpTest CiderOperatorAggOpTest.cpp)
target_link_libraries(CiderOperatorAggOpTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderOperatorAggOpTest CiderOperatorAggOpTest ${TEST_ARGS})

add_executable(CiderOperatorHashJoinTest CiderOperatorHashJoinTest.cpp)
target_link_libraries(CiderOperatorHashJoinTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderOperatorHashJoinTest CiderOperatorHashJoinTest ${TEST_ARGS})

add_subdirectory(expression)
add_subdirectory(planTransformerTest)
add_subdirectory(SubstraitTest)
