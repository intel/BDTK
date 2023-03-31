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

set(CIDER_TESTS_UTILS_SOURCE
    DuckDbArrowAdaptor.cpp
    DuckDbQueryRunner.cpp
    CiderNextgenQueryRunner.cpp
    CiderNextgenBenchmarkRunner.cpp
    CiderNextgenBenchmarkBase.cpp
    CiderNextgenTestBase.cpp
    CiderArrowChecker.cpp
    CiderArrowStringifier.cpp
    Utils.cpp
    CiderInt128.cpp)

add_library(test_utils ${CIDER_TESTS_UTILS_SOURCE})
target_link_libraries(
  test_utils
  duckdb
  cider_static
  nextgen
  QueryEngine
  cider_function
  cider_hashtable_join
  cider_module
  dl)

set(TEST_ARGS "--gtest_output=xml:../")

set(EXECUTE_TEST_UTILS_LIBS gtest test_utils)

set(EXECUTE_TEST_UTILS_LIBS
    ${EXECUTE_TEST_UTILS_LIBS}
    PARENT_SCOPE)
add_executable(IsthmusTest IsthmusTest.cpp)
target_link_libraries(IsthmusTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(IsthmusTest ${EXECUTABLE_OUTPUT_PATH}/IsthmusTest ${TEST_ARGS})

add_executable(ArrowArrayBuilderTest ArrowArrayBuilderTest.cpp)
target_link_libraries(ArrowArrayBuilderTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(ArrowArrayBuilderTest ${EXECUTABLE_OUTPUT_PATH}/ArrowArrayBuilderTest
         ${TEST_ARGS})

add_executable(CiderInt128Test CiderInt128Test.cpp)
target_link_libraries(CiderInt128Test ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderInt128Test ${EXECUTABLE_OUTPUT_PATH}/CiderInt128Test ${TEST_ARGS})

add_executable(QueryArrowDataGeneratorTest QueryArrowDataGeneratorTest.cpp)
target_link_libraries(QueryArrowDataGeneratorTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(QueryArrowDataGeneratorTest
         ${EXECUTABLE_OUTPUT_PATH}/QueryArrowDataGeneratorTest ${TEST_ARGS})

set(TEST_PROGRAMS IsthmusTest)

add_executable(CiderNextgenQueryRunnerTest CiderNextgenQueryRunnerTest.cpp)
target_link_libraries(CiderNextgenQueryRunnerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderNextgenQueryRunnerTest
         ${EXECUTABLE_OUTPUT_PATH}/CiderNextgenQueryRunnerTest ${TEST_ARGS})

add_executable(CiderArrowCheckerTest CiderArrowCheckerTest.cpp)
target_link_libraries(CiderArrowCheckerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderArrowCheckerTest ${EXECUTABLE_OUTPUT_PATH}/CiderArrowCheckerTest
         ${TEST_ARGS})

add_executable(CiderParquetReaderTest CiderParquetReaderTest.cpp)
target_link_libraries(CiderParquetReaderTest ${EXECUTE_TEST_UTILS_LIBS}
                      cider_parquet_reader)
add_test(CiderParquetReaderTest
         ${EXECUTABLE_OUTPUT_PATH}/CiderParquetReaderTest ${TEST_ARGS})
