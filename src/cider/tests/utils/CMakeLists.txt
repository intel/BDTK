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

set(CIDER_TESTS_UTILS_SOURCE
    Utils.cpp
    CiderBatchStringifier.cpp
    DuckDbArrowAdaptor.cpp
    DuckDbQueryRunner.cpp
    CiderQueryRunner.cpp
    CiderTestBase.cpp
    CiderBatchChecker.cpp
    CiderBenchmarkRunner.cpp
    CiderRunner.cpp
    CiderInt128.cpp)

add_library(test_utils ${CIDER_TESTS_UTILS_SOURCE})
target_link_libraries(test_utils cider_plan_parser)

set(TEST_ARGS "--gtest_output=xml:../")

set(EXECUTE_TEST_UTILS_LIBS gtest cider_util test_utils duckdb substrait cider)

set(EXECUTE_TEST_UTILS_LIBS
    ${EXECUTE_TEST_UTILS_LIBS}
    PARENT_SCOPE)
add_executable(IsthmusTest IsthmusTest.cpp)
target_link_libraries(IsthmusTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(IsthmusTest ${EXECUTABLE_OUTPUT_PATH}/IsthmusTest ${TEST_ARGS})

add_executable(DuckDbQueryRunnerTest DuckDbQueryRunnerTest.cpp)
target_link_libraries(DuckDbQueryRunnerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(DuckDbQueryRunnerTest ${EXECUTABLE_OUTPUT_PATH}/DuckDbQueryRunnerTest
         ${TEST_ARGS})

add_executable(ArrowArrayBuilderTest ArrowArrayBuilderTest.cpp)
target_link_libraries(ArrowArrayBuilderTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(ArrowArrayBuilderTest ${EXECUTABLE_OUTPUT_PATH}/ArrowArrayBuilderTest
         ${TEST_ARGS})

add_executable(CiderInt128Test CiderInt128Test.cpp)
target_link_libraries(CiderInt128Test ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderInt128Test ${EXECUTABLE_OUTPUT_PATH}/CiderInt128Test ${TEST_ARGS})

add_executable(CiderBatchBuilderTest CiderBatchBuilderTest.cpp)
target_link_libraries(CiderBatchBuilderTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderBatchBuilderTest ${EXECUTABLE_OUTPUT_PATH}/CiderBatchBuilderTest
         ${TEST_ARGS})

add_executable(CiderBatchCheckerTest CiderBatchCheckerTest.cpp)
target_link_libraries(CiderBatchCheckerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderBatchCheckerTest ${EXECUTABLE_OUTPUT_PATH}/CiderBatchCheckerTest
         ${TEST_ARGS})
add_executable(QueryDataGeneratorTest QueryDataGeneratorTest.cpp)
target_link_libraries(QueryDataGeneratorTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(QueryDataGeneratorTest
         ${EXECUTABLE_OUTPUT_PATH}/QueryDataGeneratorTest ${TEST_ARGS})

add_executable(QueryArrowDataGeneratorTest QueryArrowDataGeneratorTest.cpp)
target_link_libraries(QueryArrowDataGeneratorTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(QueryArrowDataGeneratorTest
         ${EXECUTABLE_OUTPUT_PATH}/QueryArrowDataGeneratorTest ${TEST_ARGS})

add_executable(CiderQueryRunnerTest CiderQueryRunnerTest.cpp)
target_link_libraries(CiderQueryRunnerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderQueryRunnerTest ${EXECUTABLE_OUTPUT_PATH}/CiderQueryRunnerTest
         ${TEST_ARGS})

add_executable(CiderMultiBatchTest CiderMultiBatchTest.cpp)
target_link_libraries(CiderMultiBatchTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderMultiBatchTest ${EXECUTABLE_OUTPUT_PATH}/CiderMultiBatchTest
         ${TEST_ARGS})

add_executable(CiderRunnerTest CiderRunnerTest.cpp)
target_link_libraries(CiderRunnerTest ${EXECUTE_TEST_UTILS_LIBS})
add_test(CiderRunnerTest ${EXECUTABLE_OUTPUT_PATH}/CiderRunnerTest ${TEST_ARGS})

set(TEST_PROGRAMS IsthmusTest DuckDbQueryRunnerTest CiderBatchBuilderTest
                  CiderBatchCheckerTest)
