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

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tests ${CMAKE_CURRENT_BINARY_DIR})

set(TEST_BASE_PATH "./tmp")
add_definitions("-DBASE_PATH=\"${TEST_BASE_PATH}\"")

add_executable(CiderNewHashTableTest CiderNewHashTableTest.cpp)
add_executable(CiderNewAggHashTableTest CiderNewAggHashTableTest.cpp)
add_executable(Substrait2IRTest Substrait2IRTest.cpp)
add_executable(ExpressionEvalTest ExpressionEvalTest.cpp)
add_executable(CiderAllocatorTest CiderAllocatorTest.cpp)
add_executable(CiderExceptionTest CiderExceptionTest.cpp)
add_executable(CiderLogTest CiderLogTest.cpp)
add_executable(StringHeapTest StringHeapTest.cpp)

set(EXECUTE_TEST_LIBS
    gtest
    cider_util
    test_utils
    duckdb
    substrait
    cider
    dl
    cider_hashtable_join
    fmt::fmt)

target_link_libraries(CiderNewHashTableTest ${EXECUTE_TEST_LIBS}
                      cider_hashtable)
target_link_libraries(CiderNewAggHashTableTest ${EXECUTE_TEST_LIBS}
                      cider_hashtable)
target_link_libraries(Substrait2IRTest cider_plan_parser ${EXECUTE_TEST_LIBS})
target_link_libraries(ExpressionEvalTest ${EXECUTE_TEST_LIBS}
                      cider_expr_builder)
target_link_libraries(CiderAllocatorTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderExceptionTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderLogTest ${EXECUTE_TEST_LIBS})
target_link_libraries(StringHeapTest ${EXECUTE_TEST_LIBS})

target_compile_options(CiderNewHashTableTest PRIVATE -msse4.2 -msse2)
target_compile_options(CiderNewAggHashTableTest PRIVATE -msse4.2 -msse2)

set(TEST_ARGS "--gtest_output=xml:../")
add_test(CiderNewHashTableTest CiderNewHashTableTest ${TEST_ARGS})
add_test(CiderNewAggHashTableTest CiderNewAggHashTableTest ${TEST_ARGS})
add_test(Substrait2IRTest Substrait2IRTest ${TEST_ARGS})
add_test(ExpressionEvalTest ExpressionEvalTest ${TEST_ARGS})
add_test(CiderAllocatorTest CiderAllocatorTest ${TEST_ARGS})
add_test(CiderExceptionTest CiderExceptionTest ${TEST_ARGS})
add_test(CiderLogTest CiderLogTest ${TEST_ARGS})
add_test(StringHeapTest StringHeapTest ${TEST_ARGS})

find_package(fmt REQUIRED)

if(DEFINED ENV{ISTHMUS_EXEC})
  message("defined env var ISTHMUS_EXEC: $ENV{ISTHMUS_EXEC}")
  add_subdirectory(utils)
  add_subdirectory(hashtable)
endif()

add_subdirectory(nextgen)
add_subdirectory(plan)
add_subdirectory(processor)
