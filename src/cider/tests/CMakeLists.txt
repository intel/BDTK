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

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tests ${CMAKE_CURRENT_BINARY_DIR})

set(TEST_BASE_PATH "./tmp")
add_definitions("-DBASE_PATH=\"${TEST_BASE_PATH}\"")

add_executable(CodeGeneratorTest CodeGeneratorTest.cpp)
add_executable(CompileWorkUnitTest CompileWorkUnitTest.cpp)
add_executable(CiderAggHashTableTest CiderAggHashTableTest.cpp)
add_executable(CiderNewHashTableTest CiderNewHashTableTest.cpp)
add_executable(CiderNongroupbyAggTest CiderNongroupbyAggTest.cpp)
add_executable(CiderCompileModuleTest CiderCompileModuleTest.cpp)
add_executable(CiderRuntimeModuleTest CiderRuntimeModuleTest.cpp)
add_executable(Substrait2IRTest Substrait2IRTest.cpp)
add_executable(CiderAggSpillBufferMgrTest CiderAggSpillBufferMgrTest.cpp)
add_executable(CiderBatchTest CiderBatchTest.cpp)
add_executable(CiderRuntimeModuleJoinTest CiderRuntimeModuleJoinTest.cpp)
add_executable(CiderBitUtilsTest CiderBitUtilsTest.cpp)
add_executable(ExpressionEvalTest ExpressionEvalTest.cpp)
add_executable(CiderAllocatorTest CiderAllocatorTest.cpp)
add_executable(CiderArrowBatchTest CiderArrowBatchTest.cpp)
add_executable(CiderExceptionTest CiderExceptionTest.cpp)
add_executable(CiderLogTest CiderLogTest.cpp)
add_executable(Sql2IR Sql2IR.cpp)

set(EXECUTE_TEST_LIBS
    cider
    gtest
    test_utils
    fmt::fmt
    ${CIDER_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${Boost_LIBRARIES}
    ${ZLIB_LIBRARIES})

target_link_libraries(CodeGeneratorTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CompileWorkUnitTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderAggHashTableTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderNewHashTableTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderNongroupbyAggTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderCompileModuleTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderRuntimeModuleTest ${EXECUTE_TEST_LIBS})
target_link_libraries(Substrait2IRTest cider_plan_parser ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderAggSpillBufferMgrTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderBatchTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderRuntimeModuleJoinTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderBitUtilsTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ExpressionEvalTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderAllocatorTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderArrowBatchTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderExceptionTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CiderLogTest ${EXECUTE_TEST_LIBS})
target_link_libraries(Sql2IR ${EXECUTE_TEST_LIBS})

set(TEST_ARGS "--gtest_output=xml:../")
add_test(CodeGeneratorTest CodeGeneratorTest ${TEST_ARGS})
add_test(CompileWorkUnitTest CompileWorkUnitTest ${TEST_ARGS})
add_test(CiderAggHashTableTest CiderAggHashTableTest ${TEST_ARGS})
add_test(CiderNewHashTableTest CiderNewHashTableTest ${TEST_ARGS})
add_test(CiderNongroupbyAggTest CiderNongroupbyAggTest ${TEST_ARGS})
add_test(CiderCompileModuleTest CiderCompileModuleTest ${TEST_ARGS})
add_test(CiderRuntimeModuleTest CiderRuntimeModuleTest ${TEST_ARGS})
add_test(Substrait2IRTest Substrait2IRTest ${TEST_ARGS})
add_test(CiderAggSpillBufferMgrTest CiderAggSpillBufferMgrTest ${TEST_ARGS})
add_test(CiderBatchTest CiderBatchTest ${TEST_ARGS})
add_test(CiderRuntimeModuleJoinTest CiderRuntimeModuleJoinTest ${TEST_ARGS})
add_test(CiderBitUtilsTest CiderBitUtilsTest ${TEST_ARGS})
add_test(ExpressionEvalTest ExpressionEvalTest ${TEST_ARGS})
add_test(CiderAllocatorTest CiderAllocatorTest ${TEST_ARGS})
add_test(CiderArrowBatchTest CiderArrowBatchTest ${TEST_ARGS})
add_test(CiderExceptionTest CiderExceptionTest ${TEST_ARGS})
add_test(CiderLogTest CiderLogTest ${TEST_ARGS})

find_package(fmt REQUIRED)

if(DEFINED ENV{ISTHMUS_EXEC})
  message("defined env var ISTHMUS_EXEC: $ENV{ISTHMUS_EXEC}")
  add_subdirectory(utils)
  add_subdirectory(functionality)
  add_subdirectory(profiling)
  add_subdirectory(benchmark)
  add_subdirectory(hashtable)
endif()

add_subdirectory(nextgen)
add_subdirectory(plan)
