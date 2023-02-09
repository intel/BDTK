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

# set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})
set(TEST_ARGS "--gtest_output=xml:../")
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../utils)
file(GLOB cider_functionality_test_files ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
foreach(cider_functionality_test_file ${cider_functionality_test_files})
  string(REGEX MATCH "functionality/.*" test_relative_path
               ${cider_functionality_test_file})
  string(REGEX REPLACE "functionality/" "" target_name ${test_relative_path})
  string(REGEX REPLACE ".cpp" "" target_name ${target_name})
  # following ut will core, due to Executor after Executor relative codes
  # deleted those ut need delete
  if(${target_name} MATCHES "CiderFilterTest")
    continue()
  elseif(${target_name} MATCHES "CiderGroupByTest")
    continue()
  elseif(${target_name} MATCHES "CiderHashJoinTest")
    continue()
  elseif(${target_name} MATCHES "CiderNonEqualLoopJoinTest")
    continue()
  elseif(${target_name} MATCHES "CiderStringTest")
    continue()
  elseif(${target_name} MATCHES "PrimitiveTypeArrowTest")
    continue()
  elseif(${target_name} MATCHES "PrimitiveTypeQueryArrowTest")
    continue()
  elseif(${target_name} MATCHES "PrimitiveTypeQueryTest")
    continue()
  endif()

  add_executable(${target_name} ${cider_functionality_test_file})
  target_link_libraries(${target_name} ${EXECUTE_TEST_UTILS_LIBS})
  add_test(${target_name} ${EXECUTABLE_OUTPUT_PATH}/${target_name} ${TEST_ARGS})
endforeach()
