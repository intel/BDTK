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

set(NEXTGEN_FUNCTIONAL_TEST_LIBS
    gtest
    cider_util
    test_utils
    duckdb
    substrait
    cider
    cider_function
    cider_func_lkup
    QueryEngine
    dl)

file(GLOB next_gen_test_files ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
foreach(next_gen_test_file ${next_gen_test_files})
  string(REGEX MATCH "functional/.*" test_relative_path ${next_gen_test_file})
  string(REGEX REPLACE "functional/" "" target_name ${test_relative_path})
  string(REGEX REPLACE ".cpp" "" target_name ${target_name})
  add_executable(${target_name} ${next_gen_test_file})
  target_link_libraries(${target_name} ${NEXTGEN_FUNCTIONAL_TEST_LIBS})
  add_test(${target_name} ${EXECUTABLE_OUTPUT_PATH}/${target_name}
           --use_nextgen_compiler)
endforeach()

add_subdirectory(benchmark)
