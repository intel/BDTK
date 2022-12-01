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

set(NEXTGEN_COMPILER_TEST_LIBS gtest cider_util test_utils duckdb substrait
                               cider)

add_executable(NextgenCompilerTest NextgenCompilerTest.cpp)
target_link_libraries(NextgenCompilerTest ${NEXTGEN_COMPILER_TEST_LIBS})
add_test(NextgenCompilerTest ${EXECUTABLE_OUTPUT_PATH}/NextgenCompilerTest
         --use_nextgen_compiler)

add_executable(FilterProjectTest FilterProjectTest.cpp)
target_link_libraries(FilterProjectTest ${NEXTGEN_COMPILER_TEST_LIBS})
add_test(FilterProjectTest ${EXECUTABLE_OUTPUT_PATH}/FilterProjectTest
         ${TEST_ARGS})
