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

add_executable(NextgenCompilerTest NextgenCompilerTest.cpp)
target_link_libraries(NextgenCompilerTest ${EXECUTE_TEST_LIBS})
add_test(NextgenCompilerTest ${EXECUTABLE_OUTPUT_PATH}/NextgenCompilerTest)

add_executable(FilterProjectTest FilterProjectTest.cpp)
target_link_libraries(FilterProjectTest ${EXECUTE_TEST_LIBS})
add_test(FilterProjectTest ${EXECUTABLE_OUTPUT_PATH}/FilterProjectTest
         ${TEST_ARGS})

add_executable(AggTest AggTest.cpp)
target_link_libraries(AggTest ${EXECUTE_TEST_LIBS})
add_test(AggTest ${EXECUTABLE_OUTPUT_PATH}/AggTest ${TEST_ARGS})

add_executable(ArrayTest ArrayTest.cpp)
target_link_libraries(ArrayTest ${EXECUTE_TEST_LIBS})
add_test(ArrayTest ${EXECUTABLE_OUTPUT_PATH}/ArrayTest ${TEST_ARGS})

add_executable(ContextTest ContextTest.cpp)
target_link_libraries(ContextTest ${EXECUTE_TEST_LIBS})
add_test(ContextTest ${EXECUTABLE_OUTPUT_PATH}/ContextTest ${TEST_ARGS})

add_executable(HashJoinTest HashJoinTest.cpp)
target_link_libraries(HashJoinTest ${EXECUTE_TEST_LIBS})
add_test(HashJoinTest ${EXECUTABLE_OUTPUT_PATH}/HashJoinTest ${TEST_ARGS})

add_executable(CrossJoinTest CrossJoinTest.cpp)
target_link_libraries(CrossJoinTest ${EXECUTE_TEST_LIBS})
add_test(CrossJoinTest ${EXECUTABLE_OUTPUT_PATH}/CrossJoinTest ${TEST_ARGS})

add_executable(DecimalOpTest DecimalOpTest.cpp)
target_link_libraries(DecimalOpTest ${EXECUTE_TEST_LIBS})
add_test(DecimalOpTest ${EXECUTABLE_OUTPUT_PATH}/DecimalOpTest ${TEST_ARGS})
