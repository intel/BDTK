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

add_subdirectory(utils)

set(VELOX_PLAN_TRANSFORMER_TEST_LIBS velox_plan_transformer_test_utils
                                     ${VELOX_PLUGIN_TEST_LIBS})

set(TEST_ARGS "")

add_executable(CiderCompoundPatternTest CiderCompoundPatternTest.cpp)
target_link_libraries(CiderCompoundPatternTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderCompoundPatternTest CiderCompoundPatternTest ${TEST_ARGS})

add_executable(CiderPatternNullptrRewriterTest
               CiderPatternNullptrRewriterTest.cpp)
target_link_libraries(CiderPatternNullptrRewriterTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderPatternNullptrRewriterTest CiderPatternNullptrRewriterTest
         ${TEST_ARGS})

add_executable(CiderPatternKeepOrginalRewriterTest
               CiderPatternKeepOrginalRewriterTest.cpp)
target_link_libraries(CiderPatternKeepOrginalRewriterTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderPatternKeepOrginalRewriterTest
         CiderPatternKeepOrginalRewriterTest ${TEST_ARGS})

add_executable(CiderCompoundJoinMixedTest CiderCompoundJoinMixedTest.cpp)
target_link_libraries(CiderCompoundJoinMixedTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderCompoundJoinMixedTest CiderCompoundJoinMixedTest ${TEST_ARGS})

add_executable(CiderLeftDeepJoinPatternTest CiderLeftDeepJoinPatternTest.cpp)
target_link_libraries(CiderLeftDeepJoinPatternTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderLeftDeepJoinPatternTest CiderLeftDeepJoinPatternTest ${TEST_ARGS})

add_executable(CiderPatternJoinMixedTest CiderPatternJoinMixedTest.cpp)
target_link_libraries(CiderPatternJoinMixedTest
                      ${VELOX_PLAN_TRANSFORMER_TEST_LIBS})
add_test(CiderPatternJoinMixedTest CiderPatternJoinMixedTest ${TEST_ARGS})
