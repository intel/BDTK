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

set(EXPRESSION_TEST_LIBS ${CIDER_BENCHMARKS_SUPPLEMENT_DEPENDENCIES}
                         ${VELOX_BENCHMARKS_DEPENDENCIES})

add_executable(ArithmeticAndComparison ArithmeticAndComparison.cpp)
target_link_libraries(ArithmeticAndComparison ${EXPRESSION_TEST_LIBS})

add_executable(PipelineOperator PipelineOperator.cpp)
target_link_libraries(PipelineOperator ${EXPRESSION_TEST_LIBS})

add_executable(StringFunctionBenchmark StringFunctionBenchmark.cpp)
target_link_libraries(StringFunctionBenchmark ${EXPRESSION_TEST_LIBS})
