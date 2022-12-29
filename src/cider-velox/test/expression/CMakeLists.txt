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

set(SRC_FILE ExprEvalUtils.cpp VeloxToCiderExpr.cpp)
add_library(eu_mocker ${SRC_FILE})
if(NOT LIBSUBSTRAITCPP_INCLUDE_DIR OR NOT LIBSUBSTRAITCPP_LIB)
  add_dependencies(eu_mocker SUBSTRAITCPP)
endif()
find_package(gflags COMPONENTS shared)
find_library(GLOG glog)
find_package(folly CONFIG REQUIRED)
set(FOLLY_WITH_DEPENDENCIES
    ${FOLLY_LIBRARIES} ${Boost_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARIES}
    ${EVENT} ${SNAPPY} ${CMAKE_DL_LIBS})
set(FOLLY ${FOLLY_LIBRARIES})
set(FOLLY_BENCHMARK Folly::follybenchmark)
set(EXPRESSION_TEST_LIBS velox_vector_fuzzer ${VELOX_PLUGIN_TEST_LIBS}
                         ${FOLLY_BENCHMARK} eu_mocker cider)
set(TEST_ARGS "")

add_executable(CiderSimpleArithmetic CiderSimpleArithmetic.cpp)
target_link_libraries(CiderSimpleArithmetic ${EXPRESSION_TEST_LIBS})
# FIXME: add test later add_test(CiderSimpleArithmetic CiderSimpleArithmetic
# ${TEST_ARGS})

add_executable(CiderComparisonConjunct CiderComparisonConjunct.cpp)
target_link_libraries(CiderComparisonConjunct ${EXPRESSION_TEST_LIBS})
