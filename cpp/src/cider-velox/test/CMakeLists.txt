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

add_library(velox_plugin_test_util BatchDataGenerator.cpp CiderPlanBuilder.cpp)

add_library(
  plan_transformer_test_util
  ${CMAKE_CURRENT_SOURCE_DIR}/planTransformerTest/utils/PlanTansformerTestUtil.cpp
)

set(VELOX_PLUGIN_TEST_LIBS
    cider_velox_static
    cider_static
    velox_substrait_convertor
    velox_substrait_plan_converter
    plan_transformer_test_util
    velox_plugin_test_util
    velox_functions_test_lib
    velox_exec
    velox_dwio_common
    velox_aggregates
    velox_aggregates_test_lib
    velox_functions_lib
    velox_functions_prestosql
    velox_hive_connector
    velox_type
    velox_serialization
    velox_exec_test_lib
    velox_functions_json
    velox_vector
    velox_vector_test_lib
    velox_arrow_bridge
    velox_memory
    velox_dwio_common_exception
    cider_function
    gtest
    gtest_main)

set(TEST_ARGS "")

add_executable(CiderPlanNodeTest CiderPlanNodeTest.cpp)
target_link_libraries(CiderPlanNodeTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderPlanNodeTest CiderPlanNodeTest ${TEST_ARGS})

add_executable(ArrowUtilsTest ArrowUtilsTest.cpp)
target_link_libraries(ArrowUtilsTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(ArrowUtilsTest ArrowUtilsTest ${TEST_ARGS})

add_executable(CiderOperatorTest CiderOperatorTest.cpp)
target_link_libraries(CiderOperatorTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderOperatorTest CiderOperatorTest ${TEST_ARGS})

add_executable(CiderPatternTest CiderPatternTest.cpp)
target_link_libraries(CiderPatternTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderPatternTest CiderPatternTest ${TEST_ARGS})

# add_executable(AggWithRandomDataTest AggWithRandomDataTest.cpp)
# target_link_libraries(AggWithRandomDataTest ${VELOX_PLUGIN_TEST_LIBS})
# add_test(AggWithRandomDataTest AggWithRandomDataTest ${TEST_ARGS})

add_executable(CiderDataTypeTest CiderDataTypeTest.cpp)
target_link_libraries(CiderDataTypeTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderDataTypeTest CiderDataTypeTest ${TEST_ARGS})

add_executable(CiderOperatorCompareOpTest CiderOperatorCompareOpTest.cpp)
target_link_libraries(CiderOperatorCompareOpTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderOperatorCompareOpTest CiderOperatorCompareOpTest ${TEST_ARGS})

add_executable(CiderFilterProjectTest CiderFilterProjectTest.cpp)
target_link_libraries(CiderFilterProjectTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderFilterProjectTest CiderFilterProjectTest ${TEST_ARGS})

add_executable(CiderScalarFunctionTest CiderScalarFunctionTest.cpp)
target_link_libraries(CiderScalarFunctionTest ${VELOX_PLUGIN_TEST_LIBS})
add_test(CiderScalarFunctionTest CiderScalarFunctionTest ${TEST_ARGS})

# add_executable(CiderOperatorAggOpTest CiderOperatorAggOpTest.cpp)
# target_link_libraries(CiderOperatorAggOpTest ${VELOX_PLUGIN_TEST_LIBS})
# add_test(CiderOperatorAggOpTest CiderOperatorAggOpTest ${TEST_ARGS})

# add_executable(CiderOperatorHashJoinTest CiderOperatorHashJoinTest.cpp)
# target_link_libraries(CiderOperatorHashJoinTest ${VELOX_PLUGIN_TEST_LIBS})
# add_test(CiderOperatorHashJoinTest CiderOperatorHashJoinTest ${TEST_ARGS})

add_subdirectory(planTransformerTest)
add_subdirectory(SubstraitTest)
