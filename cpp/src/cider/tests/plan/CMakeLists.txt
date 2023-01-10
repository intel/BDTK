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

set(DEP_LIBS ${EXECUTE_TEST_LIBS} cider_plan_validator)

set(TEST_ARGS "--gtest_output=xml:../")

add_executable(CiderPlanValidatorTest CiderPlanValidatorTest.cpp)
target_link_libraries(CiderPlanValidatorTest ${DEP_LIBS})
add_test(CiderPlanValidatorTest
         ${EXECUTABLE_OUTPUT_PATH}/CiderPlanValidatorTest ${TEST_ARGS})
