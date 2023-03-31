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

# add_custom_target(cider-plan-all)

set(CIDER_LIB_PLAN_DEPS)

add_subdirectory(parser)
add_subdirectory(lookup)
add_subdirectory(builder)
add_subdirectory(validator)
add_subdirectory(substrait)

# add_dependencies(cider-plan-all cider-plan-parser cider-plan-lookup
# cider-plan-builder cider-plan-validator cider-plan-substrait)

add_library(
  cider_plan STATIC
  $<TARGET_OBJECTS:cider_plan_parser_objlib>
  $<TARGET_OBJECTS:cider_plan_func_lookup_objlib>
  $<TARGET_OBJECTS:cider_plan_expr_builder_objlib>
  $<TARGET_OBJECTS:cider_plan_substrait_objlib>
  $<TARGET_OBJECTS:cider_plan_validator_objlib>)
add_dependencies(substrait SubstraitCpp::function)

target_link_libraries(
  cider_plan
  QueryEngine
  cider_util
  substrait
  SubstraitCpp::function
  SubstraitCpp::type
  SubstraitCpp::common
  yaml-cpp)
