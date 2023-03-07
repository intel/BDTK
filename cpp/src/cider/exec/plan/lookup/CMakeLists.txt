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

set(FUNC_LOOKUP_SRC_FILE FunctionLookupEngine.cpp)
add_library(cider_func_lkup STATIC ${FUNC_LOOKUP_SRC_FILE})
if(NOT LIBSUBSTRAITCPP_INCLUDE_DIR
   OR NOT LIBSUBSTRAITCPP_FUNCTION_LIB
   OR NOT LIBSUBSTRAITCPP_TYPE_LIB)
  add_dependencies(cider_func_lkup SUBSTRAITCPP)
endif()
target_link_libraries(
  cider_func_lkup ${SUBSTRAITCPP_FUNCTION_LIB} ${SUBSTRAITCPP_TYPE_LIB}
  ${SUBSTRAITCPP_COMMON_LIB} substrait yaml-cpp)
