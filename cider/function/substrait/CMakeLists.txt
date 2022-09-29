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

set(SRCS
    ${PROTO_SRCS} SubstraitType.cpp SubstraitSignature.cpp
    SubstraitFunction.cpp SubstraitExtension.cpp SubstraitFunctionLookup.cpp)

add_library(substrait_function_loop_up STATIC ${SRCS})
target_link_libraries(substrait_function_loop_up substrait yaml-cpp)
