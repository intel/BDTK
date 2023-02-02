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

#
# IgzipCommon
#

project(igzip_common)

set(IGZIP_COMMON_SOURCES igzip_wrapper.c)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

add_library(igzip_common STATIC ${IGZIP_COMMON_SOURCES})
target_link_libraries(igzip_common ISAL::igzip)
