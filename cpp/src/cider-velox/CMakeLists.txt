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

project(VeloxPlugin)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt")

find_library(GLOG glog)

set(VELOX_ROOT ${CMAKE_BINARY_DIR}/thirdparty/velox)
include_directories(${VELOX_ROOT})
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/velox)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/velox/velox/external/xxhash)
include_directories(
  ${CMAKE_SOURCE_DIR}/thirdparty/velox/third_party/xsimd/include)

include_directories(src)
add_subdirectory(src)

# required to set ${VELOX_ENABLE_PARQUET} ON in velox directory
option(ENABLE_BENCHMARK "Build benchmark" ON)
if(ENABLE_BENCHMARK)
  add_subdirectory(benchmark)
endif()

if(CIDER_ENABLE_TESTS)
  add_subdirectory(test)
endif()
