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

cmake_minimum_required(VERSION 3.16)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(ModularSQL LANGUAGES C CXX)

# if no build build type is specified, default to release builds
if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      Release
      CACHE STRING "Choose the type of build.")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules")

option(CIDER_ENABLE_TESTS "Build unit tests" ON)
option(CIDER_ENABLE_GOOGLETEST "Build GoogleTest for Cider." ON)
option(CIDER_ENABLE_DUCKDB "Build DuckDB for Cider." ON)
option(CIDER_ENABLE_VELOX "Build Velox Plugin for Cider." ON)
option(CIDER_ENABLE_SUBSTRAIT "Build Substrait for Cider." ON)
option(CIDER_ENABLE_BENCHMARK "Build benchmark for Cider." ON)
option(CIDER_ENABLE_YAMLCPP "Build yaml-cpp for Cider." ON)

if(${CIDER_ENABLE_VELOX})
  # Disable components when enable velox build
  set(CIDER_ENABLE_GOOGLETEST OFF)
  set(CIDER_ENABLE_DUCKDB OFF)
  set(CIDER_ENABLE_SUBSTRAIT OFF)
  set(CIDER_ENABLE_YAMLCPP OFF)
endif()

if(CIDER_ENABLE_TESTS)
  enable_testing()
endif()

if(${CIDER_ENABLE_GOOGLETEST})
  # googletest
  add_subdirectory(thirdparty/googletest)
  include_directories(thirdparty/googletest/googletest/include)
else()
  include_directories(
    thirdparty/velox/third_party/googletest/googletest/include)
endif()

if(CIDER_ENABLE_BENCHMARK)
  set(BENCHMARK_ENABLE_INSTALL OFF)
  set(BENCHMARK_ENABLE_TESTING OFF)
  add_subdirectory(thirdparty/benchmark)
  include_directories(thirdparty/benchmark/include)
endif()

if(${CIDER_ENABLE_DUCKDB})
  add_subdirectory(thirdparty/duckdb-amalgamation)
  include_directories(${CMAKE_BINARY_DIR}/thirdparty/duckdb-amalgamation)
else()
  include_directories(thirdparty/velox/velox/external/duckdb)
endif()

if(${CIDER_ENABLE_SUBSTRAIT})
  set(substrait_directory ${CMAKE_SOURCE_DIR}/thirdparty/substrait/proto)
  include_directories(thirdparty/substrait/proto)
else()
  set(substrait_directory
      ${CMAKE_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto)
  include_directories(thirdparty/velox/velox/substrait/proto)
endif()

if(${CIDER_ENABLE_YAMLCPP})
  add_subdirectory(thirdparty/yaml-cpp)
  include_directories(thirdparty/yaml-cpp/include)
else()
  include_directories(thirdparty/velox/third_party/yaml-cpp/include)
endif()

# TODO: remove after cleanup public API dependency RapidJSON
include_directories(thirdparty/rapidjson/include)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

# robin-hood-hashing
include_directories(thirdparty/robin-hood-hashing/src/include)

#
# Subdirectories
#
include_directories(cider)
include_directories(cider/include)
add_subdirectory(cider)

if(CIDER_ENABLE_VELOX)
  add_subdirectory(thirdparty/velox)
  add_subdirectory(cider-velox)
endif()
