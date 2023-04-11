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

cmake_minimum_required(VERSION 3.16)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
find_program(CCACHE "sccache")
if(CCACHE)
  set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE})
  set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
endif(CCACHE)

set(BDTK_VERSION "0.9.0")

string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" BDTK_BASE_VERSION
             "${BDTK_VERSION}")

# if no build build type is specified, default to release builds
if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      Release
      CACHE STRING "Choose the type of build.")
endif()

project(BDTK VERSION "${BDTK_BASE_VERSION}")

set(BDTK_VERSION_MAJOR "${BDTK_VERSION_MAJOR}")
set(BDTK_VERSION_MINOR "${BDTK_VERSION_MINOR}")
set(BDTK_VERSION_PATCH "${BDTK_VERSION_PATCH}")

set(BDTK_SO_VERSION "${BDTK_VERSION_MAJOR}")
set(BDTK_FULL_SO_VERSION
    "${BDTK_SO_VERSION}.${BDTK_VERSION_MINOR}.${BDTK_VERSION_PATCH}")

message(
  STATUS "BDTK version: "
         "${BDTK_VERSION_MAJOR}.${BDTK_VERSION_MINOR}.${BDTK_VERSION_PATCH} "
         "(full: '${BDTK_VERSION}')")
message(
  STATUS "BDTK SO version: ${BDTK_SO_VERSION} (full: ${BDTK_FULL_SO_VERSION})")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules")

option(BDTK_ENABLE_ICL "Build ICL compression library for BDTK" OFF)
option(ICL_WITH_QPL "Build ICL with QPL codec" OFF)
option(ICL_WITH_QAT "Build ICL with QAT codec" OFF)
option(ICL_WITH_IGZIP "Build ICL with IGZIP codec" ON)
option(ICL_BUILD_TESTS "Build the ICL googletest unit tests" ON)
option(ICL_BUILD_BENCHMARKS "Build the ICL micro benchmarks" ON)
option(BDTK_ENABLE_CIDER "Build Cider library for BDTK" ON)
option(CIDER_ENABLE_TESTS "Build unit tests" ON)
option(CIDER_ENABLE_GOOGLETEST "Build GoogleTest for Cider." ON)
option(CIDER_ENABLE_DUCKDB "Build DuckDB for Cider." ON)
option(CIDER_ENABLE_VELOX "Build Velox Plugin for Cider." ON)
option(CIDER_ENABLE_SUBSTRAIT "Build Substrait for Cider." ON)
option(CIDER_ENABLE_BENCHMARK "Build benchmark for Cider." OFF)
option(CIDER_ENABLE_AVX512 "Enable avx512 instructions." OFF)

if(BDTK_ENABLE_CIDER)
  if(CIDER_ENABLE_VELOX)
    # Disable components when enable velox build
    set(CIDER_ENABLE_GOOGLETEST OFF)
    set(CIDER_ENABLE_DUCKDB OFF)
    set(CIDER_ENABLE_SUBSTRAIT OFF)
  endif()

  if(CIDER_ENABLE_GOOGLETEST)
    # googletest
    add_subdirectory(thirdparty/googletest)
    include_directories(thirdparty/googletest/googletest/include)
  else()
    include_directories(
      thirdparty/velox/third_party/googletest/googletest/include)
  endif()

  if(CIDER_ENABLE_BENCHMARK OR ENABLE_BENCHMARK)
    set(CMAKE_CXX_FLAGS "-g -ggdb -fno-omit-frame-pointer")
  endif()

  if(ENABLE_BENCHMARK)
    set(VELOX_ENABLE_TPCH_CONNECTOR ON)
  endif()

  if(CIDER_ENABLE_BENCHMARK)
    set(BENCHMARK_ENABLE_INSTALL OFF)
    set(BENCHMARK_ENABLE_TESTING OFF)
    add_subdirectory(thirdparty/benchmark)
    include_directories(thirdparty/benchmark/include)
  endif()

  if(CIDER_ENABLE_DUCKDB)
    add_subdirectory(thirdparty/duckdb-amalgamation)
    include_directories(${CMAKE_BINARY_DIR}/thirdparty/duckdb-amalgamation)
  else()
    include_directories(thirdparty/velox/velox/external/duckdb)
  endif()

  if(CIDER_ENABLE_SUBSTRAIT)
    set(SUBSTRAIT_ROOT ${CMAKE_SOURCE_DIR}/thirdparty/substrait/proto)
  else()
    set(SUBSTRAIT_ROOT
        ${CMAKE_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto)
  endif()

  # robin-hood-hashing
  include_directories(thirdparty/robin-hood-hashing/src/include)

  # substrait
  include(substrait)
  include(substrait-cpp)

  if(CIDER_ENABLE_VELOX)
    add_subdirectory(thirdparty/velox)
  endif()

  include(BuildUtils)

  set(CIDER_DEPENDENCIES_LIBS)
  list(APPEND CIDER_DEPENDENCIES_LIBS substrait)
  list(APPEND CIDER_DEPENDENCIES_LIBS SubstraitCpp::function SubstraitCpp::type
       SubstraitCpp::common)
  list(APPEND CIDER_DEPENDENCIES_LIBS yaml-cpp)
  list(
    APPEND
    CIDER_DEPENDENCIES_LIBS
    cider_util
    cider_processor
    cider_plan
    jitlib
    nextgen
    QueryEngine
    cider_hashtable_join
    cider_module)

  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  icl_car(_FIRST_LIB ${CIDER_DEPENDENCIES_LIBS})
  icl_cdr(_OTHER_LIBS ${CIDER_DEPENDENCIES_LIBS})
  create_merged_static_lib(
    cider_static
    NAME
    cider_static
    ROOT
    ${_FIRST_LIB}
    TO_MERGE
    ${_OTHER_LIBS})

  set(CIDER_VELOX_DEPENDENCIES_LIBS)
  list(APPEND CIDER_VELOX_DEPENDENCIES_LIBS velox_plugin)
  list(APPEND CIDER_VELOX_DEPENDENCIES_LIBS velox_substrait_convertor
       velox_plan_transformer cider_plan_transformer)

  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  icl_car(_FIRST_LIB ${CIDER_VELOX_DEPENDENCIES_LIBS})
  icl_cdr(_OTHER_LIBS ${CIDER_VELOX_DEPENDENCIES_LIBS})
  create_merged_static_lib(
    cider_velox_static
    NAME
    cider_velox_static
    ROOT
    ${_FIRST_LIB}
    TO_MERGE
    ${_OTHER_LIBS})

  include_directories(src/cider)
  include_directories(src/cider/include)
  add_subdirectory(src/cider)
  if(CIDER_ENABLE_VELOX)
    add_subdirectory(src/cider-velox)
  endif()

endif()

if(BDTK_ENABLE_ICL)
  if(NOT BDTK_ENABLE_CIDER)
    if(ICL_BUILD_TESTS)
      add_subdirectory(thirdparty/googletest)
      include_directories(thirdparty/googletest/googletest/include)
    endif()
    if(ICL_BUILD_BENCHMARKS)
      set(BENCHMARK_ENABLE_INSTALL OFF)
      set(BENCHMARK_ENABLE_TESTING OFF)
      add_subdirectory(thirdparty/benchmark)
      include_directories(thirdparty/benchmark/include)
      include_directories(thirdparty/googletest/googlemock/include)
    endif()
  endif()
  add_subdirectory(src/compression)
endif()
