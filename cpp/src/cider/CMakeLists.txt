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

# Required for LLVM / JIT, alternative to setting CMP0065 to OLD
set(CMAKE_ENABLE_EXPORTS True)

if("${CMAKE_VERSION}" VERSION_GREATER 3.11.999)
  cmake_policy(SET CMP0074 NEW)
endif()

project(cider)

add_definitions("-DBOOST_LOG_DYN_LINK")

# Required for macOS with Boost 1.71.0+ See
# https://gitlab.kitware.com/cmake/cmake/issues/19714
set(Boost_NO_BOOST_CMAKE 1)

option(SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS
       "Suppress NullLogger deprecated warnings.")
if(SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
  add_definitions("-DSUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS")
endif()

option(ENABLE_JIT_DEBUG "Enable debugging symbols for the JIT" OFF)
if(ENABLE_JIT_DEBUG)
  add_definitions("-DWITH_JIT_DEBUG")
endif()

option(
  ENABLE_PERF_JIT_LISTENER
  "Enable dumping perf symbols for the JIT, needs LLVM bulit with -DLLVM_USE_PERF"
  OFF)
if(ENABLE_PERF_JIT_LISTENER)
  add_definitions("-DENABLE_PERF_JIT_LISTENER")
endif()

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wall -Wno-unused-local-typedefs -fdiagnostics-color=auto -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -fPIC"
)

option(OPTIMIZE_FOR_NATIVE "Build with -march=native" OFF)
if(OPTIMIZE_FOR_NATIVE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
  if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-register")
endif()
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

# RapidJSON
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/rapidjson/include)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

# robin-hood-hashing
include_directories(
  ${CMAKE_SOURCE_DIR}/thirdparty/robin-hood-hashing/src/include)

# Dependencies
find_package(gflags REQUIRED)
if(NOT ${gflags_FOUND})
  message(FATAL_ERROR "gflags not found, please install gflags")
endif()

find_package(FMT REQUIRED)
if(NOT ${FMT_FOUND})
  message(FATAL_ERROR "fmt not found, please install fmt")
endif()
add_definitions(-DFMT_HEADER_ONLY)

find_package(LLVM CONFIG REQUIRED)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

find_library(CLANG_LIB clang-cpp)
find_library(LLVM_LIB LLVM)
set(clang_libs ${CLANG_LIB})
set(llvm_libs ${LLVM_LIB})

# Boost
find_package(
  Boost
  COMPONENTS program_options
             system
             thread
             timer
             locale
             iostreams
             context
  REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

option(ENABLE_VELOX_FUNCTION "Enable Velox Functions" OFF)
if(ENABLE_VELOX_FUNCTION)
  add_definitions(-DENABLE_VELOX_FUNCTION)
endif()

add_subdirectory(common)
add_subdirectory(exec)
add_subdirectory(function)
add_subdirectory(util)
add_subdirectory(examples)
if(CIDER_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(CIDER_ENABLE_BENCHMARK)
  add_subdirectory(benchmarks)
endif()
