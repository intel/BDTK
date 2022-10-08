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

set(function_source_files
    scalar/RuntimeFunctions.cpp
    vector/ArrayOps.cpp
    RuntimeFunctions.bc
    ExtensionFunctionsBinding.cpp
    ExtensionFunctionsWhitelist.cpp
    FunctionLookup.cpp
    string/StringLike.cpp
    string/StringFunctions.cpp
    string/StringOpInfo.cpp
    string/StringOps.cpp
    datetime/DateTruncate.cpp
    datetime/ExtractFromTime.cpp
    hash/MurmurHash.cpp)

find_program(llvm_clangpp_cmd NAME clang++ HINTS ${LLVM_TOOLS_BINARY_DIR})

list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_subdirectory(substrait)

if(ENABLE_JIT_DEBUG)
  set(RT_OPT_FLAGS -O0 -g)
else()
  set(RT_OPT_FLAGS -O3)
endif()

if(NOT PREFER_STATIC_LIBS)
  list(APPEND MAPD_DEFINITIONS "-DBOOST_LOG_DYN_LINK")
endif()

if(SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
  list(APPEND MAPD_DEFINITIONS "-DSUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS")
endif()

add_custom_command(
  DEPENDS
    scalar/RuntimeFunctions.h
    scalar/RuntimeFunctions.cpp
    scalar/DecodersImpl.h
    scalar/TopKRuntime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/template/operator/join/hashtable/runtime/JoinHashTableQueryRuntime.cpp
    string/StringLike.cpp
    aggregate/CiderRuntimeFunctions.h
    aggregate/GroupByRuntime.cpp
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/RuntimeFunctions.bc
  COMMAND
    ${llvm_clangpp_cmd} ARGS -std=c++17 ${RT_OPT_FLAGS} -c -emit-llvm
    ${CLANG_SDK_INC} ${CLANG_CRT_INC} ${MAPD_DEFINITIONS} -I
    ${CMAKE_CURRENT_SOURCE_DIR}/../ -I ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/scalar/RuntimeFunctions.cpp)

add_library(cider_function ${function_source_files})

target_link_libraries(cider_function substrait_function_look_up)
