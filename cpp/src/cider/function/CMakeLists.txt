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

set(function_source_files
    ../../cider/exec/template/DynamicWatchdog.cpp
    ExtensionFunctionsBinding.cpp
    ExtensionFunctionsWhitelist.cpp
    string/StringLike.cpp
    string/StringFunctions.cpp
    string/StringOpInfo.cpp
    string/StringOps.cpp
    datetime/DateTruncate.cpp
    datetime/ExtractFromTime.cpp)

set(runtime_function_source_files scalar/RuntimeFunctions.cpp)

find_program(llvm_clangpp_cmd NAME clang++ HINTS ${LLVM_TOOLS_BINARY_DIR})

list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

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

execute_process(
  COMMAND bash -c "realpath -m $(which gcc)/../.."
  OUTPUT_VARIABLE GCC_TOOLCHAIN
  OUTPUT_STRIP_TRAILING_WHITESPACE)

add_custom_command(
  DEPENDS
    scalar/RuntimeFunctions.h
    scalar/RuntimeFunctions.cpp
    scalar/DecodersImpl.h
    string/StringLike.cpp
    aggregate/CiderRuntimeFunctions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/nextgen/context/ContextRuntimeFunctions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/nextgen/operators/OperatorRuntimeFunctions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/nextgen/function/CiderStringFunctions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/nextgen/function/CiderSetFunctions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../exec/nextgen/function/CiderDateFunctions.cpp
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/RuntimeFunctions.bc
  COMMAND
    ${llvm_clangpp_cmd} ARGS -std=c++17 ${RT_OPT_FLAGS} -c -emit-llvm
    --gcc-toolchain=${GCC_TOOLCHAIN} ${CLANG_SDK_INC} ${CLANG_CRT_INC}
    ${MAPD_DEFINITIONS} -I ${CMAKE_CURRENT_SOURCE_DIR}/../ -I
    ${CMAKE_CURRENT_SOURCE_DIR}/../include -I
    ${CMAKE_SOURCE_DIR}/thirdparty/robin-hood-hashing/src/include
    ${CMAKE_CURRENT_SOURCE_DIR}/scalar/RuntimeFunctions.cpp)

set(PYTHON_EXECUTABLE "/usr/bin/python")
add_custom_command(
  DEPENDS scalar/ExtensionFunctions.hpp
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast.raw
  COMMAND
    ${llvm_clangpp_cmd} ARGS -DNO_BOOST -std=c++17 -fsyntax-only -Xclang
    -ast-dump -fno-diagnostics-color -Wno-return-type-c-linkage -I
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/scalar/ExtensionFunctions.hpp >
    ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast.raw)

add_custom_command(
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast.raw
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast
  COMMAND
    ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/scripts/parse_ast.py
    ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast.raw >
    ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast)

add_custom_target(
  QueryEngineFunctionsTargets
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/RuntimeFunctions.bc
          ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast)

add_library(cider_runtime_function SHARED ${runtime_function_source_files})
add_dependencies(cider_runtime_function QueryEngineFunctionsTargets)
target_link_libraries(cider_runtime_function re2)

add_library(cider_function SHARED ${function_source_files})
add_dependencies(cider_function QueryEngineFunctionsTargets)
target_link_libraries(cider_function nextgen substrait)
