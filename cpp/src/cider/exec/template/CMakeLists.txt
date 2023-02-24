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
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -Wall -Wno-attributes"
)

set_source_files_properties(RuntimeFunctionsCodegenWithIncludes.cpp
                            PROPERTIES COMPILE_FLAGS -O0)
set(query_engine_source_files DateTimeTranslator.cpp ExpressionRewrite.cpp)

if(NOT PREFER_STATIC_LIBS)
  list(APPEND MAPD_DEFINITIONS "-DBOOST_LOG_DYN_LINK")
endif()

if(SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
  list(APPEND MAPD_DEFINITIONS "-DSUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS")
endif()

find_program(llvm_clangpp_cmd NAME clang++ HINTS ${LLVM_TOOLS_BINARY_DIR})

list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(QueryEngine ${query_engine_source_files})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/../../function/RuntimeFunctions.bc
        ${CMAKE_CURRENT_BINARY_DIR}/../../function/ExtensionFunctions.ast
  DESTINATION QueryEngine
  COMPONENT "QE")

find_package(FMT REQUIRED)

set(QUERY_ENGINE_LIBS cider_function cider_type_plan cider_util cider_type_plan
                      fmt::fmt)

list(APPEND QUERY_ENGINE_LIBS ${llvm_libs} ${ZLIB_LIBRARIES})

if(ENABLE_VELOX_FUNCTION)
  find_library(GLOG glog)
  find_package(gflags COMPONENTS shared)
  find_library(FMT fmt)
  find_library(EVENT event)
  find_package(folly REQUIRED)
  set(FOLLY_WITH_DEPENDENCIES ${FOLLY_LIBRARIES} ${FMT} ${GFLAGS_LIBRARIES}
                              ${GLOG_LIBRARIES} ${EVENT})
  list(APPEND QUERY_ENGINE_LIBS ${FOLLY_WITH_DEPENDENCIES})
endif()

target_link_libraries(QueryEngine ${QUERY_ENGINE_LIBS})
