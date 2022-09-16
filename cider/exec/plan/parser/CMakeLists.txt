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

# Set up Proto
find_package(Protobuf REQUIRED)
find_package(FMT REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(PROTO_OUTPUT_DIR "${substrait_directory}")
set(substrait_proto_directory ${substrait_directory}/substrait)
file(MAKE_DIRECTORY ${substrait_proto_directory})
file(GLOB PROTO_FILES ${substrait_proto_directory}/*.proto
     ${substrait_proto_directory}/extensions/*.proto)
foreach(PROTO ${PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${substrait_proto_directory} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  list(APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/substrait/${PROTO_NAME}.pb.cc")
  list(APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/substrait/${PROTO_NAME}.pb.h")
endforeach()
set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(PROTO_DIR ${substrait_proto_directory}/, DIRECTORY)

# Generate Substrait hearders
add_custom_command(
  OUTPUT ${PROTO_OUTPUT_FILES}
  COMMAND protoc --proto_path ${substrait_directory}/ --cpp_out
          ${PROTO_OUTPUT_DIR} ${PROTO_FILES}
  DEPENDS ${PROTO_DIR}
  COMMENT "Running PROTO compiler"
  VERBATIM)
add_custom_target(substrait_proto_cider ALL DEPENDS ${PROTO_OUTPUT_FILES})
add_dependencies(substrait_proto_cider protobuf::libprotobuf)

add_library(substrait ${PROTO_SRCS})
target_link_libraries(substrait protobuf fmt::fmt)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wno-attributes -Wno-write-strings -Wno-unused-function -Wno-unused-label -Wno-sign-compare"
)

set(CIDER_PLAN_PARSER_SRCS
    ParserNode.cpp
    Translator.cpp
    SubstraitToAnalyzerExpr.cpp
    SubstraitToRelAlgExecutionUnit.cpp
    ConverterHelper.cpp
    SubstraitExprBuilder.cpp
    BaseContext.cpp
    PlanContext.cpp
    VariableContext.cpp
    RelVisitor.cpp
    PlanRelVisitor.cpp
    CiderSort.cpp)
add_library(cider_plan_parser STATIC ${CIDER_PLAN_PARSER_SRCS})
target_link_libraries(cider_plan_parser substrait cider_util fmt::fmt)
