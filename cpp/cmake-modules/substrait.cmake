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

macro(build_substrait)
  message(STATUS "Generating substrait files")

  if(NOT DEFINED SUBSTRAIT_ROOT)
    message(FATAL_ERROR "SUBSTRAIT_ROOT not defined, please set SUBSTRAIT_ROOT")
  endif()
  message(STATUS "SUBSTRAIT_ROOT: ${SUBSTRAIT_ROOT}")

  # Set up Proto
  find_package(Protobuf REQUIRED)
  find_package(FMT REQUIRED)

  set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty/substrait")
  set(SUBSTRAIT_INCLUDE_DIR ${PROTO_OUTPUT_DIR})
  set(substrait_proto_directory ${SUBSTRAIT_ROOT}/substrait)
  file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})
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
    COMMAND protoc --proto_path ${SUBSTRAIT_ROOT}/ --cpp_out ${PROTO_OUTPUT_DIR}
            ${PROTO_FILES}
    DEPENDS ${PROTO_DIR}
    COMMENT "Running PROTO compiler"
    VERBATIM)
  add_custom_target(substrait_proto_gen ALL DEPENDS ${PROTO_OUTPUT_FILES})

  add_library(substrait STATIC ${PROTO_SRCS})
  add_dependencies(substrait substrait_proto_gen)
  set_target_properties(substrait PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_link_libraries(substrait ${Protobuf_LIBRARIES} fmt::fmt)
  target_include_directories(substrait PUBLIC "${SUBSTRAIT_INCLUDE_DIR}")

  include_directories(${SUBSTRAIT_INCLUDE_DIR})
endmacro()

build_substrait()
