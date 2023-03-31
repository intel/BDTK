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

include(ExternalProject)

macro(build_substrait_cpp)
  message(STATUS "Building substrait-cpp from source")

  set(SUBSTRAIT_CPP_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/thirdparty/substrait_cpp_ep")

  set(SUBSTRAIT_CPP_CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_TESTING=OFF
      "-DCMAKE_INSTALL_PREFIX=${SUBSTRAIT_CPP_PREFIX}")

  ExternalProject_Add(
    substrait_cpp_ep
    GIT_REPOSITORY "https://github.com/substrait-io/substrait-cpp.git"
    GIT_TAG "717bb017576536b5e970fa5bd06f1c3d51fa8939"
    PREFIX ${SUBSTRAIT_CPP_PREFIX}
    CMAKE_ARGS ${SUBSTRAIT_CPP_CMAKE_ARGS})

  ExternalProject_Get_Property(substrait_cpp_ep SOURCE_DIR)
  set(SUBSTRAIT_CPP_INCLUDE_DIR "${SOURCE_DIR}/include")

  ExternalProject_Get_Property(substrait_cpp_ep BINARY_DIR)
  set(SUBSTRAIT_CPP_BINARY_DIR "${BINARY_DIR}/substrait")

  file(MAKE_DIRECTORY "${SUBSTRAIT_CPP_INCLUDE_DIR}")

  set(SUBSTRAIT_CPP_STATIC_LIBRARY_COMMON
      "${SUBSTRAIT_CPP_BINARY_DIR}/common/${CMAKE_STATIC_LIBRARY_PREFIX}substrait_common${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(SUBSTRAIT_CPP_STATIC_LIBRARY_TYPE
      "${SUBSTRAIT_CPP_BINARY_DIR}/type/${CMAKE_STATIC_LIBRARY_PREFIX}substrait_type${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(SUBSTRAIT_CPP_STATIC_LIBRARY_FUNCTION
      "${SUBSTRAIT_CPP_BINARY_DIR}/function/${CMAKE_STATIC_LIBRARY_PREFIX}substrait_function${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  add_library(SubstraitCpp::common STATIC IMPORTED)
  set_target_properties(
    SubstraitCpp::common
    PROPERTIES IMPORTED_LOCATION "${SUBSTRAIT_CPP_STATIC_LIBRARY_COMMON}"
               INTERFACE_INCLUDE_DIRECTORIES "${SUBSTRAIT_CPP_INCLUDE_DIR}")

  add_library(SubstraitCpp::type STATIC IMPORTED)
  set_target_properties(
    SubstraitCpp::type
    PROPERTIES IMPORTED_LOCATION "${SUBSTRAIT_CPP_STATIC_LIBRARY_TYPE}"
               INTERFACE_INCLUDE_DIRECTORIES "${SUBSTRAIT_CPP_INCLUDE_DIR}")
  add_dependencies(SubstraitCpp::type substrait_cpp_ep)

  add_library(SubstraitCpp::function STATIC IMPORTED)
  set_target_properties(
    SubstraitCpp::function
    PROPERTIES IMPORTED_LOCATION "${SUBSTRAIT_CPP_STATIC_LIBRARY_FUNCTION}"
               INTERFACE_INCLUDE_DIRECTORIES "${SUBSTRAIT_CPP_INCLUDE_DIR}")
  add_dependencies(SubstraitCpp::function substrait_cpp_ep)

  include_directories(${SUBSTRAIT_CPP_INCLUDE_DIR})
endmacro()

build_substrait_cpp()
