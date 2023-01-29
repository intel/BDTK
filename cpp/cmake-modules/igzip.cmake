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

# check nasm compiler
include(CheckLanguage)
check_language(ASM_NASM)
if(NOT CMAKE_ASM_NASM_COMPILER)
  message(
    FATAL_ERROR
      "NASM compiler can not be found, Please install NASM! ISA-L build need NASM!"
  )
endif()

enable_language(ASM_NASM)

include(ExternalProject)

set(ISAL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/isal_ep-install")
set(ISAL_STATIC_LIB
    "${ISAL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}isal${CMAKE_STATIC_LIBRARY_SUFFIX}"
)

set(ISAL_CONFIGURE_COMMAND "./autogen.sh")
set(ISAL_BUILD_COMMAND "./configure")
set(ISAL_BUILD_ARGS "--prefix=${ISAL_PREFIX}" "--enable-shared=no" "CFLAGS=-O2 -fPIC")
set(ISAL_INSTALL_COMMAND "make")
set(ISAL_INSTALL_ARGS "install" "-j4")

ExternalProject_Add(
  isal_ep
  GIT_REPOSITORY "https://github.com/intel/isa-l.git"
  GIT_TAG "c2bec3ea65ce35b01311d1cc4b314f6b4986b9c8"
  PREFIX ${ISAL_PREFIX}
  SOURCE_DIR ${ISAL_PREFIX}/src/isal_ep
  INSTALL_DIR ${ISAL_PREFIX}
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ${ISAL_CONFIGURE_COMMAND}
  BUILD_COMMAND ${ISAL_BUILD_COMMAND} ${ISAL_BUILD_ARGS}
  INSTALL_COMMAND ${ISAL_INSTALL_COMMAND} ${ISAL_INSTALL_ARGS}
  BUILD_BYPRODUCTS "${ISAL_STATIC_LIB}")

file(MAKE_DIRECTORY "${ISAL_PREFIX}/include")

add_library(ISAL::igzip STATIC IMPORTED)
set_target_properties(
  ISAL::igzip PROPERTIES IMPORTED_LOCATION "${ISAL_STATIC_LIB}"
                         INTERFACE_INCLUDE_DIRECTORIES "${ISAL_PREFIX}/include")
add_dependencies(ISAL::igzip isal_ep)
