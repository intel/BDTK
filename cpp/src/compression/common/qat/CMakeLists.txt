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

#
# QatCommon
#

project(QatCommon)

set(CMAKE_CXX_STANDARD 17)
set(QAT_COMMON_SOURCES qat_wrapper.cpp)

find_package(Qpl REQUIRED COMPONENTS qplhl)

set(QAT_LINK_LIBS qatzip)

add_custom_target(QatCommon)

add_icl_lib(
  QatCommon
  BUILD_STATIC
  BUILD_SHARED
  SOURCES
  ${QAT_COMMON_SOURCES}
  OUTPUTS
  QAT_COMMON_LIBRARIES
  STATIC_LINK_LIBS
  ${QAT_LINK_LIBS}
  SHARED_LINK_LIBS
  ${QAT_LINK_LIBS}
  PRIVATE_INCLUDES
  ${CMAKE_CURRENT_SOURCE_DIR}
  EXTRA_INCLUDES
  ${QAT_INCLUDE_DIRS})

add_dependencies(QatCommon ${QAT_COMMON_LIBRARIES})
