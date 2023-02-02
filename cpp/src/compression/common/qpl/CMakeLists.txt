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
# IaxCommon
#

project(IaxCommon)

set(CMAKE_CXX_STANDARD 17)
set(IAX_COMMON_SOURCES qpl_wrapper.cpp)

find_package(Qpl REQUIRED COMPONENTS qplhl)

set(IAX_LINK_LIBS Qpl::qplhl)

get_target_property(IAX_INCLUDE_DIRS Qpl::qplhl INTERFACE_INCLUDE_DIRECTORIES)

# install(FILES iax_wrapper.h DESTINATION
# "${CMAKE_INSTALL_INCLUDEDIR}/icl/common/iax")

add_custom_target(IaxCommon)

add_icl_lib(
  IaxCommon
  BUILD_STATIC
  BUILD_SHARED
  SOURCES
  ${IAX_COMMON_SOURCES}
  OUTPUTS
  IAX_COMMON_LIBRARIES
  STATIC_LINK_LIBS
  ${IAX_LINK_LIBS}
  SHARED_LINK_LIBS
  ${IAX_LINK_LIBS}
  PRIVATE_INCLUDES
  ${CMAKE_CURRENT_SOURCE_DIR}
  EXTRA_INCLUDES
  ${IAX_INCLUDE_DIRS})

add_dependencies(IaxCommon ${IAX_COMMON_LIBRARIES})
