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

find_package(gflags COMPONENTS shared)
set(SRC_FILE
    CiderCompileModule.cpp
    CiderRuntimeModule.cpp
    CiderExprEvaluator.cpp
    CiderTypes.cpp
    CiderOptions.cpp
    batch/CiderBatchUtils.cpp
    batch/CiderArrowBufferHolder.cpp
    batch/CiderBatch.cpp)
add_library(cider SHARED ${SRC_FILE})
target_link_libraries(
  cider
  cider_processor
  QueryEngine
  memory
  cider_plan_parser
  ${Boost_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${GFLAGS_LIBRARIES}
  ${TBB_LIBS}
  yaml-cpp)

set_target_properties(cider PROPERTIES VERSION "${BDTK_FULL_SO_VERSION}"
                                       SOVERSION "${BDTK_SO_VERSION}")

install(
  TARGETS cider
  DESTINATION "lib"
  COMPONENT "CIDER")
install(
  FILES CiderInterface.h
        CiderRuntimeModule.h
        CiderCompileModule.h
        CiderBatch.h
        CiderOptions.h
        CiderTableSchema.h
        CiderException.h
  DESTINATION "Cider"
  COMPONENT "include")
