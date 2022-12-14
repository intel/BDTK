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
if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")
endif()

set(datamgr_source_files
    Buffer/AbstractBuffer.cpp
    Chunk/Chunk.cpp
    DataMgr.cpp
    DataMgrBufferProvider.cpp
    DataMgrDataProvider.cpp
    BufferMgr/CpuBufferMgr/CpuBufferMgr.cpp
    BufferMgr/CpuBufferMgr/CpuBuffer.cpp
    BufferMgr/CpuBufferMgr/TieredCpuBufferMgr.cpp
    BufferMgr/BufferMgr.cpp
    BufferMgr/Buffer.cpp)

add_library(memory ${datamgr_source_files})

target_link_libraries(memory cider_util ${Boost_THREAD_LIBRARY} ${TBB_LIBS}
                      ${CMAKE_DL_LIBS})
