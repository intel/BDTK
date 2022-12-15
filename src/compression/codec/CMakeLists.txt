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

#
# IclCodec
#

project(IclCodec)

set(ICL_CODEC_SOURCES icl_codec.cpp)
set(ICL_CODEC_LIBRATY)
if(ICL_WITH_QPL)
  list(APPEND ICL_CODEC_SOURCES qpl_codec.cpp)
endif()
if(ICL_WITH_QAT)
  list(APPEND ICL_CODEC_SOURCES qat_codec.cpp)
endif()
if(ICL_WITH_IGZIP)
  list(APPEND ICL_CODEC_SOURCES igzip_codec.cpp)
  list(APPEND ICL_CODEC_LINK_LIBS igzip_common)
endif()

add_library(icl_codec SHARED ${ICL_CODEC_SOURCES})
target_link_libraries(icl_codec ${ICL_CODEC_LINK_LIBS})
