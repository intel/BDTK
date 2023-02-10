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
# IclCodec
#

project(IclCodec)

set(ICL_CODEC_LIBRATY)
set(ICL_BUNDLED_STATIC_LIBS)
set(ICL_CODEC_SOURCES icl_codec.cpp)
if(ICL_WITH_QPL)
  list(APPEND ICL_CODEC_SOURCES qpl_codec.cpp)
  list(APPEND ICL_CODEC_LINK_LIBS qpl_common)
endif()
if(ICL_WITH_QAT)
  list(APPEND ICL_CODEC_SOURCES qat_codec.cpp)
  list(APPEND ICL_CODEC_LINK_LIBS qat_common)
endif()
if(ICL_WITH_IGZIP)
  list(APPEND ICL_CODEC_SOURCES igzip_codec.cpp)
  list(APPEND ICL_CODEC_LINK_LIBS igzip_common)
  list(APPEND ICL_BUNDLED_STATIC_LIBS igzip_common)
  list(APPEND ICL_BUNDLED_STATIC_LIBS ISAL::igzip)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_library(icl_codec SHARED ${ICL_CODEC_SOURCES})
target_link_libraries(icl_codec ${ICL_CODEC_LINK_LIBS})

add_library(icl_codec_static ${ICL_CODEC_SOURCES})
target_link_libraries(icl_codec_static ${ICL_CODEC_LINK_LIBS})

list(APPEND ICL_BUNDLED_STATIC_LIBS icl_codec_static)

icl_car(_FIRST_LIB ${ICL_BUNDLED_STATIC_LIBS})
icl_cdr(_OTHER_LIBS ${ICL_BUNDLED_STATIC_LIBS})
create_merged_static_lib(
  icl_bundled_dependencies
  NAME
  icl_codec
  ROOT
  ${_FIRST_LIB}
  TO_MERGE
  ${_OTHER_LIBS})
