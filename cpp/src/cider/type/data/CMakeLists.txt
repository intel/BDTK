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
add_library(cider_type_data string/StringDictionary.cpp
                            string/StringDictionaryProxy.cpp)

if(ENABLE_FOLLY)
  target_link_libraries(
    cider_type_data
    cider_util
    ${Boost_LIBRARIES}
    ${Thrift_LIBRARIES}
    ${PROFILER_LIBS}
    ${Folly_LIBRARIES}
    ${TBB_LIBS})
else()
  target_link_libraries(cider_type_data cider_util cider_function ${Boost_LIBRARIES}
                        ${Thrift_LIBRARIES} ${PROFILER_LIBS} ${TBB_LIBS})
endif()
