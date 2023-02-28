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

set(DEP_LIBS ${EXECUTE_TEST_LIBS})

set(TEST_ARGS "--gtest_output=xml:../")

find_package(folly REQUIRED)
find_package(gflags REQUIRED)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt")

set_and_check(FOLLY_INCLUDE_DIR /usr/local/include/folly)
set_and_check(FOLLY_CMAKE_DIR /usr/local/lib/cmake/folly)
if(NOT TARGET Folly::folly)
  include("${FOLLY_CMAKE_DIR}/folly-targets.cmake")
endif()

include_directories(${folly_INCLUDE_DIRS})

add_executable(CiderHashTableTest CiderHashTableTest.cpp)
target_link_libraries(CiderHashTableTest ${DEP_LIBS} ${FOLLY_LIBRARIES})
add_test(CiderHashTableTest ${EXECUTABLE_OUTPUT_PATH}/CiderHashTableTest ${TEST_ARGS})
