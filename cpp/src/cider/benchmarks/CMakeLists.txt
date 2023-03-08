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

find_package(FMT REQUIRED)

set(DEP_LIBS benchmark::benchmark nextgen)

add_executable(jitlib_benchmark JITlibBenchmark.cpp)
add_executable(HashMapBenchmark HashMapBenchmark.cpp)

set(JITlibBenchmarkCompileFlag -g)
if(CIDER_ENABLE_AVX512)
  list(APPEND JITlibBenchmarkCompileFlag -mavx512f)
endif()

target_compile_options(jitlib_benchmark PRIVATE ${JITlibBenchmarkCompileFlag})
target_compile_options(HashMapBenchmark PRIVATE -msse4.2 -msse2)

target_link_libraries(jitlib_benchmark ${DEP_LIBS} folly fmt::fmt)
target_link_libraries(HashMapBenchmark benchmark::benchmark ${DEP_LIBS} folly
                      cider_hashtable)
