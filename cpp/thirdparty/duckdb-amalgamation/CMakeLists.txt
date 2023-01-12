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

set(DUCKDB_FILES duckdb.cpp)
set_source_files_properties(${DUCKDB_FILES} PROPERTIES GENERATED TRUE)
add_custom_command(
  OUTPUT ${DUCKDB_FILES}
  COMMAND cd ${CMAKE_SOURCE_DIR}/thirdparty/duckdb && python
          scripts/amalgamation.py --extended
  COMMAND
    ${CMAKE_COMMAND} -E copy
    ${CMAKE_SOURCE_DIR}/thirdparty/duckdb/src/amalgamation/duckdb.cpp
    ${CMAKE_BINARY_DIR}/thirdparty/duckdb-amalgamation
  COMMAND
    ${CMAKE_COMMAND} -E copy
    ${CMAKE_SOURCE_DIR}/thirdparty/duckdb/src/amalgamation/duckdb.hpp
    ${CMAKE_BINARY_DIR}/thirdparty/duckdb-amalgamation)

add_compile_definitions(DISABLE_DUCKDB_REMOTE_INSTALL)

add_library(duckdb ${DUCKDB_FILES})

target_link_libraries(duckdb dl)

set_property(TARGET duckdb PROPERTY JOB_POOL_COMPILE)
