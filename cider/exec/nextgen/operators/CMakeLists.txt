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

macro(add_opnode op_node_name)
  list(APPEND OPERATORS_SOURCE ${CMAKE_CURRENT_LIST_DIR}/${op_node_name}.cpp)
endmacro(add_opnode)

add_opnode(FilterNode)
add_opnode(ProjectNode)
add_opnode(ArrowSourceNode)
add_opnode(ColumnToRowNode)
add_opnode(RowToColumnNode)
add_opnode(HashJoinNode)

add_library(cider_operators OBJECT ${OPERATORS_SOURCE})
