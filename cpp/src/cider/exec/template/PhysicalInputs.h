/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef CIDER_EXEC_PHYSICALINPUTS_H
#define CIDER_EXEC_PHYSICALINPUTS_H

// #include "exec/template/common/descriptors/InputDescriptors.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "type/plan/Analyzer.h"
#include "type/schema/ColumnInfo.h"
#include "type/schema/TableInfo.h"

#include <ostream>
#include <unordered_set>

struct PhysicalInput {
  int col_id;
  int table_id;

  bool operator==(const PhysicalInput& that) const {
    return col_id == that.col_id && table_id == that.table_id;
  }
};

std::ostream& operator<<(std::ostream&, PhysicalInput const&);

namespace std {

template <>
struct hash<PhysicalInput> {
  size_t operator()(const PhysicalInput& phys_input) const {
    return phys_input.col_id ^ phys_input.table_id;
  }
};
}  // namespace std

inline InputColDescriptor column_var_to_descriptor(const Analyzer::ColumnVar* var) {
  return InputColDescriptor(var->get_column_info(), var->get_rte_idx());
}

#endif  // CIDER_EXEC_PHYSICALINPUTS_H
