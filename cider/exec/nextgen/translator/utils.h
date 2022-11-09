/*
 * Copyright (c) 2022 Intel Corporation.
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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_UTILS_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_UTILS_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/translator/dummy.h"
#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::translator {
using namespace cider::jitlib;

JITTypeTag getJITTag(const Analyzer::Expr* col_var) {
  CHECK(!col_var);
  const auto& col_ti = col_var->get_type_info();
  switch (col_ti.get_type()) {
    case kINT:
      return JITTypeTag::INT32;
    default:
      TODO("MaJian", "add other type maps");
  }
  return JITTypeTag::INVALID;
}

JITTypeTag getJITTag(JITValuePointer& ptr) {
  return JITTypeTag::INVALID;
}

}  // namespace cider::exec::nextgen::translator
#endif
