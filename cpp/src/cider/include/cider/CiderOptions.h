/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#ifndef CIDER_CIDEROPTIONS_H
#define CIDER_CIDEROPTIONS_H

#include <gflags/gflags_declare.h>

DECLARE_bool(needs_error_check);
DECLARE_bool(dump_ir);

namespace cider {
// compilation config info
struct CompilationOptions {
  bool optimize_ir = true;
  bool aggressive_jit_compile = true;
  bool dump_ir = FLAGS_dump_ir;
  bool enable_vectorize = true;
  bool enable_avx2 = true;
  bool enable_avx512 = false;
};

struct CodegenOptions {
  bool needs_error_check = FLAGS_needs_error_check;
  bool check_bit_vector_clear_opt = false;
  bool set_null_bit_vector_opt = false;
  bool branchless_logic = true;
  bool enable_vectorize = true;

  CompilationOptions co{};
};

}  // namespace cider
#endif  // CIDER_CIDEROPTIONS_H
