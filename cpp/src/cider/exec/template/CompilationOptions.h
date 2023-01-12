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

#ifndef QUERYENGINE_COMPILATIONOPTIONS_H
#define QUERYENGINE_COMPILATIONOPTIONS_H

#include <cstdio>
#include <vector>

enum class ExecutorOptLevel { Default, LoopStrengthReduction, ReductionJIT };

enum class ExecutorExplainType { Default, Optimized };

enum class ExecutorDispatchMode { KernelPerFragment, MultifragmentKernel };

struct CompilationOptions {
  bool hoist_literals;
  ExecutorOptLevel opt_level;
  bool with_dynamic_watchdog;
  bool allow_lazy_fetch;
  bool filter_on_deleted_column{true};  // if false, ignore the delete column during table
                                        // scans. Primarily disabled for delete queries.
  ExecutorExplainType explain_type{ExecutorExplainType::Default};
  bool register_intel_jit_listener{false};
  bool use_cider_groupby_hash{false};
  bool use_default_col_range{false};
  bool use_cider_data_format{false};
  bool needs_error_check{false};
  bool use_nextgen_compiler{false};

  static CompilationOptions defaults() {
    return CompilationOptions{true,
                              ExecutorOptLevel::Default,
                              false,
                              true,
                              true,
                              ExecutorExplainType::Default,
                              false};
  }
};

enum class ExecutorType { Native, Extern, TableFunctions };

struct ExecutionOptions {
  bool output_columnar_hint;
  bool allow_multifrag;
  uint32_t just_explain;  // return the generated IR for the first step
  bool allow_loop_joins;
  bool with_watchdog;  // Per work unit, not global.
  bool jit_debug;
  bool just_validate;
  bool with_dynamic_watchdog;            // Per work unit, not global.
  unsigned dynamic_watchdog_time_limit;  // Dynamic watchdog time limit, in milliseconds.
  bool find_push_down_candidates;
  bool just_calcite_explain;
  bool allow_runtime_query_interrupt;
  double running_query_interrupt_freq;
  unsigned pending_query_interrupt_freq;
  ExecutorType executor_type = ExecutorType::Native;
  std::vector<size_t> outer_fragment_indices{};
  bool multifrag_result = false;
  bool preserve_order = false;

  static ExecutionOptions defaults() {
    return ExecutionOptions{false,
                            true,
                            0,
                            false,
                            true,
                            false,
                            false,
                            false,
                            0,
                            false,
                            false,
                            false,
                            0.5,
                            1000};
  }

  ExecutionOptions with_multifrag_result(bool enable = true) const {
    ExecutionOptions eo = *this;
    eo.multifrag_result = enable;
    return eo;
  }

  ExecutionOptions with_preserve_order(bool enable = true) const {
    ExecutionOptions eo = *this;
    eo.preserve_order = enable;
    return eo;
  }
};

#endif  // QUERYENGINE_COMPILATIONOPTIONS_H
