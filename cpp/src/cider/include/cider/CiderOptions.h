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

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <cstddef>
#include <cstdint>

DECLARE_bool(hoist_literals);
DECLARE_bool(with_dynamic_watchdog);
DECLARE_bool(allow_lazy_fetch);
DECLARE_bool(filter_on_deleted_column);
DECLARE_uint64(max_groups_buffer_entry_guess);
DECLARE_int32(crt_min_byte_width);
DECLARE_bool(has_cardinality_estimation);
DECLARE_bool(use_cider_groupby_hash);
DECLARE_bool(use_default_col_range);
DECLARE_bool(use_cider_data_format);
DECLARE_bool(needs_error_check);
DECLARE_bool(use_nextgen_compiler);
DECLARE_bool(null_separate);

DECLARE_bool(output_columnar_hint);
DECLARE_bool(allow_multifrag);
DECLARE_uint32(just_explain);
DECLARE_bool(allow_loop_joins);
DECLARE_bool(jit_debug);
DECLARE_bool(with_watchdog);
DECLARE_bool(just_validate);
DECLARE_bool(with_dynamic_watchdog_exec);
DECLARE_uint64(dynamic_watchdog_time_limit);
DECLARE_bool(find_push_down_candidates);
DECLARE_bool(just_calcite_explain);
DECLARE_bool(allow_runtime_query_interrupt);
DECLARE_double(running_query_interrupt_freq);
DECLARE_uint64(pending_query_interrupt_freq);
DECLARE_bool(force_direct_hash);

// wrapper for Omnisci CompilationOptions
struct CiderCompilationOption {
  bool hoist_literals;
  bool with_dynamic_watchdog;
  bool allow_lazy_fetch;
  bool filter_on_deleted_column;
  size_t max_groups_buffer_entry_guess;
  int8_t crt_min_byte_width;
  bool has_cardinality_estimation;

  bool use_cider_groupby_hash;
  bool use_default_col_range;
  bool use_cider_data_format;
  bool needs_error_check;
  bool use_nextgen_compiler;

  static CiderCompilationOption defaults() {
    return CiderCompilationOption{FLAGS_hoist_literals,
                                  FLAGS_with_dynamic_watchdog,
                                  FLAGS_allow_lazy_fetch,
                                  FLAGS_filter_on_deleted_column,
                                  FLAGS_max_groups_buffer_entry_guess,
                                  (int8_t)FLAGS_crt_min_byte_width,
                                  FLAGS_has_cardinality_estimation,
                                  FLAGS_use_cider_groupby_hash,
                                  FLAGS_use_default_col_range,
                                  FLAGS_use_cider_data_format,
                                  FLAGS_needs_error_check,
                                  FLAGS_use_nextgen_compiler};
  }
};

// wrapper for Omnisci ExecutionOptions
struct CiderExecutionOption {
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
  bool force_direct_hash;

  static CiderExecutionOption defaults() {
    return CiderExecutionOption{FLAGS_output_columnar_hint,
                                FLAGS_allow_multifrag,
                                FLAGS_just_explain,
                                FLAGS_allow_loop_joins,
                                FLAGS_jit_debug,
                                FLAGS_with_watchdog,
                                FLAGS_just_validate,
                                FLAGS_with_dynamic_watchdog_exec,
                                (unsigned)FLAGS_dynamic_watchdog_time_limit,
                                FLAGS_find_push_down_candidates,
                                FLAGS_just_calcite_explain,
                                FLAGS_allow_runtime_query_interrupt,
                                FLAGS_running_query_interrupt_freq,
                                (unsigned)FLAGS_pending_query_interrupt_freq,
                                FLAGS_force_direct_hash};
  }

 private:
  // hashtable
  // to be added
};

#endif  // CIDER_CIDEROPTIONS_H
