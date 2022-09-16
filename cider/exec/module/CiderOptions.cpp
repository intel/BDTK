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

#include "cider/CiderOptions.h"

// Compilation Options
DEFINE_bool(hoist_literals, true, "hoist literals");
DEFINE_bool(with_dynamic_watchdog, false, "with dynamic watchdog");
DEFINE_bool(allow_lazy_fetch, false, "allow lazy fetch");
DEFINE_bool(filter_on_deleted_column, true, "filter on deleted column");
DEFINE_uint64(max_groups_buffer_entry_guess, 0, "max groups buffer entry guess");
DEFINE_int32(crt_min_byte_width, 8, "crt min byte width");
DEFINE_bool(has_cardinality_estimation, true, "has cardinality estimation");
DEFINE_bool(use_cider_groupby_hash, true, "use cider groupby hash");
DEFINE_bool(use_default_col_range, true, "use default col range");
DEFINE_bool(use_cider_data_format, false, "use cider data format");
DEFINE_bool(needs_error_check, false, "needs error check");

// Execution Options
DEFINE_bool(output_columnar_hint, false, "output columnar hint");
DEFINE_bool(allow_multifrag, true, "allow multifrag");
DEFINE_bool(just_explain, true, "just explain");
DEFINE_bool(allow_loop_joins, false, "allow loop joins");
DEFINE_bool(jit_debug, true, "jit debug");
DEFINE_bool(with_watchdog, false, "with watchdog");
DEFINE_bool(just_validate, false, "just validate");
DEFINE_bool(with_dynamic_watchdog_exec, false, "with dynamic watchdog per work unit");
DEFINE_uint64(dynamic_watchdog_time_limit, 0, "dynamic watchdog time limit");
DEFINE_bool(find_push_down_candidates, false, "find push down candidates");
DEFINE_bool(just_calcite_explain, false, "just calcite explain");
DEFINE_bool(allow_runtime_query_interrupt, false, "allow runtime query interrupt");
DEFINE_double(running_query_interrupt_freq, 0.5, "running query interrupt freq");
DEFINE_uint64(pending_query_interrupt_freq, 1000, "pending query interrupt freq");
DEFINE_bool(force_direct_hash, false, "force direct hash");
