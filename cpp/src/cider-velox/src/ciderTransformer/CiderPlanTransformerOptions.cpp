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

#include "CiderPlanTransformerOptions.h"

DEFINE_bool(enable_flatten_dictionary_encoding, false, "Enable flatten dictionary encoding to flat encoding");
DEFINE_bool(left_deep_join_pattern, false, "Enable LeftDeepJoinPattern ");
DEFINE_bool(compound_pattern, false, "Enable CompoundPattern ");
DEFINE_bool(filter_pattern, true, "Enable FilterPattern ");
DEFINE_bool(project_pattern, true, "Enable ProjectPattern ");
DEFINE_bool(partial_agg_pattern, false, "Enable PartialAggPattern ");
DEFINE_bool(top_n_pattern, false, "Enable TopNPattern ");
DEFINE_bool(order_by_pattern, false, "Enable OrderByPattern ");
