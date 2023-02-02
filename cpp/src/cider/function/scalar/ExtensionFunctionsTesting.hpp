/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#define CPU_DEVICE_CODE 0x637075  // 'cpu' in hex

EXTENSION_NOINLINE
int32_t ct_device_selection_udf_any(int32_t input) {
  return CPU_DEVICE_CODE;
}

EXTENSION_NOINLINE
int32_t ct_device_selection_udf_cpu__cpu_(int32_t input) {
  return CPU_DEVICE_CODE;
}

EXTENSION_NOINLINE
int32_t ct_device_selection_udf_both__cpu_(int32_t input) {
  return CPU_DEVICE_CODE;
}

#undef CPU_DEVICE_CODE
