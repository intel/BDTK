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

#ifndef _ICL_COMPRESSION_COMMON_IGZIP_WRAPPER_H_
#define _ICL_COMPRESSION_COMMON_IGZIP_WRAPPER_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct igzip_wrapper_context {
  int compression_level;
  uint8_t* level_buf;
  uint32_t level_buf_size;
} igzip_wrapper_context_t;

void* igzip_wrapper_init(int compression_level);

void igzip_wrapper_destroy(void* context);

int64_t igzip_wrapper_max_compressed_len(int64_t input_length, const uint8_t* input);

int64_t igzip_wrapper_compress(void* context,
                               int64_t input_length,
                               const uint8_t* input,
                               int64_t output_length,
                               uint8_t* output);

int64_t igzip_wrapper_decompress(void* context,
                                 int64_t input_length,
                                 const uint8_t* input,
                                 int64_t output_length,
                                 uint8_t* output);

int igzip_wrapper_minimum_compression_level();

int igzip_wrapper_maximum_compression_level();

int igzip_wrapper_default_compression_level();

#ifdef __cplusplus
}
#endif

#endif  // _ICL_COMPRESSION_COMMON_QPL_WRAPPER_H_
