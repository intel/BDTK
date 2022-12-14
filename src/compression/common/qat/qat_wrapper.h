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

#ifndef _ICL_COMPRESSION_COMMON_QAT_WRAPPER_H_
#define _ICL_COMPRESSION_COMMON_QAT_WRAPPER_H_

#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

int32_t qat_wrapper_max_compress_len(int32_t input_length);

int32_t qat_wrapper_compress(const uint8_t* pSrc,
                             int32_t srcLen,
                             uint8_t* pDst,
                             int32_t* pDstLen);

int32_t qat_wrapper_decompress(const uint8_t* pSrc,
                               int32_t srcLen,
                               uint8_t* pDst,
                               int32_t* pDstLen);

#ifdef __cplusplus
}
#endif

#endif  // _ICL_COMPRESSION_COMMON_QAT_WRAPPER_H_
