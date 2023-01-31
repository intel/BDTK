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

#include <iostream>

#include <qatzip.h>

__thread QzSession_T g_qzSession = {
    .internal = NULL,
};

#ifdef __cplusplus
extern "C" {
#endif

int32_t qat_wrapper_compress(const uint8_t* pSrc,
                             int32_t srcLen,
                             uint8_t* pDst,
                             int32_t* pDstLen) {
  uint32_t uncompressed_size = static_cast<uint32_t>(srcLen);
  uint32_t compressed_size = static_cast<uint32_t>(*pDstLen);
  int32_t status =
      qzCompress(&g_qzSession, pSrc, &uncompressed_size, pDst, &compressed_size, 1);
  if (status == QZ_OK) {
    *pDstLen = compressed_size;
  } else if (status == QZ_PARAMS) {
    std::cerr << "QAT compression failure: params is invalid" << std::endl;
  } else if (status == QZ_FAIL) {
    std::cerr << "QAT compression failure: Function did not succeed" << std::endl;
  } else {
    std::cerr << "QAT compression failure: Error code " << status << std::endl;
  }

  return status;
}

int32_t qat_wrapper_decompress(const uint8_t* pSrc,
                               int32_t srcLen,
                               uint8_t* pDst,
                               int32_t* pDstLen) {
  uint32_t compressed_size = static_cast<uint32_t>(srcLen);
  uint32_t uncompressed_size = static_cast<uint32_t>(*pDstLen);
  int32_t status =
      qzDecompress(&g_qzSession, pSrc, &compressed_size, pDst, &uncompressed_size);
  if (status == QZ_OK) {
    *pDstLen = uncompressed_size;
  } else if (status == QZ_PARAMS) {
    std::cerr << "QAT decompression failure: params is invalid" << std::endl;
  } else if (status == QZ_FAIL) {
    std::cerr << "QAT decompression failure: Function did not succeed" << std::endl;
  } else {
    std::cerr << "QAT decompression failure: Error code " << status << std::endl;
  }

  return status;
}

int32_t qat_wrapper_max_compress_len(int32_t input_length) {
  return qzMaxCompressedLength(static_cast<size_t>(input_length), &g_qzSession);
}

#ifdef __cplusplus
}
#endif
