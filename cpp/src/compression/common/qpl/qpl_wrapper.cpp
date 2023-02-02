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
#include <vector>

#include <chaining/operation_chain.hpp>
#include <operations/analytics/extract_operation.hpp>
#include <operations/compression/deflate_operation.hpp>
#include <operations/compression/inflate_operation.hpp>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef USE_IAX_LOW_LEVEL_API
int32_t iax_wrapper_compress(const uint8_t* pSrc,
                             int32_t srcLen,
                             uint8_t* pDst,
                             int32_t* pDstLen) {
  IaxJob* pJob;
  Iax32u jobSize;
  IaxStatus status = iaxStsOk;

  iaxGetJobSize(iaxPathSW, &jobSize);

  pJob = reinterpret_cast<IaxJob*>(new Iax8u[jobSize]);

  iaxInitJob(iaxPathSW, pJob);

  pJob->op = IAX_OP_COMPRESS;
  pJob->nextIn = pSrc;
  pJob->nextOut = pDst;
  pJob->availIn = srcLen;
  pJob->availOut = *pDstLen;
  pJob->flags = IAX_FLAG_FIRST | IAX_FLAG_DYNAMIC_HUFFMAN | IAX_FLAG_LAST;

  status = iaxSubmitJob(pJob);
  if (iaxStsOk != status) {
    std::cerr << "Failed to submit compression job:" << status << std::endl;
  }

  *pDstLen = pJob->totalOut;

  delete[] pJob;

  return status;
}

int32_t iax_wrapper_decompress(const uint8_t* pSrc,
                               int32_t srcLen,
                               uint8_t* pDst,
                               int32_t* pDstLen) {
  IaxJob* pJob;
  Iax32u jobSize;
  IaxStatus status = iaxStsOk;

  iaxGetJobSize(iaxPathSW, &jobSize);

  pJob = reinterpret_cast<IaxJob*>(new Iax8u[jobSize]);

  iaxInitJob(iaxPathSW, pJob);

  pJob->op = IAX_OP_DECOMPRESS;
  pJob->nextIn = pSrc;
  pJob->nextOut = pDst;
  pJob->availIn = srcLen;
  pJob->availOut = *pDstLen;
  pJob->flags = IAX_FLAG_FIRST | IAX_FLAG_LAST;

  status = iaxSubmitJob(pJob);
  if (iaxStsOk != status) {
    std::cerr << "Failed to submit decompression job:" << status << std::endl;
  }

  *pDstLen = pJob->totalOut;

  delete[] pJob;

  return status;
}

int32_t iax_wrapper_decode_parquet_rle(uint32_t valueCount,
                                       const uint8_t* pSrc,
                                       int32_t srcLen,
                                       uint8_t* pDst,
                                       int32_t* pDstLen) {
  IaxJob* pJob;
  Iax32u jobSize;
  IaxStatus status = iaxStsOk;

  if (*pDstLen < valueCount * sizeof(Iax32u)) {
    std::cerr << "Dst buffer too small for decode job" << std::endl;
  }

  iaxGetJobSize(iaxPathSW, &jobSize);

  pJob = reinterpret_cast<IaxJob*>(new Iax8u[jobSize]);

  iaxInitJob(iaxPathSW, pJob);

  pJob->op = IAX_OP_EXTRACT;
  pJob->nextIn = pSrc;
  pJob->availIn = srcLen;
  pJob->nextOut = pDst;
  pJob->availOut = *pDstLen;

  pJob->parser = IAXP_PARQUET_RLE;
  pJob->numInputElements = valueCount;
  // pJob->src1BitWidth     = src1BitWidth;
  pJob->outBitWidth = IAXOW_32;
  pJob->paramLow = 0;
  pJob->paramHigh = valueCount - 1;

  status = iaxSubmitJob(pJob);
  if (iaxStsOk != status) {
    std::cerr << "Failed to submit decode parquet rle job:" << status << std::endl;
  }

  delete[] pJob;

  return status;
}
#else
int32_t iax_wrapper_compress(const uint8_t* pSrc,
                             int32_t srcLen,
                             uint8_t* pDst,
                             int32_t* pDstLen) {
  uint32_t resultSize = 0;
  uint32_t resultStatus = 0;
  auto deflateOperation = qpl::deflate_operation::builder()
                              .compression_level(qpl::compression_levels::default_level)
                              .compression_mode<qpl::compression_modes::dynamic_mode>()
                              .gzip_mode(false)
                              .build();
  // auto result = qpl::execute<qpl::auto_detect>(deflateOperation,
  // auto result = qpl::execute<qpl::hardware>(deflateOperation,
  auto result = qpl::execute<qpl::software>(
      deflateOperation, pSrc, pSrc + srcLen, pDst, pDst + *pDstLen);
  result.handle([&resultSize](uint32_t value) -> void { resultSize = value; },
                [&resultStatus](uint32_t status) -> void {
                  try {
                    resultStatus = status;
                    qpl::util::handle_status(status);
                  } catch (qpl::exception& e) {
                    std::cerr << "Failed to submit compression job: " << e.what()
                              << std::endl;
                  } catch (...) {
                    std::cerr << "Failed to submit compression job: "
                              << "Unknown Exception" << std::endl;
                  }
                });
  *pDstLen = resultSize;
  return resultStatus;
}

int32_t iax_wrapper_decompress(const uint8_t* pSrc,
                               int32_t srcLen,
                               uint8_t* pDst,
                               int32_t* pDstLen) {
  int32_t resultStatus = 0;
  uint32_t resultSize = 0;
  auto inflateOperation = qpl::inflate_operation();
  // auto result = qpl::execute<qpl::auto_detect>(inflateOperation,
  // auto result = qpl::execute<qpl::hardware>(inflateOperation,
  auto result = qpl::execute<qpl::software>(
      inflateOperation, pSrc, pSrc + srcLen, pDst, pDst + *pDstLen);
  result.handle([&resultSize](uint32_t value) -> void { resultSize = value; },
                [&resultStatus](uint32_t status) -> void {
                  try {
                    resultStatus = status;
                    qpl::util::handle_status(status);
                  } catch (qpl::exception& e) {
                    std::cerr << "Failed to submit decompression job: " << e.what()
                              << std::endl;
                  } catch (...) {
                    std::cerr << "Failed to submit decompression job: "
                              << "Unknown Exception" << std::endl;
                  }
                });
  *pDstLen = resultSize;
  return resultStatus;
}

int32_t iax_wrapper_decode_parquet_rle(uint32_t valueCount,
                                       const uint8_t* pSrc,
                                       int32_t srcLen,
                                       uint8_t* pDst,
                                       int32_t* pDstLen) {
  int32_t resultStatus = 0;
  uint32_t resultSize = 0;
  auto extractOperation = qpl::extract_operation::builder(0, valueCount - 1)
                              .parser<qpl::parsers::parquet_rle>(valueCount)
                              .output_vector_width(32)
                              .build();
  // auto result = qpl::execute<qpl::auto_detect>(extractOperation,
  // auto result = qpl::execute<qpl::hardware>(extractOperation,
  auto result = qpl::execute<qpl::software>(
      extractOperation, pSrc, pSrc + srcLen, pDst, pDst + *pDstLen);
  result.handle([&resultSize](uint32_t value) -> void { resultSize = value; },
                [&resultStatus](uint32_t status) -> void {
                  try {
                    resultStatus = status;
                    qpl::util::handle_status(status);
                  } catch (qpl::exception& e) {
                    std::cerr << "Failed to submit parquet rle exact job: " << e.what()
                              << std::endl;
                  } catch (...) {
                    std::cerr << "Failed to submit parquet rle exact job: "
                              << "Unknown Exception" << std::endl;
                  }
                });
  *pDstLen = resultSize;
  return resultStatus;
}

int32_t iax_wrapper_decompress_decode_parquet_rle(int32_t valueCount,
                                                  const uint8_t* pSrc,
                                                  int32_t srcLen,
                                                  uint8_t* pDst,
                                                  int32_t* pDstLen) {
  std::vector<uint8_t> src(pSrc, pSrc + srcLen);
  std::vector<uint8_t> dst(*pDstLen);
  int32_t resultStatus = 0;
  uint32_t resultSize = 0;
  auto inflateOperation = qpl::inflate_operation();
  auto extractOperation = qpl::extract_operation::builder(0, valueCount - 1)
                              .parser<qpl::parsers::parquet_rle>(valueCount)
                              .output_vector_width(32)
                              .build();
  auto chain = inflateOperation | extractOperation;
  // auto result = qpl::execute<qpl::auto_detect>(chain, src, dst);
  // auto result = qpl::execute<qpl::hardware>(chain, src, dst);
  auto result = qpl::execute<qpl::software>(chain, src, dst);
  result.handle([&resultSize](uint32_t value) -> void { resultSize = value; },
                [&resultStatus](uint32_t status) -> void {
                  try {
                    resultStatus = status;
                    qpl::util::handle_status(status);
                  } catch (qpl::exception& e) {
                    std::cerr << "Failed to submit parquet decompress and exact job: "
                              << e.what() << std::endl;
                  } catch (...) {
                    std::cerr << "Failed to submit parquet decompress and exact job: "
                              << "Unknown Exception" << std::endl;
                  }
                });
  std::memcpy(pDst, dst.data(), resultSize);
  *pDstLen = resultSize;
  return resultStatus;
}

int32_t iax_wrapper_max_compress_len(int32_t input_length) {
  // TODO: should get from iax user library
  return input_length > 1024 ? input_length * 3 / 2 : 1024;
}

#ifdef __cplusplus
}
#endif

#endif
