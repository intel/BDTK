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
#ifndef _ICL_COMPRESSION_CODEC_ICL_CODEC_H_
#define _ICL_COMPRESSION_CODEC_ICL_CODEC_H_

#include <cstdint>
#include <memory>

namespace icl {
namespace codec {

/// \brief ICL Compression codec
class IclCompressionCodec {
 public:
  virtual ~IclCompressionCodec() = default;

  static std::unique_ptr<IclCompressionCodec> MakeIclCompressionCodec(
      std::string codec_name,
      int compression_level);

  /// \brief One-shot decompression function
  ///
  /// output_buffer_len must be correct and therefore be obtained in advance.
  /// The actual decompressed length is returned.
  virtual int64_t Decompress(int64_t input_len,
                             const uint8_t* input,
                             int64_t output_buffer_len,
                             uint8_t* output_buffer) = 0;

  /// \brief One-shot compression function
  ///
  /// output_buffer_len must first have been computed using MaxCompressedLen().
  /// The actual compressed length is returned.
  virtual int64_t Compress(int64_t input_len,
                           const uint8_t* input,
                           int64_t output_buffer_len,
                           uint8_t* output_buffer) = 0;

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) = 0;

  /// \brief Return the smallest supported compression level
  virtual int minimum_compression_level() const = 0;

  /// \brief Return the largest supported compression level
  virtual int maximum_compression_level() const = 0;

  /// \brief Return the default compression level
  virtual int default_compression_level() const = 0;
};

}  // namespace codec
}  // namespace icl

#endif  // _ICL_COMPRESSION_CODEC_ICL_CODEC_H_
