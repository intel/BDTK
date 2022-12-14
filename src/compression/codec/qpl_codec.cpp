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

#include "iax_wrapper.h"
#include "icl_codec_internal.h"

#include <cstddef>
#include <cstdint>

namespace icl {
namespace codec {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Qpl implementation

class QplCodec : public IclCompressionCodec {
 public:
  int64_t Decompress(int64_t input_len,
                     const uint8_t* input,
                     int64_t output_buffer_len,
                     uint8_t* output_buffer) override {
    int32_t decompressed_size = static_cast<int32_t>(output_buffer_len);
    int32_t status = iax_wrapper_decompress(
        input, static_cast<int32_t>(input_len), output_buffer, &decompressed_size);
    if (status) {
      return -1;
    }

    return static_cast<int64_t>(decompressed_size);
  }

  int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) override {
    return iax_wrapper_max_compress_len(static_cast<int32_t>(input_len));
  }

  int64_t Compress(int64_t input_len,
                   const uint8_t* input,
                   int64_t output_buffer_len,
                   uint8_t* output_buffer) override {
    int32_t compressed_size = static_cast<int32_t>(output_buffer_len);
    int32_t status = iax_wrapper_compress(
        input, static_cast<int32_t>(input_len), output_buffer, &compressed_size);
    if (status) {
      return -1;
    }

    return static_cast<int64_t>(compressed_size);
  }
};

}  // namespace

std::unique_ptr<IclCompressionCodec> MakeQplCodec(int compression_level) {
  return std::unique_ptr<IclCompressionCodec>(new QplCodec());
}

}  // namespace internal
}  // namespace codec
}  // namespace icl
