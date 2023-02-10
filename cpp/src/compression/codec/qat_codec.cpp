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

#include "codec/icl_codec_internal.h"
#include "common/qat/qat_wrapper.h"

#include <cstddef>
#include <cstdint>

namespace icl {
namespace codec {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Qat implementation

class QatCodec : public IclCompressionCodec {
 public:
  explicit QatCodec(int compression_level) {
    context_ =
        static_cast<struct qat_wrapper_context*>(qat_wrapper_init(compression_level));
  }

  ~QatCodec() override { qat_wrapper_destroy(context_); }

  int64_t Decompress(int64_t input_length,
                     const uint8_t* input,
                     int64_t output_length,
                     uint8_t* output) override {
    int64_t decompressed_size =
        qat_wrapper_decompress(context_, input_length, input, output_length, output);
    return decompressed_size;
  }

  int64_t MaxCompressedLen(int64_t input_length, const uint8_t* input) override {
    return qat_wrapper_max_compressed_len(context_, input_length, input);
  }

  int64_t Compress(int64_t input_length,
                   const uint8_t* input,
                   int64_t output_length,
                   uint8_t* output) override {
    int64_t compressed_size =
        qat_wrapper_compress(context_, input_length, input, output_length, output);
    return compressed_size;
  }

  int minimum_compression_level() const override {
    return qat_wrapper_minimum_compression_level();
  }

  int maximum_compression_level() const override {
    return qat_wrapper_maximum_compression_level();
  }

  int default_compression_level() const override {
    return qat_wrapper_default_compression_level();
  }

 private:
  struct qat_wrapper_context* context_;
};

}  // namespace

std::unique_ptr<IclCompressionCodec> MakeQatCodec(int compression_level) {
  return std::unique_ptr<IclCompressionCodec>(new QatCodec(compression_level));
}

}  // namespace internal
}  // namespace codec
}  // namespace icl
