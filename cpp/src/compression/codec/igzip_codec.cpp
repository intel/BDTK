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
#include "common/igzip/igzip_wrapper.h"

#include <cstddef>
#include <cstdint>

namespace icl {
namespace codec {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Igzip implementation

class IgzipCodec : public IclCompressionCodec {
 public:
  explicit IgzipCodec(int compression_level) {
    context_ =
        static_cast<struct igzip_wrapper_context*>(igzip_wrapper_init(compression_level));
  }

  ~IgzipCodec() override { igzip_wrapper_destroy(context_); }

  int64_t Decompress(int64_t input_length,
                     const uint8_t* input,
                     int64_t output_length,
                     uint8_t* output) override {
    int64_t decompressed_size =
        igzip_wrapper_decompress(context_, input_length, input, output_length, output);
    return decompressed_size;
  }

  int64_t MaxCompressedLen(int64_t input_length, const uint8_t* input) override {
    return igzip_wrapper_max_compressed_len(context_, input_length, input);
  }

  int64_t Compress(int64_t input_length,
                   const uint8_t* input,
                   int64_t output_length,
                   uint8_t* output) override {
    int64_t compressed_size =
        igzip_wrapper_compress(context_, input_length, input, output_length, output);
    return compressed_size;
  }

  int minimum_compression_level() const override {
    return igzip_wrapper_minimum_compression_level();
  }

  int maximum_compression_level() const override {
    return igzip_wrapper_maximum_compression_level();
  }

  int default_compression_level() const override {
    return igzip_wrapper_default_compression_level();
  }

 private:
  struct igzip_wrapper_context* context_;
};

}  // namespace

std::unique_ptr<IclCompressionCodec> MakeIgzipCodec(int compression_level) {
  return std::unique_ptr<IclCompressionCodec>(new IgzipCodec(compression_level));
}

}  // namespace internal
}  // namespace codec
}  // namespace icl
