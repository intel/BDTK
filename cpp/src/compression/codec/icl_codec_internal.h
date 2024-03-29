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
#ifndef _ICL_COMPRESSION_CODEC_ICL_CODEC_INTERNAL_H_
#define _ICL_COMPRESSION_CODEC_ICL_CODEC_INTERNAL_H_

#include "icl_codec.h"

#include <memory>

namespace icl {
namespace codec {

// ----------------------------------------------------------------------
// Internal Codec factories
namespace internal {

// Qpl codec.
constexpr int kQplDefaultCompressionLevel = 1;

std::unique_ptr<IclCompressionCodec> MakeQplCodec(
    int compression_level = kQplDefaultCompressionLevel);

// Qat codec.
constexpr int kQatDefaultCompressionLevel = 1;

std::unique_ptr<IclCompressionCodec> MakeQatCodec(
    int compression_level = kQatDefaultCompressionLevel);

// IGzip Codec
constexpr int kIgzipDefaultCompressionLevel = 1;

std::unique_ptr<IclCompressionCodec> MakeIgzipCodec(
    int compression_level = kIgzipDefaultCompressionLevel);

}  // namespace internal
}  // namespace codec
}  // namespace icl

#endif  // _ICL_COMPRESSION_CODEC_ICL_CODEC_INTERNAL_H_
