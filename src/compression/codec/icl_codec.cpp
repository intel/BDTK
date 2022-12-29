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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>

#include "codec/icl_codec.h"
#include "codec/icl_codec_internal.h"

namespace icl {
namespace codec {

std::unique_ptr<IclCompressionCodec> IclCompressionCodec::MakeIclCompressionCodec(
    std::string codec_name,
    int compression_level) {
  std::transform(
      codec_name.begin(), codec_name.end(), codec_name.begin(), [](unsigned char c) {
        return std::toupper(c);
      });
  if (codec_name == "QPL") {
#ifdef ICL_WITH_QPL
    return internal::MakeQplCodec(compression_level);
#else
    goto OUT_NOT_BUILT;
#endif
  } else if (codec_name == "QAT") {
#ifdef ICL_WITH_QAT
    return internal::MakeQatCodec(compression_level);
#else
    goto OUT_NOT_BUILT;
#endif
  } else if (codec_name == "IGZIP") {
#ifdef ICL_WITH_IGZIP
    return internal::MakeIgzipCodec(compression_level);
#else
    goto OUT_NOT_BUILT;
#endif
  } else {
#ifdef ICL_WITH_IGZIP
    std::cerr << "Unsupported backend \'" << codec_name << "\', fallback to IGZIP codec"
              << std::endl;
    return internal::MakeIgzipCodec(compression_level);
#endif
    std::cerr << "Unsupported backend \'" << codec_name << "\'" << std::endl;
    return nullptr;
  }

OUT_NOT_BUILT:
  std::cerr << "Support for backend \'" << codec_name << "\' not built" << std::endl;
  return nullptr;
}

}  // namespace codec
}  // namespace icl
