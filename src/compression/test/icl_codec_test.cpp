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

#include "codec/icl_codec.h"

#include <gtest/gtest.h>
#include <random>

using namespace icl::codec;

namespace {

void randomBytes(int64_t n, uint8_t* out) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
}

std::vector<uint8_t> MakeRandomData(int data_size) {
  std::vector<uint8_t> data(data_size);
  randomBytes(data_size, data.data());
  return data;
}
// Check roundtrip of one-shot compression and decompression functions.
void CheckCodecRoundtrip(std::unique_ptr<IclCompressionCodec>& codec,
                         const std::vector<uint8_t>& data) {
  int max_compressed_len =
      static_cast<int>(codec->MaxCompressedLen(data.size(), data.data()));
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size());

  int64_t actual_size =
      codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data());
  compressed.resize(actual_size);

  int64_t actual_decompressed_size = codec->Decompress(
      compressed.size(), compressed.data(), decompressed.size(), decompressed.data());

  ASSERT_EQ(data, decompressed);
  ASSERT_EQ(data.size(), actual_decompressed_size);
}

}  // namespace

TEST(TestIclCodec, IgzipCodecTest) {
  int sizes[] = {0, 10000, 100000};
  auto codec = IclCompressionCodec::MakeIclCompressionCodec("igzip", 2);
  for (int data_size : sizes) {
    std::vector<uint8_t> data = MakeRandomData(data_size);
    CheckCodecRoundtrip(codec, data);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    std::cout << e.what();
  }

  return err;
}
