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

#include <gtest/gtest.h>
#include "TestHelpers.h"
#include "cider/CiderException.h"

class CiderLogTest : public ::testing::Test {};

TEST_F(CiderLogTest, log) {
  LOG(DEBUG4) << "DEBUG4 log";
  LOG(DEBUG3) << "DEBUG3 log";
  LOG(DEBUG2) << "DEBUG2 log";
  LOG(DEBUG1) << "DEBUG1 log";
  LOG(INFO) << "INFO log";
  LOG(WARNING) << "WARNING log";
  LOG(ERROR) << "ERROR log";
  LOG(FATAL) << "FATAL log";
}

/*
 * Run the CiderLogTest test file alone, for example: ./CiderLogTest
 * Or run CiderLogTest with parameters, for example: ./CiderLogTest --log-directory
 * bdtk_log --log-file-name bdtk_log
 */
int main(int argc, char** argv) {
  std::cout << "argc = " << argc << std::endl;
  for (size_t i = 0; i < argc; i++) {
    std::cout << "argc[" << i << "] = " << argv[i] << std::endl;
  }

  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  std::cout << log_options.full_log_dir() << std::endl;
  logger::init(log_options);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
