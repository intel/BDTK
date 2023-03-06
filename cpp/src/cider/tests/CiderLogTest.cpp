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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "cider/CiderException.h"
#include "util/Logger.h"

class CiderLogTest : public ::testing::Test {};

TEST_F(CiderLogTest, log) {
  LOG(INFO) << "INFO log";
  LOG(WARNING) << "WARNING log";
  LOG(ERROR) << "ERROR log";
  // EXPECT_THROW({ LOG(FATAL) << "FATAL log"; }, CheckFatalException);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  try {
    RUN_ALL_TESTS();
  } catch(const CheckFatalException& e) {
    std::cout << "CheckFatalException exception" << std::endl;
  } catch (...) {
    std::cout << "unknown exception" << std::endl;
  }
  return 0;
}
