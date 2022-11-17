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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <string>
#include "exec/plan/validator/CiderPlanValidator.h"
#include "util/Logger.h"

namespace bf = boost::filesystem;

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/../substrait_plan_files/";
}

TEST(CiderPlanValidator, InvalidAggAndJoinTest) {
  std::ifstream sub_json(getDataFilesPath() + "cider_plan_validator_join.json");
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  std::string sub_data = buffer.str();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(sub_data, &sub_plan);
  auto plan_slice = validator::CiderPlanValidator::getCiderSupportedSlice(
      sub_plan, generator::FrontendEngine::VELOX);
  // Agg(invalid phase) <- proj <- filter <- proj <- join(type not supported) <-project
  // <-read
  CHECK_EQ(plan_slice.rel_nodes.size(), 3);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  log_options.max_files_ = 0;  // stderr only by default
  logger::init(log_options);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
