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
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include "exec/plan/validator/CiderPlanValidator.h"
#include "util/Logger.h"

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/../substrait_plan_files/";
}

std::string get_json_data(std::string file_name) {
  std::ifstream sub_json(getDataFilesPath() + file_name);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  return buffer.str();
}

TEST(CiderPlanValidator, InvalidAggAndJoinTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(
      get_json_data("cider_plan_validator_join.json"), sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg(invalid phase) <- proj <- filter <- proj <- join(type not supported) <-project
  // <-read
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 3);
}

TEST(CiderPlanValidator, InvalidAggTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(get_json_data("cider_pv_invalid_agg.json"),
                                              sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg(invalid phase) <- proj <- filter <- proj <- join <- project <- read
  // <-read
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 6);
}

TEST(CiderPlanValidator, InvalidJoinTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(get_json_data("cider_pv_invalid_join.json"),
                                              sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg <- proj <- filter <- proj <- join(invalid type) <- project <- read
  // <-read
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 4);
}

TEST(CiderPlanValidator, InvalidReadTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(get_json_data("cider_pv_invalid_read.json"),
                                              sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg <- proj <- filter <- proj <- join <- project <- read (invalid type)
  // <-read
  // read has a decimal type which will fail following nodes except Agg
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 1);
}

TEST(CiderPlanValidator, MultiJoinTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(get_json_data("cider_pv_multi_join.json"),
                                              sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg <- proj <- filter <- proj <- join <- project <- join <- read
  // <-read
  // read has a decimal type which will fail following nodes except Agg
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 6);
}

TEST(CiderPlanValidator, UnsupportedFunctionTest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(
      get_json_data("cider_pv_unsupported_function.json"), sub_plan);
  auto plan_slices = validator::CiderPlanValidator::getCiderSupportedSlice(
      *sub_plan, PlatformType::PrestoPlatform);
  // Agg <- proj <- filter <- proj <- join(unsupported function add_ov) <- project <- read
  // <-read
  CHECK_EQ(plan_slices[0].rel_nodes.size(), 4);
}

TEST(CiderPlanValidator, ValidateAPITest) {
  google::protobuf::Arena arena;
  substrait::Plan* sub_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  google::protobuf::util::JsonStringToMessage(
      get_json_data("cider_pv_unsupported_function.json"), sub_plan);
  // Agg <- proj <- filter <- proj <- join(unsupported function add_ov) <- project <- read
  // <-read
  CHECK(
      !validator::CiderPlanValidator::validate(*sub_plan, PlatformType::PrestoPlatform));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
