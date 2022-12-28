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

#include "TestHelpers.h"

#include <fstream>
#include "cider/CiderCompileModule.h"

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/substrait_plan_files/";
}

CiderBatch buildCiderBatch() {
  const int col_num = 9;
  const int row_num = 20;

  //"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE",
  //"O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"
  std::vector<int8_t*> table_ptr_tmp(col_num, nullptr);
  // int64_t O_Orderkey, int64_t O_CUSTKEY
  int64_t* orderKey_buf = new int64_t[row_num];
  int64_t* custKey_buf = new int64_t[row_num];
  for (int i = 0; i < row_num; i++) {
    orderKey_buf[i] = i;
    custKey_buf[i] = 100 + i;
  }
  table_ptr_tmp[0] = (int8_t*)orderKey_buf;
  table_ptr_tmp[1] = (int8_t*)custKey_buf;

  std::vector<const int8_t*> table_ptr;
  for (auto column_ptr : table_ptr_tmp) {
    table_ptr.push_back(static_cast<const int8_t*>(column_ptr));
  }

  CiderBatch orders_table(row_num, table_ptr);
  return orders_table;
}

TEST(CiderCompileModuleTest, FilterProject) {
  std::string file_name = "simple_project_filter.json";
  std::ifstream sub_json(getDataFilesPath() + file_name);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  std::string sub_data = buffer.str();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(sub_data, &sub_plan);
  auto ciderCompileModule =
      CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());
  ciderCompileModule->feedBuildTable(std::move(buildCiderBatch()));
  auto result = ciderCompileModule->compile(sub_plan);
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
