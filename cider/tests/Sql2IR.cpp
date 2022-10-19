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

#include <cstddef>
#include <iostream>

#include <google/protobuf/util/json_util.h>
#include <boost/program_options.hpp>

#include "cider/CiderAllocator.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderOptions.h"
#include "cider/CiderTableSchema.h"
#include "cider/batch/CiderBatch.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "util/Logger.h"
#include "utils/Utils.h"

const substrait::Rel* findJoinRightNode(const substrait::Rel& rel_node) {
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kJoin:
      return &rel_node.join().right();
    case substrait::Rel::RelTypeCase::kFilter:
      return findJoinRightNode(rel_node.filter().input());
    case substrait::Rel::RelTypeCase::kProject:
      return findJoinRightNode(rel_node.project().input());
    case substrait::Rel::RelTypeCase::kAggregate:
      return findJoinRightNode(rel_node.aggregate().input());
    default:
      return nullptr;
  }
}

const substrait::ReadRel* findReadNode(const substrait::Rel& rel_node) {
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kRead:
      return &rel_node.read();
    case substrait::Rel::RelTypeCase::kFilter:
      return findReadNode(rel_node.filter().input());
    case substrait::Rel::RelTypeCase::kProject:
      return findReadNode(rel_node.project().input());
    case substrait::Rel::RelTypeCase::kAggregate:
      return findReadNode(rel_node.aggregate().input());
    default:
      return nullptr;
  }
}

void feedBuildTable(substrait::Plan& plan,
                    std::shared_ptr<CiderCompileModule> cider_compile_module) {
  substrait::Rel root = plan.relations(0).root().input();
  const substrait::Rel* right_node = findJoinRightNode(root);
  if (right_node == nullptr) {
    return;
  }
  const substrait::ReadRel* read_node = findReadNode(*right_node);
  auto& schema = read_node->base_schema();
  size_t count = schema.names_size();

  std::vector<std::string> col_names;
  std::vector<::substrait::Type> col_types;
  for (int i = 0; i < count; ++i) {
    col_names.emplace_back(schema.names(i));
    col_types.emplace_back(schema.struct_().types(i));
  }

  auto table_schema = std::make_shared<CiderTableSchema>(col_names, col_types);

  cider_compile_module->feedBuildTable(CiderBatch(1, table_schema));
}

int main(int argc, char** argv) {
  namespace po = boost::program_options;

  // set option
  std::string sql, create_ddl;
  bool dump_plan = false;
  uint32_t dump_ir_level = 1;
  bool gen_cfg = false;  // need dump_ir_level == 2
  po::options_description options("Allowed Options");
  options.add_options()("sql", po::value<std::string>(&sql)->default_value(sql), "sql");
  options.add_options()("create-ddl",
                        po::value<std::string>(&create_ddl)->default_value(create_ddl),
                        "table create ddl");
  options.add_options()("dump-plan",
                        po::value<bool>(&dump_plan)->default_value(dump_plan),
                        "dump substait plan");
  options.add_options()("dump-ir-level",
                        po::value<uint32_t>(&dump_ir_level)->default_value(dump_ir_level),
                        "dump ir level. 1: func, 2: module");
  options.add_options()("gen-cfg",
                        po::value<bool>(&gen_cfg)->default_value(gen_cfg),
                        "generate module cfg");

  // parse option
  po::variables_map vm;
  po::store(
      po::command_line_parser(argc, argv).options(options).allow_unregistered().run(),
      vm);
  po::notify(vm);

  // output substrait plan
  std::string json = RunIsthmus::processSql(sql, create_ddl);
  if (dump_plan) {
    std::ofstream substrait_plan_file("./sql_2_ir_substrait_plan.json");
    substrait_plan_file << json;
    substrait_plan_file.close();
  }

  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);

  auto cider_compile_module =
      CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());

  // build table
  feedBuildTable(plan, cider_compile_module);

  // compile
  auto cco = CiderCompilationOption::defaults();
  auto ceo = CiderExecutionOption::defaults();
  ceo.just_explain = dump_ir_level;
  auto res = cider_compile_module->compile(plan, cco, ceo);

  // output IR
  auto llvm_ir = res->getIR();
  std::ofstream llvm_ir_file("./sql_2_ir_module.ll");
  llvm_ir_file << llvm_ir;
  llvm_ir_file.close();

  if (dump_ir_level == 2 && gen_cfg) {
    const char* cmd = "opt -dot-cfg -dot-callgraph -disable-output sql_2_ir_module.ll";
    system(cmd);
  }

  return 0;
}
