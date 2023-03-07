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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "cider/processor/JoinHashTableBuilder.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/plan/parser/TypeUtils.h"
#include "exec/processor/StatefulProcessor.h"
#include "exec/processor/StatelessProcessor.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::processor;

// Hashjoin integrate with BatchProcessor test
namespace {

std::pair<std::unique_ptr<BatchProcessor>, std::shared_ptr<JoinHashTableBuilder>>
createBatchProcessorFromSql(const std::string& sql, const std::string& ddl) {
  std::string json = RunIsthmus::processSql(sql, ddl);
  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);
  auto allocator = std::make_shared<CiderDefaultAllocator>();
  auto context = std::make_shared<BatchProcessorContext>(allocator);
  auto processor = cider::exec::processor::BatchProcessor::Make(plan, context);
  auto table_build_context = std::make_shared<JoinHashTableBuildContext>(allocator);
  auto join_hash_builder = makeJoinHashTableBuilder(
      plan.relations(0).root().input().join(), table_build_context);

  return std::make_pair(std::move(processor), join_hash_builder);
}

}  // namespace

TEST(HashJoinBatchProcessorTest, statelessProcessorProcessNextBatchTest) {
  std::string ddl =
      "CREATE TABLE table_probe(l_a BIGINT NOT NULL, l_b BIGINT NOT NULL, l_c BIGINT "
      "NOT NULL);"
      "CREATE TABLE table_build(r_a BIGINT NOT NULL, r_b BIGINT NOT NULL, r_c BIGINT "
      "NOT NULL);";
  std::string sql =
      "select * from table_probe join table_build on table_probe.l_a = "
      "table_build.r_a";
  auto [processor, join_hash_builder] = createBatchProcessorFromSql(sql, ddl);
  EXPECT_EQ(processor->getProcessorType(), BatchProcessor::Type::kStateless);

  auto build_table = ArrowArrayBuilder();
  auto&& [build_schema, build_array] =
      build_table.setRowNum(4)
          .addColumn<int64_t>("r_a",
                              CREATE_SUBSTRAIT_TYPE(I64),
                              {1, 2, 3, 4},
                              {false, false, false, false})
          .template addColumn<int64_t>("r_b",
                                       CREATE_SUBSTRAIT_TYPE(I64),
                                       {2, 3, 4, 5},
                                       {false, false, false, false})
          .template addColumn<int64_t>(
              "r_c", CREATE_SUBSTRAIT_TYPE(I64), {222, 333, 444, 555})
          .build();

  Analyzer::context::Batch build_batch(*build_schema, *build_array);
  std::vector<std::vector<int64_t>> expected_res = {{1, 2, 3, 4},
                                                    {2, 3, 5, 6},
                                                    {222, 333, 555, 666},
                                                    {1, 2, 3, 4},
                                                    {2, 3, 4, 5},
                                                    {222, 333, 444, 555}};

  join_hash_builder->appendBatch(std::make_shared<Analyzer::context::Batch>(build_batch));
  auto hm = join_hash_builder->build();
  processor->feedHashBuildTable(std::move(hm));

  auto input_builder = ArrowArrayBuilder();
  auto&& [input_schema, input_array] =
      input_builder.setRowNum(6)
          .addColumn<int64_t>("l_a",
                              CREATE_SUBSTRAIT_TYPE(I64),
                              {1, 2, 6, 3, 4, 5},
                              {false, false, false, false, false, false})
          .template addColumn<int64_t>("l_b",
                                       CREATE_SUBSTRAIT_TYPE(I64),
                                       {2, 3, 4, 5, 6, 7},
                                       {false, false, false, false, false, false})
          .template addColumn<int64_t>(
              "l_c", CREATE_SUBSTRAIT_TYPE(I64), {222, 333, 444, 555, 666, 777})
          .build();

  processor->processNextBatch(input_array, input_schema);

  struct ArrowArray output_array;
  struct ArrowSchema output_schema;
  processor->getResult(output_array, output_schema);

  size_t expected_row_len = expected_res[0].size();

  EXPECT_EQ(output_array.length, expected_row_len);
  auto check_array = [expected_row_len](ArrowArray* array,
                                        const std::vector<int64_t>& expected_cols) {
    EXPECT_EQ(array->length, expected_row_len);
    int64_t* data_buffer = (int64_t*)array->buffers[1];
    for (size_t i = 0; i < expected_row_len; ++i) {
      EXPECT_EQ(data_buffer[i], expected_cols[i]);
    }
  };

  for (size_t i = 0; i < expected_res.size(); ++i) {
    check_array(output_array.children[i], expected_res[i]);
  }

  std::cout << "Query result has " << output_array.length << " rows" << std::endl;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
