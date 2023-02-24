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
#include <string>
#include "util/Logger.h"

#include "exec/plan/parser/TypeUtils.h"
#include "exec/processor/StatefulProcessor.h"
#include "exec/processor/StatelessProcessor.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::processor;

namespace {

std::shared_ptr<BatchProcessor> createBatchProcessorFromSql(const std::string& sql,
                                                            const std::string& ddl) {
  std::string json = RunIsthmus::processSql(sql, ddl);
  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);
  auto allocator = std::make_shared<CiderDefaultAllocator>();
  auto context = std::make_shared<BatchProcessorContext>(allocator);
  auto processor = makeBatchProcessor(plan, context);
  return processor;
}

}  // namespace

TEST(CiderBatchProcessorTest, statelessProcessorCompileTest) {
  std::string ddl = R"(
        CREATE TABLE test(col_1 BIGINT NOT NULL, col_2 BIGINT NOT NULL, col_3 BIGINT NOT NULL);
        )";
  std::string sql = "SELECT col_1 + col_2 FROM test WHERE col_1 <= col_2";
  auto processor = createBatchProcessorFromSql(sql, ddl);
  std::cout << "Processor type:" << processor->getProcessorType() << std::endl;
  EXPECT_EQ(processor->getProcessorType(), BatchProcessor::Type::kStateless);
}

TEST(CiderBatchProcessorTest, statelessProcessorProcessNextBatchTest) {
  std::string ddl = R"(
        CREATE TABLE test(col_1 BIGINT NOT NULL, col_2 BIGINT NOT NULL, col_3 BIGINT NOT NULL);
        )";
  std::string sql = "SELECT col_1 + col_2 FROM test WHERE col_1 <= col_2";
  auto processor = createBatchProcessorFromSql(sql, ddl);
  EXPECT_EQ(processor->getProcessorType(), BatchProcessor::Type::kStateless);

  struct ArrowArray* input_array;
  struct ArrowSchema* input_schema;
  QueryArrowDataGenerator::generateBatchByTypes(input_schema,
                                                input_array,
                                                99,
                                                {"col_1", "col_2", "col_3"},
                                                {CREATE_SUBSTRAIT_TYPE(I64),
                                                 CREATE_SUBSTRAIT_TYPE(I64),
                                                 CREATE_SUBSTRAIT_TYPE(I64)});

  processor->processNextBatch(input_array, input_schema);

  struct ArrowArray output_array;
  struct ArrowSchema output_schema;
  processor->getResult(output_array, output_schema);

  std::cout << "Query result has " << output_array.length << " rows" << std::endl;
}

TEST(CiderBatchProcessorTest, statefulProcessorProcessNextBatchTest) {
  std::string ddl = R"(
        CREATE TABLE test(col_1 BIGINT, col_2 INT);
        )";
  std::string sql = "SELECT sum(col_1), sum(col_2) FROM test";

  auto input_builder = ArrowArrayBuilder();
  auto&& [input_schema, input_array] =
      input_builder.setRowNum(10)
          .addColumn<int64_t>(
              "col_1",
              CREATE_SUBSTRAIT_TYPE(I64),
              {1, 2, 3, 1, 2, 4, 1, 2, 3, 4},
              {true, false, false, false, false, false, false, false, false, false})
          .addColumn<int32_t>("col_2",
                              CREATE_SUBSTRAIT_TYPE(I32),
                              {1, 11, 111, 2, 22, 222, 3, 33, 333, 555})
          .build();

  auto processor = createBatchProcessorFromSql(sql, ddl);
  EXPECT_EQ(processor->getProcessorType(), BatchProcessor::Type::kStateful);
  input_array->release = nullptr;
  input_schema->release = nullptr;
  processor->processNextBatch(input_array, input_schema);
  processor->processNextBatch(input_array, input_schema);

  processor->finish();

  struct ArrowArray output_array;
  struct ArrowSchema output_schema;
  processor->getResult(output_array, output_schema);

  EXPECT_EQ(output_array.length, 1);
  EXPECT_EQ(output_array.n_children, 2);
  EXPECT_EQ(*(int64_t*)(output_array.children[0]->buffers[1]), 22 * 2);
  EXPECT_EQ(*(int32_t*)(output_array.children[1]->buffers[1]), 1293 * 2);
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
