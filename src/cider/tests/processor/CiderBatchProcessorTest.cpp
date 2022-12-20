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
#include <string>

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
