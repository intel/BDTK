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
#include "TestHelpers.h"
#include "cider/CiderBatch.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"
#include "cider/CiderTableSchema.h"
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/template/AggregatedColRange.h"
#include "exec/template/Execute.h"
#include "exec/template/InputMetadata.h"
#include "util/Logger.h"

namespace bf = boost::filesystem;

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/substrait_plan_files/";
}

std::vector<InputTableInfo> buildInputTableInfo(
    std::vector<CiderTableSchema> table_schemas) {
  std::vector<InputTableInfo> query_infos;
  for (int i = 0; i < table_schemas.size(); i++) {
    Fragmenter_Namespace::FragmentInfo fi_0;
    fi_0.fragmentId = i;
    fi_0.shadowNumTuples = 1024;
    fi_0.physicalTableId = 100 + i;  // just for test usage here.
    fi_0.setPhysicalNumTuples(1024);
    // add chunkMetadata
    for (int j = 0; j < table_schemas[i].getColumnTypes().size(); j++) {
      auto chunk_meta = std::make_shared<ChunkMetadata>();
      chunk_meta->numBytes = 0;
      chunk_meta->numElements = 0;
      fi_0.setChunkMetadata(j, chunk_meta);
    }
    Fragmenter_Namespace::TableInfo ti_0;
    ti_0.fragments = {fi_0};
    ti_0.setPhysicalNumTuples(1024);
    InputTableInfo iti_0{100, 100 + i, ti_0};
    query_infos.push_back(iti_0);
  }
  return query_infos;
}

CiderBatch buildOrdersCiderBatch() {
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

CiderBatch buildLineitemCiderBatch(int row_num) {
  const int col_num = 16;

  // L_ORDERKEY BIGINT NOT NULL, L_PARTKEY BIGINT NOT NULL, L_SUPPKEY BIGINT NOT NULL,
  // L_LINENUMBER INTEGER, L_QUANTITY DECIMAL, L_EXTENDEDPRICE DECIMAL, L_DISCOUNT
  // DECIMAL, L_TAX DECIMAL, L_RETURNFLAG CHAR(1), L_LINESTATUS CHAR(1), L_SHIPDATE DATE,
  // L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25), L_SHIPMODE CHAR(10),
  // L_COMMENT VARCHAR(44))"

  std::vector<int8_t*> table_ptr_tmp(col_num, nullptr);

  int64_t* orderkey_buf = new int64_t[row_num];
  int32_t* linenumber_buf = new int32_t[row_num];

  for (int i = 0; i < row_num; i++) {
    orderkey_buf[i] = i;
    linenumber_buf[i] = i;
  }

  table_ptr_tmp[0] = (int8_t*)orderkey_buf;
  table_ptr_tmp[3] = (int8_t*)linenumber_buf;

  std::vector<const int8_t*> table_ptr;
  for (auto column_ptr : table_ptr_tmp) {
    table_ptr.push_back(static_cast<const int8_t*>(column_ptr));
  }

  CiderBatch lineitem_table(row_num, table_ptr);
  return lineitem_table;
}

::substrait::Plan generateSubstraitPlanByFile(std::string file_name) {
  std::ifstream sub_json(getDataFilesPath() + file_name);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  std::string sub_data = buffer.str();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(sub_data, &sub_plan);
  return sub_plan;
}

void RunOneBatchAgg(std::string file_name) {
  ::substrait::Plan sub_plan = generateSubstraitPlanByFile(file_name);
  generator::SubstraitToRelAlgExecutionUnit eu_translator(sub_plan);
  eu_translator.createRelAlgExecutionUnit();

  auto cider_compile_module = CiderCompileModule::Make();

  cider_compile_module->feedBuildTable(std::move(buildOrdersCiderBatch()));

  auto compile_res = cider_compile_module->compile(sub_plan);

  auto cider_runtime_module = std::make_shared<CiderRuntimeModule>(compile_res);

  const int row_num = 20;
  const CiderBatch inBatch = buildLineitemCiderBatch(row_num);

  cider_runtime_module->processNextBatch(inBatch);
  auto [_, outBatch] = cider_runtime_module->fetchResults();

  auto res_ptr = outBatch->column(0);
  int64_t res = reinterpret_cast<const int64_t*>(res_ptr)[0];
  // all rows could join, so the result should be (0+19)*20/2=190
  std::cout << "final agg result is " << res << ", total matched row "
            << outBatch->row_num() << std::endl;
  CHECK_EQ(res, 190);
}

TEST(CiderRuntimeJoinTest, basic_join_test) {
  // SELECT SUM(l_linenumber) FROM lineitem, orders WHERE o_orderkey = l_orderkey
  RunOneBatchAgg("join_sum.json");

  // SELECT SUM(l_linenumber) FROM lineitem JOIN orders ON l_orderkey = o_orderkey
  RunOneBatchAgg("join_sum2.json");

  // SELECT SUM(l_linenumber), SUM(O_CUSTKEY) FROM lineitem JOIN orders ON l_orderkey =
  // o_orderkey
  RunOneBatchAgg("join_sum3.json");
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
