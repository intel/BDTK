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

#include "TpchQ6Task.h"
#include "cider/batch/CiderBatchUtils.h"
// #include "velox/common/time/CpuWallTimer.h"
// #include "velox/common/time/Timer.h"
#include <ctime>
#include "velox/vector/arrow/Bridge.h"

class TpchQ6DummyTaskTest : public testing::Test {};

TEST_F(TpchQ6DummyTaskTest, test) {
  // CpuWallTiming t;
  // {
  //   CpuWallTimer timer(t);
  auto task = trino::velox::TpchQ6Task::Make();
  while (!task->isFinished()) {
    ArrowArray* output_array = CiderBatchUtils::allocateArrowArray();
    ArrowSchema* output_schema = CiderBatchUtils::allocateArrowSchema();
    time_t start = time(nullptr);
    task->nextBatch(output_schema, output_array);
    time_t stop = time(nullptr);
    std::cout << start << "    " << stop << std::endl;
  }
  //   std::cout << t.toString() << std::endl;
  //   t.clear();
  // }
}
