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

#ifndef CIDER_TESTS_UTILS_NEXTGEN_BENCHMARK_RUNNER_H_
#define CIDER_TESTS_UTILS_NEXTGEN_BENCHMARK_RUNNER_H_

#include "CiderNextgenQueryRunner.h"

namespace cider::test::util {
class CiderNextgenBenchmarkRunner : public CiderNextgenQueryRunner {
 public:
  void runQueryOneBatch(const std::string& file_or_sql,
                        const struct ArrowArray& input_array,
                        const struct ArrowSchema& input_schema,
                        struct ArrowArray& output_array,
                        struct ArrowSchema& output_schema) override;
};
}  // namespace cider::test::util
#endif
