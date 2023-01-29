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

#ifndef CIDER_TESTS_UTILS_ARROW_CHECKER_H_
#define CIDER_TESTS_UTILS_ARROW_CHECKER_H_

#include "exec/module/batch/ArrowABI.h"
#include "tests/utils/CiderArrowStringifier.h"

namespace cider::test::util {

class CiderArrowChecker {
 public:
  static bool checkArrowEq(const struct ArrowArray* expect_array,
                           const struct ArrowArray* actual_array,
                           const struct ArrowSchema* expect_schema,
                           const struct ArrowSchema* actual_schema);

  static bool checkArrowEqIgnoreOrder(const struct ArrowArray* expect_array,
                                      const struct ArrowArray* actual_array,
                                      const struct ArrowSchema* expect_schema,
                                      const struct ArrowSchema* actual_schema);

 private:
  static bool compareRowVectors(std::vector<ConcatenatedRow>& expected_row_vector,
                                std::vector<ConcatenatedRow>& actual_row_vector,
                                bool ignore_order = true);

  static std::vector<ConcatenatedRow> toConcatenatedRowVector(
      const struct ArrowArray* array,
      const struct ArrowSchema* schema);
};

}  // namespace cider::test::util

#endif  // CIDER_TESTS_UTILS_ARROW_CHECKER_H_
