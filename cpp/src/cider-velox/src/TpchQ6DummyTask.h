/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "TpchQ6Task.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
namespace trino::velox {
class TpchQ6DummyTask : public TpchQ6Task {
 public:
  explicit TpchQ6DummyTask();

  void nextBatch(ArrowSchema* c_schema, ArrowArray* c_array) override;

  bool isFinished() override;

  void close() override;

 private:
  std::shared_ptr<exec::Task> task_;
  bool is_finished_ = false;
};

}  // namespace trino::velox