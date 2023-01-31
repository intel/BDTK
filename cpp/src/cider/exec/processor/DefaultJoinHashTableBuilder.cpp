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

#include "DefaultJoinHashTableBuilder.h"

namespace cider::exec::processor {

// TODO: pass some arguments that will decide hashtable type
std::unique_ptr<JoinHashTable> DefaultJoinHashTableBuilder::build() {
  // TODO: get the choosed hashtable type
  // reset to other hashtable if changed, comment out due to only have one hashtable now
  // hashTable_.reset(new JoinHashTable());
  return std::move(hashTable_);
}
// TODO: get the join key. Right use hard-code col 0
void DefaultJoinHashTableBuilder::appendBatch(
    std::shared_ptr<cider::exec::nextgen::context::Batch> batch) {
  int length = batch->getArray()->children[0]->length;
  for (int i = 0; i < length; i++) {
    int key = *((reinterpret_cast<int*>(
                    const_cast<void*>(batch->getArray()->children[0]->buffers[1]))) +
                i);

    hashTable_->getHashTable()->emplace(key, {batch.get(), i});
  }
}

std::shared_ptr<JoinHashTableBuilder> makeJoinHashTableBuilder(
    const ::substrait::JoinRel& joinRel,
    const std::shared_ptr<JoinHashTableBuildContext>& context) {
  return std::make_shared<DefaultJoinHashTableBuilder>(joinRel, context);
}

}  // namespace cider::exec::processor
