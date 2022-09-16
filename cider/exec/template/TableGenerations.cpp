/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include "TableGenerations.h"
#include "util/Logger.h"

void TableGenerations::setGeneration(const uint32_t id,
                                     const TableGeneration& generation) {
  const auto it_ok = id_to_generation_.emplace(id, generation);
  CHECK(it_ok.second);
}

const TableGeneration& TableGenerations::getGeneration(const uint32_t id) const {
  const auto it = id_to_generation_.find(id);
  CHECK(it != id_to_generation_.end());
  return it->second;
}

void TableGenerations::clear() {
  decltype(id_to_generation_)().swap(id_to_generation_);
}
