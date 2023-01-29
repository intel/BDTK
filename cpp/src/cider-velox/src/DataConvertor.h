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
#pragma once

#include <cfloat>
#include <cstring>
#include <iostream>
#include <tuple>
#include <vector>

#include "CiderNullValues.h"
#include "cider/CiderInterface.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::plugin {

enum CONVERT_TYPE { ARROW, DIRECT };

enum CIDER_DIMEN { SECOND = 0, MILLISECOND = 3, MICROSECOND = 6, NANOSECOND = 9 };

struct CiderResultSet {
  CiderResultSet(int8_t** col_buffer, int num_rows)
      : colBuffer(col_buffer), numRows(num_rows) {}

  int8_t** colBuffer;
  int numRows;
};

class DataConvertor {
 public:
  DataConvertor() {}

  static std::shared_ptr<DataConvertor> create(CONVERT_TYPE type);

  virtual CiderBatch convertToCider(RowVectorPtr input,
                                    int num_rows,
                                    std::chrono::microseconds* timer,
                                    memory::MemoryPool* pool) = 0;

  virtual RowVectorPtr convertToRowVector(const CiderBatch& input,
                                          const CiderTableSchema& schema,
                                          memory::MemoryPool* pool) = 0;
};

}  // namespace facebook::velox::plugin
