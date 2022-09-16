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

#ifndef CIDER_CIDERAGGTARGETCOLEXTRACTOR_H
#define CIDER_CIDERAGGTARGETCOLEXTRACTOR_H

#include <string>
#include <vector>
#include "cider/batch/CiderBatch.h"

class CiderAggTargetColExtractor {
 public:
  CiderAggTargetColExtractor(const std::string& name, size_t colIndex)
      : name_(name), col_index_(colIndex) {}

  virtual void extract(const std::vector<const int8_t*>&, int8_t*) = 0;
  virtual void extract(const std::vector<const int8_t*>&, CiderBatch*) = 0;

  std::string getName() { return name_; }

 protected:
  const std::string name_;
  const size_t col_index_;
  size_t null_offset_;
};

#endif
