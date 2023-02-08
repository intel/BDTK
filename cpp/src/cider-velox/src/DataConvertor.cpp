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
#include "DataConvertor.h"

#include "ArrowDataConvertor.h"
#include "RawDataConvertor.h"

namespace facebook::velox::plugin {

std::shared_ptr<DataConvertor> DataConvertor::create(CONVERT_TYPE type) {
  std::shared_ptr<DataConvertor> convertor;
  switch (type) {
    case CONVERT_TYPE::DIRECT:
      convertor = std::make_shared<RawDataConvertor>();
      return convertor;
    case CONVERT_TYPE::ARROW:
      convertor = std::make_shared<ArrowDataConvertor>();
      return convertor;
    default:
      VELOX_USER_FAIL("invalid input");
  }
}

}  // namespace facebook::velox::plugin
