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

#include <string>

#include "cider/CiderTypes.h"
#include "util/DateTimeParser.h"

int64_t CiderDateType::toInt64(const std::string& string_val) {
  DateTimeParser parser;
  parser.setFormatType(DateTimeParser::FormatType::Date);
  auto res = parser.parse(string_val, 0);
  if (res.has_value()) {
    return res.value();
  }
  throw std::runtime_error("Not a valid date string!");
}

std::string CiderByteArray::toString(const CiderByteArray& ciderByteArray) {
  if (ciderByteArray.len == 0 || ciderByteArray.ptr == nullptr) {
    return "";
  }
  std::string copy((char*)ciderByteArray.ptr, ciderByteArray.len);
  return copy;
}
