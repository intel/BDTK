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

#include "DdlUtils.h"

#include <boost/program_options.hpp>

#include "rapidjson/document.h"
#include "type/data/sqltypes.h"

namespace ddl_utils {
SqlType::SqlType(SQLTypes type, int param1, int param2, bool is_array, int array_size)
    : type(type)
    , param1(param1)
    , param2(param2)
    , is_array(is_array)
    , array_size(array_size) {}

SQLTypes SqlType::get_type() const {
  return type;
}

int SqlType::get_param1() const {
  return param1;
}

void SqlType::set_param1(int param) {
  param1 = param;
}

int SqlType::get_param2() const {
  return param2;
}

bool SqlType::get_is_array() const {
  return is_array;
}

void SqlType::set_is_array(bool a) {
  is_array = a;
}

int SqlType::get_array_size() const {
  return array_size;
}

void SqlType::set_array_size(int s) {
  array_size = s;
}

std::string SqlType::to_string() const {
  std::string str;
  switch (type) {
    case kBOOLEAN:
      str = "BOOLEAN";
      break;
    case kCHAR:
      str = "CHAR(" + boost::lexical_cast<std::string>(param1) + ")";
      break;
    case kVARCHAR:
      str = "VARCHAR(" + boost::lexical_cast<std::string>(param1) + ")";
      break;
    case kTEXT:
      str = "TEXT";
      break;
    case kNUMERIC:
      str = "NUMERIC(" + boost::lexical_cast<std::string>(param1);
      if (param2 > 0) {
        str += ", " + boost::lexical_cast<std::string>(param2);
      }
      str += ")";
      break;
    case kDECIMAL:
      str = "DECIMAL(" + boost::lexical_cast<std::string>(param1);
      if (param2 > 0) {
        str += ", " + boost::lexical_cast<std::string>(param2);
      }
      str += ")";
      break;
    case kBIGINT:
      str = "BIGINT";
      break;
    case kINT:
      str = "INT";
      break;
    case kTINYINT:
      str = "TINYINT";
      break;
    case kSMALLINT:
      str = "SMALLINT";
      break;
    case kFLOAT:
      str = "FLOAT";
      break;
    case kDOUBLE:
      str = "DOUBLE";
      break;
    case kTIME:
      str = "TIME";
      if (param1 < 6) {
        str += "(" + boost::lexical_cast<std::string>(param1) + ")";
      }
      break;
    case kTIMESTAMP:
      str = "TIMESTAMP";
      if (param1 <= 9) {
        str += "(" + boost::lexical_cast<std::string>(param1) + ")";
      }
      break;
    case kDATE:
      str = "DATE";
      break;
    default:
      assert(false);
      break;
  }
  if (is_array) {
    str += "[";
    if (array_size > 0) {
      str += boost::lexical_cast<std::string>(array_size);
    }
    str += "]";
  }
  return str;
}

void SqlType::check_type() {
  switch (type) {
    case kCHAR:
    case kVARCHAR:
      if (param1 <= 0) {
        throw std::runtime_error("CHAR and VARCHAR must have a positive dimension.");
      }
      break;
    case kDECIMAL:
    case kNUMERIC:
      if (param1 <= 0) {
        throw std::runtime_error("DECIMAL and NUMERIC must have a positive precision.");
      } else if (param1 > 19) {
        throw std::runtime_error(
            "DECIMAL and NUMERIC precision cannot be larger than 19.");
      } else if (param1 <= param2) {
        throw std::runtime_error(
            "DECIMAL and NUMERIC must have precision larger than scale.");
      }
      break;
    case kTIMESTAMP:
      if (param1 == -1) {
        param1 = 0;  // set default to 0
      } else if (param1 != 0 && param1 != 3 && param1 != 6 &&
                 param1 != 9) {  // support ms, us, ns
        throw std::runtime_error(
            "Only TIMESTAMP(n) where n = (0,3,6,9) are supported now.");
      }
      break;
    case kTIME:
      if (param1 == -1) {
        param1 = 0;  // default precision is 0
      }
      if (param1 > 0) {  // @TODO(wei) support sub-second precision later.
        throw std::runtime_error("Only TIME(0) is supported now.");
      }
      break;
    default:
      param1 = 0;
      break;
  }
}

Encoding::Encoding(std::string* encoding_name, int encoding_param)
    : encoding_name(encoding_name), encoding_param(encoding_param) {}

const std::string* Encoding::get_encoding_name() const {
  return encoding_name.get();
}

int Encoding::get_encoding_param() const {
  return encoding_param;
}

}  // namespace ddl_utils
