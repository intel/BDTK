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
#ifndef NEXTGEN_UTILS_TYPEUTILS_H
#define NEXTGEN_UTILS_TYPEUTILS_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "type/plan/SqlTypes.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::utils {
inline jitlib::JITTypeTag getJITTypeTag(const SQLTypes& st) {
  switch (st) {
    case kBOOLEAN:
      return jitlib::JITTypeTag::BOOL;
    case kTINYINT:
      return jitlib::JITTypeTag::INT8;
    case kSMALLINT:
      return jitlib::JITTypeTag::INT16;
    case kINT:
      return jitlib::JITTypeTag::INT32;
    case kBIGINT:
      return jitlib::JITTypeTag::INT64;
    case kFLOAT:
      return jitlib::JITTypeTag::FLOAT;
    case kDOUBLE:
      return jitlib::JITTypeTag::DOUBLE;
    case kVARCHAR:
    case kCHAR:
    case kTEXT:
      return jitlib::JITTypeTag::VARCHAR;
    case kDATE:
      return jitlib::JITTypeTag::INT32;
    case kTIME:
    case kINTERVAL_YEAR_MONTH:
    case kINTERVAL_DAY_TIME:
    case kTIMESTAMP:
      return jitlib::JITTypeTag::INT64;
    case kDECIMAL:
      return jitlib::JITTypeTag::INT128;
    case kNULLT:
    default:
      return jitlib::JITTypeTag::INVALID;
  }
  UNREACHABLE();
}

inline int64_t getBufferNum(SQLTypes type) {
  switch (type) {
    case kBOOLEAN:
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
    case kFLOAT:
    case kDOUBLE:
    case kDECIMAL:
    case kARRAY:
      return 2;
    case kVARCHAR:
    case kCHAR:
    case kTEXT:
      return 3;
    case kSTRUCT:
      return 1;
    default:
      UNIMPLEMENTED();
  }
  UNREACHABLE();
  return -1;
}

inline int64_t getTypeBytes(SQLTypes type) {
  switch (type) {
    case kBOOLEAN:
    case kTINYINT:
      return 1;
    case kSMALLINT:
      return 2;
    case kINT:
    case kFLOAT:
    case kDATE:
      return 4;
    case kBIGINT:
    case kDOUBLE:
    case kTIMESTAMP:
    case kTIME:
    case kINTERVAL_YEAR_MONTH:
    case kINTERVAL_DAY_TIME:
      return 8;
    case kDECIMAL:
      return 16;
    default:
      UNIMPLEMENTED();
  }
  UNREACHABLE();
  return -1;
}

inline std::string getSQLTypeName(SQLTypes type) {
  switch (type) {
    case kNULLT:
      return "null";
    case kTINYINT:
      return "int8";
    case kSMALLINT:
      return "int16";
    case kINT:
      return "int32";
    case kBIGINT:
      return "int64";
    case kFLOAT:
      return "float";
    case kDOUBLE:
      return "double";
    case kDATE:
      return "date";
    case kDECIMAL:
      return "decimal";
    case kBOOLEAN:
      return "bool";
    default:
      UNIMPLEMENTED();
  }
  UNREACHABLE();
  return "invalid";
}

inline bool isVectorizableType(SQLTypes type) {
  switch (type) {
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
    case kFLOAT:
    case kDOUBLE:
      return true;
    default:
      return false;
  }
}

}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_TYPEUTILS_H
