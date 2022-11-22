#ifndef NEXTGEN_UTILS_TYPEUTILS_H
#define NEXTGEN_UTILS_TYPEUTILS_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "type/data/sqltypes.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::utils {
inline jitlib::JITTypeTag getJITTypeTag(const SQLTypes& st) {
  switch (st) {
    case kBOOLEAN:
      return jitlib::JITTypeTag::BOOL;
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
      return jitlib::JITTypeTag::INT32;
    case kFLOAT:
      return jitlib::JITTypeTag::FLOAT;
    case kDOUBLE:
      return jitlib::JITTypeTag::DOUBLE;
    case kVARCHAR:
    case kCHAR:
    case kTEXT:
      UNIMPLEMENTED();
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
}

}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_TYPEUTILS_H
