/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

/**
 * @file		sqltypes.h
 * @brief		Constants for Builtin SQL Types supported by ModularSQL
 **/

#pragma once

#include "type/data/funcannotations.h"
#include "util/Datum.h"
#include "util/Logger.h"
#include "util/StringTransform.h"

#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

// must not change because these values persist in catalogs.
enum SQLTypes {
  kNULLT = 0,  // type for null values
  kBOOLEAN = 1,
  kCHAR = 2,
  kVARCHAR = 3,
  kNUMERIC = 4,
  kDECIMAL = 5,
  kINT = 6,
  kSMALLINT = 7,
  kFLOAT = 8,
  kDOUBLE = 9,
  kTIME = 10,
  kTIMESTAMP = 11,
  kBIGINT = 12,
  kTEXT = 13,
  kDATE = 14,
  kARRAY = 15,
  kINTERVAL_DAY_TIME = 16,
  kINTERVAL_YEAR_MONTH = 17,
  kTINYINT = 18,
  kEVAL_CONTEXT_TYPE = 24,  // Placeholder Type for ANY
  kVOID = 25,
  kCURSOR = 26,
  kCOLUMN = 27,
  kCOLUMN_LIST = 28,
  kSTRUCT = 29,
  kINT128 = 30,
  kINT256 = 31,
  kSQLTYPE_LAST
};

[[deprecated]] inline std::string toString(const SQLTypes& type) {
  switch (type) {
    case kNULLT:
      return "NULL";
    case kBOOLEAN:
      return "BOOL";
    case kCHAR:
      return "CHAR";
    case kVARCHAR:
      return "VARCHAR";
    case kNUMERIC:
      return "NUMERIC";
    case kDECIMAL:
      return "DECIMAL";
    case kINT:
      return "INT";
    case kSMALLINT:
      return "SMALLINT";
    case kFLOAT:
      return "FLOAT";
    case kDOUBLE:
      return "DOUBLE";
    case kTIME:
      return "TIME";
    case kTIMESTAMP:
      return "TIMESTAMP";
    case kBIGINT:
      return "BIGINT";
    case kTEXT:
      return "TEXT";
    case kDATE:
      return "DATE";
    case kARRAY:
      return "ARRAY";
    case kINTERVAL_DAY_TIME:
      return "DAY TIME INTERVAL";
    case kINTERVAL_YEAR_MONTH:
      return "YEAR MONTH INTERVAL";
    case kTINYINT:
      return "TINYINT";
    case kEVAL_CONTEXT_TYPE:
      return "UNEVALUATED ANY";
    case kVOID:
      return "VOID";
    case kCURSOR:
      return "CURSOR";
    case kCOLUMN:
      return "COLUMN";
    case kCOLUMN_LIST:
      return "COLUMN_LIST";
    case kSTRUCT:
      return "STRUCT";
    case kSQLTYPE_LAST:
      break;
  }
  LOG(ERROR) << "Invalid SQL type: " << type;
  return "";
}

inline std::ostream& operator<<(std::ostream& os, SQLTypes const sql_type) {
  os << toString(sql_type);
  return os;
}

struct DoNothingDeleter {
  void operator()(int8_t*) {}
};
struct FreeDeleter {
  void operator()(int8_t* p) { free(p); }
};

struct HostArrayDatum : public VarlenDatum {
  using ManagedPtr = std::shared_ptr<int8_t>;

  HostArrayDatum() = default;

  HostArrayDatum(size_t const l, ManagedPtr p, bool const n)
      : VarlenDatum(l, p.get(), n), data_ptr(p) {}

  HostArrayDatum(size_t const l, int8_t* p, bool const n)
      : VarlenDatum(l, p, n), data_ptr(p, FreeDeleter()) {}

  template <typename CUSTOM_DELETER,
            typename = std::enable_if_t<
                std::is_void<std::result_of_t<CUSTOM_DELETER(int8_t*)> >::value> >
  HostArrayDatum(size_t const l, int8_t* p, CUSTOM_DELETER custom_deleter)
      : VarlenDatum(l, p, 0 == l), data_ptr(p, custom_deleter) {}

  template <typename CUSTOM_DELETER,
            typename = std::enable_if_t<
                std::is_void<std::result_of_t<CUSTOM_DELETER(int8_t*)> >::value> >
  HostArrayDatum(size_t const l, int8_t* p, bool const n, CUSTOM_DELETER custom_deleter)
      : VarlenDatum(l, p, n), data_ptr(p, custom_deleter) {}

  ManagedPtr data_ptr;
};

struct DeviceArrayDatum : public VarlenDatum {
  DeviceArrayDatum() : VarlenDatum() {}
};

using ArrayDatum = std::conditional_t<false, DeviceArrayDatum, HostArrayDatum>;

union DataBlockPtr {
  int8_t* numbersPtr;
  std::vector<std::string>* stringsPtr;
  std::vector<ArrayDatum>* arraysPtr;
};

// must not change because these values persist in catalogs.
enum EncodingType {
  kENCODING_NONE = 0,          // no encoding
  kENCODING_FIXED = 1,         // Fixed-bit encoding
  kENCODING_RL = 2,            // Run Length encoding
  kENCODING_DIFF = 3,          // Differential encoding
  kENCODING_DICT = 4,          // Dictionary encoding
  kENCODING_SPARSE = 5,        // Null encoding for sparse columns
  kENCODING_DATE_IN_DAYS = 7,  // Date encoding in days
  kENCODING_LAST = 8
};

#define IS_INTEGER(T) \
  (((T) == kINT) || ((T) == kSMALLINT) || ((T) == kBIGINT) || ((T) == kTINYINT))
#define IS_NUMBER(T)                                                             \
  (((T) == kINT) || ((T) == kSMALLINT) || ((T) == kDOUBLE) || ((T) == kFLOAT) || \
   ((T) == kBIGINT) || ((T) == kNUMERIC) || ((T) == kDECIMAL) || ((T) == kTINYINT))
#define IS_STRING(T) (((T) == kTEXT) || ((T) == kVARCHAR) || ((T) == kCHAR))
#define IS_INTERVAL(T) ((T) == kINTERVAL_DAY_TIME || (T) == kINTERVAL_YEAR_MONTH)
#define IS_DECIMAL(T) ((T) == kNUMERIC || (T) == kDECIMAL)

#define INF_FLOAT HUGE_VALF
#define INF_DOUBLE HUGE_VAL
#define TRANSIENT_DICT_ID 0
#define TRANSIENT_DICT(ID) (-(ID))
#define REGULAR_DICT(TRANSIENTID) (-(TRANSIENTID))

constexpr auto is_datetime(SQLTypes type) {
  return type == kTIME || type == kTIMESTAMP || type == kDATE;
}

// @type SQLTypeInfo
// @brief a structure to capture all type information including
// length, precision, scale, etc.
class SQLTypeInfo {
 public:
  SQLTypeInfo(SQLTypes t, bool n, std::vector<SQLTypeInfo>&& c)
      : type(t)
      , subtype(kNULLT)
      , dimension(0)
      , scale(0)
      , notnull(n)
      , compression(kENCODING_NONE)
      , comp_param(0)
      , size(get_storage_size())
      , children(std::move(c)) {}
  SQLTypeInfo(SQLTypes t, bool n, const std::vector<SQLTypeInfo>& c)
      : type(t)
      , subtype(kNULLT)
      , dimension(0)
      , scale(0)
      , notnull(n)
      , compression(kENCODING_NONE)
      , comp_param(0)
      , size(get_storage_size())
      , children(c) {}
  SQLTypeInfo(SQLTypes t, int d, int s, bool n, EncodingType c, int p, SQLTypes st)
      : type(t)
      , subtype(st)
      , dimension(d)
      , scale(s)
      , notnull(n)
      , compression(c)
      , comp_param(p)
      , size(get_storage_size()) {}
  SQLTypeInfo(SQLTypes t, int d, int s, bool n)
      : type(t)
      , subtype(kNULLT)
      , dimension(d)
      , scale(s)
      , notnull(n)
      , compression(kENCODING_NONE)
      , comp_param(0)
      , size(get_storage_size()) {}
  SQLTypeInfo(SQLTypes t, EncodingType c, int p, SQLTypes st)
      : type(t)
      , subtype(st)
      , dimension(0)
      , scale(0)
      , notnull(false)
      , compression(c)
      , comp_param(p)
      , size(get_storage_size()) {}
  SQLTypeInfo(SQLTypes t, int d, int s) : SQLTypeInfo(t, d, s, false) {}
  SQLTypeInfo(SQLTypes t, bool n)
      : type(t)
      , subtype(kNULLT)
      , dimension(0)
      , scale(0)
      , notnull(n)
      , compression(kENCODING_NONE)
      , comp_param(0)
      , size(get_storage_size()) {}
  SQLTypeInfo(SQLTypes t) : SQLTypeInfo(t, false) {}  // NOLINT
  SQLTypeInfo(SQLTypes t, bool n, EncodingType c)
      : type(t)
      , subtype(kNULLT)
      , dimension(0)
      , scale(0)
      , notnull(n)
      , compression(c)
      , comp_param(0)
      , size(get_storage_size()) {}
  SQLTypeInfo()
      : type(kNULLT)
      , subtype(kNULLT)
      , dimension(0)
      , scale(0)
      , notnull(false)
      , compression(kENCODING_NONE)
      , comp_param(0)
      , size(0) {}

  inline SQLTypes get_type() const { return type; }
  inline SQLTypes get_subtype() const { return subtype; }
  inline int get_dimension() const { return dimension; }
  inline int get_precision() const { return dimension; }
  inline int get_input_srid() const { return dimension; }
  inline int get_scale() const { return scale; }
  inline int get_output_srid() const { return scale; }
  inline bool get_notnull() const { return notnull; }
  inline EncodingType get_compression() const { return compression; }
  inline int get_comp_param() const { return comp_param; }
  inline int get_size() const { return size; }
  inline int get_logical_size() const {
    if (compression == kENCODING_FIXED || compression == kENCODING_DATE_IN_DAYS) {
      SQLTypeInfo ti(type, dimension, scale, notnull, kENCODING_NONE, 0, subtype);
      return ti.get_size();
    }
    if (compression == kENCODING_DICT) {
      return 4;
    }
    return get_size();
  }
  inline int get_physical_cols() const { return 0; }
  inline int get_physical_coord_cols() const { return 0; }
  inline bool has_bounds() const { return false; }
  inline bool has_render_group() const { return false; }
  inline void set_type(SQLTypes t) { type = t; }
  inline void set_subtype(SQLTypes st) { subtype = st; }
  inline void set_dimension(int d) { dimension = d; }
  inline void set_precision(int d) { dimension = d; }
  inline void set_input_srid(int d) { dimension = d; }
  inline void set_scale(int s) { scale = s; }
  inline void set_output_srid(int s) { scale = s; }
  inline void set_notnull(bool n) { notnull = n; }
  inline void set_size(int s) { size = s; }
  inline void set_fixed_size() { size = get_storage_size(); }
  inline void set_compression(EncodingType c) { compression = c; }
  inline void set_comp_param(int p) { comp_param = p; }
  inline std::string get_type_name() const {
    std::string ps = "";
    if (type == kDECIMAL || type == kNUMERIC) {
      ps = "(" + std::to_string(dimension) + "," + std::to_string(scale) + ")";
    } else if (type == kTIMESTAMP) {
      ps = "(" + std::to_string(dimension) + ")";
    }
    if (type == kARRAY) {
      auto elem_ti = get_elem_type();
      auto num_elems = (size > 0) ? std::to_string(size / elem_ti.get_size()) : "";
      CHECK_LT(static_cast<int>(subtype), kSQLTYPE_LAST);
      return elem_ti.get_type_name() + ps + "[" + num_elems + "]";
    }
    if (type == kCOLUMN) {
      auto elem_ti = get_elem_type();
      auto num_elems =
          (size > 0) ? "[" + std::to_string(size / elem_ti.get_size()) + "]" : "";
      CHECK_LT(static_cast<int>(subtype), kSQLTYPE_LAST);
      return "COLUMN<" + std::string{type_name[static_cast<int>(subtype)]} + ps + ">" +
             num_elems;
    }
    if (type == kCOLUMN_LIST) {
      auto elem_ti = get_elem_type();
      auto num_elems =
          (size > 0) ? "[" + std::to_string(size / elem_ti.get_size()) + "]" : "";
      CHECK_LT(static_cast<int>(subtype), kSQLTYPE_LAST);
      return "COLUMN_LIST<" + std::string{type_name[static_cast<int>(subtype)]} + ps +
             ">" + num_elems;
    }
    return std::string{type_name[static_cast<int>(type)]} + ps;
  }
  inline std::string get_compression_name() const {
    return std::string{comp_name[(int)compression]};
  }
  std::string toString() const { return to_string(); }  // for PRINT macro
  inline std::string to_string() const {
    return concat("(type=",
                  type_name[static_cast<int>(type)].data(),
                  ", dimension=",
                  get_dimension(),
                  ", scale=",
                  get_scale(),
                  ", null=",
                  get_notnull() ? "not nullable" : "nullable",
                  ", name=",
                  get_compression_name(),
                  ", comp=",
                  get_comp_param(),
                  ", subtype=",
                  type_name[static_cast<int>(subtype)].data(),
                  ", size=",
                  get_size(),
                  ", element_size=",
                  get_elem_type().get_size(),
                  ")");
  }
  inline std::string get_buffer_name() const {
    if (is_array())
      return "Array";
    if (is_bytes())
      return "Bytes";
    if (is_column())
      return "Column";
    assert(false);
    return "";
  }
  inline bool is_string() const { return IS_STRING(type); }
  inline bool is_string_array() const { return (type == kARRAY) && IS_STRING(subtype); }
  inline bool is_integer() const { return IS_INTEGER(type); }
  inline bool is_decimal() const { return type == kDECIMAL || type == kNUMERIC; }
  inline bool is_fp() const { return type == kFLOAT || type == kDOUBLE; }
  inline bool is_number() const { return IS_NUMBER(type); }
  inline bool is_time() const { return is_datetime(type); }
  inline bool is_boolean() const { return type == kBOOLEAN; }
  inline bool is_array() const { return type == kARRAY; }  // rbc Array
  inline bool is_varlen_array() const { return type == kARRAY && size <= 0; }
  inline bool is_fixlen_array() const { return type == kARRAY && size > 0; }
  inline bool is_timeinterval() const { return IS_INTERVAL(type); }
  inline bool is_column() const { return type == kCOLUMN; }            // rbc Column
  inline bool is_column_list() const { return type == kCOLUMN_LIST; }  // rbc ColumnList
  inline bool is_bytes() const {
    return type == kTEXT && get_compression() == kENCODING_NONE;
  }  // rbc Bytes
  inline bool is_buffer() const {
    return is_array() || is_column() || is_column_list() || is_bytes();
  }
  inline bool transforms() const { return false; }

  inline bool is_varlen() const {  // TODO: logically this should ignore fixlen arrays
    return (IS_STRING(type) && compression != kENCODING_DICT) || type == kARRAY;
  }

  // need this here till is_varlen can be fixed w/o negative impact to existing code
  inline bool is_varlen_indeed() const {
    // SQLTypeInfo.is_varlen() is broken with fixedlen array now
    // and seems left broken for some concern, so fix it locally
    return is_varlen() && !is_fixlen_array();
  }

  inline bool is_dict_encoded_string() const {
    return is_string() && compression == kENCODING_DICT;
  }

  inline bool is_none_encoded_string() const {
    return is_string() && compression == kENCODING_NONE;
  }

  inline bool is_subtype_dict_encoded_string() const {
    return IS_STRING(subtype) && compression == kENCODING_DICT;
  }

  inline bool is_dict_encoded_type() const {
    return is_dict_encoded_string() ||
           (is_array() && get_elem_type().is_dict_encoded_string());
  }

  inline bool operator!=(const SQLTypeInfo& rhs) const {
    return type != rhs.get_type() || subtype != rhs.get_subtype() ||
           dimension != rhs.get_dimension() || scale != rhs.get_scale() ||
           compression != rhs.get_compression() ||
           (compression != kENCODING_NONE && comp_param != rhs.get_comp_param() &&
            comp_param != TRANSIENT_DICT(rhs.get_comp_param())) ||
           notnull != rhs.get_notnull();
  }
  inline bool operator==(const SQLTypeInfo& rhs) const {
    return type == rhs.get_type() && subtype == rhs.get_subtype() &&
           dimension == rhs.get_dimension() && scale == rhs.get_scale() &&
           compression == rhs.get_compression() &&
           (compression == kENCODING_NONE || comp_param == rhs.get_comp_param() ||
            comp_param == TRANSIENT_DICT(rhs.get_comp_param())) &&
           notnull == rhs.get_notnull();
  }

  inline int get_array_context_logical_size() const {
    if (is_string()) {
      auto comp_type(get_compression());
      if (comp_type == kENCODING_DICT || comp_type == kENCODING_FIXED ||
          comp_type == kENCODING_NONE) {
        return sizeof(int32_t);
      }
    }
    return get_logical_size();
  }

  inline void operator=(const SQLTypeInfo& rhs) {
    type = rhs.get_type();
    subtype = rhs.get_subtype();
    dimension = rhs.get_dimension();
    scale = rhs.get_scale();
    notnull = rhs.get_notnull();
    compression = rhs.get_compression();
    comp_param = rhs.get_comp_param();
    size = rhs.get_size();
    children = rhs.children;
  }

  inline bool is_castable(const SQLTypeInfo& new_type_info) const {
    // can always cast between the same type but different precision/scale/encodings
    if (type == new_type_info.get_type()) {
      return true;
      // can always cast from or to string
    } else if (is_string() || new_type_info.is_string()) {
      return true;
      // can cast between numbers
    } else if (is_number() && new_type_info.is_number()) {
      return true;
      // can cast from timestamp or date to number (epoch)
    } else if ((type == kTIMESTAMP || type == kDATE) && new_type_info.is_number()) {
      return true;
      // can cast from date to timestamp
    } else if (type == kDATE && new_type_info.get_type() == kTIMESTAMP) {
      return true;
    } else if (type == kTIMESTAMP && new_type_info.get_type() == kDATE) {
      return true;
    } else if (type == kBOOLEAN && new_type_info.is_number()) {
      return true;
    } else if (type == kARRAY && new_type_info.get_type() == kARRAY) {
      return get_elem_type().is_castable(new_type_info.get_elem_type());
    } else if (type == kCOLUMN && new_type_info.get_type() == kCOLUMN) {
      return get_elem_type().is_castable(new_type_info.get_elem_type());
    } else if (type == kCOLUMN_LIST && new_type_info.get_type() == kCOLUMN_LIST) {
      return get_elem_type().is_castable(new_type_info.get_elem_type());
    } else {
      return false;
    }
  }

  /**
   * @brief returns true if the sql_type can be cast to the type specified by
   * new_type_info with no loss of precision. Currently only used in
   * ExtensionFunctionsBindings to determine legal function matches, but we should
   * consider whether we need to rationalize implicit casting behavior more broadly in
   * QueryEngine.
   */

  inline bool is_numeric_scalar_auto_castable(const SQLTypeInfo& new_type_info) const {
    const auto& new_type = new_type_info.get_type();
    switch (type) {
      case kBOOLEAN:
        return new_type == kBOOLEAN;
      case kTINYINT:
      case kSMALLINT:
      case kINT:
        if (!new_type_info.is_number()) {
          return false;
        }
        if (new_type_info.is_fp()) {
          // We can lose precision here, but preserving existing behavior
          return true;
        }
        return new_type_info.get_logical_size() >= get_logical_size();
      case kBIGINT:
        return new_type == kBIGINT || new_type == kDOUBLE;
      case kFLOAT:
      case kDOUBLE:
        if (!new_type_info.is_fp()) {
          return false;
        }
        return (new_type_info.get_logical_size() >= get_logical_size());
      case kDECIMAL:
      case kNUMERIC:
        switch (new_type) {
          case kDECIMAL:
          case kNUMERIC:
            return new_type_info.get_dimension() >= get_dimension();
          case kDOUBLE:
            return true;
          case kFLOAT:
            return get_dimension() <= 7;
          default:
            return false;
        }
      case kTIMESTAMP:
        if (new_type != kTIMESTAMP) {
          return false;
        }
        return new_type_info.get_dimension() >= get_dimension();
      case kDATE:
        return new_type == kDATE;
      case kTIME:
        return new_type == kTIME;
      default:
        UNREACHABLE();
        return false;
    }
  }

  /**
   * @brief returns integer between 1 and 8 indicating what is roughly equivalent to the
   * logical byte size of a scalar numeric type (including boolean + time types), but with
   * decimals and numerics mapped to the byte size of their dimension (which may vary from
   * the type width), and timestamps, dates and times handled in a relative fashion.
   * Note: this function only takes the scalar numeric types above, and will throw a check
   * for other types.
   */

  inline int32_t get_numeric_scalar_scale() const {
    CHECK(type == kBOOLEAN || type == kTINYINT || type == kSMALLINT || type == kINT ||
          type == kBIGINT || type == kFLOAT || type == kDOUBLE || type == kDECIMAL ||
          type == kNUMERIC || type == kTIMESTAMP || type == kDATE || type == kTIME);
    switch (type) {
      case kBOOLEAN:
        return 1;
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
        return get_logical_size();
      case kDECIMAL:
      case kNUMERIC:
        if (get_dimension() > 7) {
          return 8;
        } else {
          return 4;
        }
      case kTIMESTAMP:
        switch (get_dimension()) {
          case 9:
            return 8;
          case 6:
            return 4;
          case 3:
            return 2;
          case 0:
            return 1;
          default:
            UNREACHABLE();
        }
      case kDATE:
        return 1;
      case kTIME:
        return 1;
      default:
        UNREACHABLE();
        return 0;
    }
  }

  inline SQLTypeInfo get_elem_type() const {
    return SQLTypeInfo(
        subtype, dimension, scale, notnull, compression, comp_param, kNULLT);
  }
  inline SQLTypeInfo get_array_type() const {
    return SQLTypeInfo(kARRAY, dimension, scale, notnull, compression, comp_param, type);
  }

  inline bool is_date_in_days() const {
    if (type == kDATE) {
      const auto comp_type = get_compression();
      if (comp_type == kENCODING_DATE_IN_DAYS) {
        return true;
      }
    }
    return false;
  }

  inline bool is_date() const { return type == kDATE; }

  inline bool is_high_precision_timestamp() const {
    if (type == kTIMESTAMP || type == kTIME) {
      const auto dimension = get_dimension();
      if (dimension > 0) {
        return true;
      }
    }
    return false;
  }

  inline bool is_timestamp() const { return type == kTIMESTAMP; }

  size_t getChildrenNum() const { return children.size(); }

  void appendChild(const SQLTypeInfo& child) { children.push_back(child); }

  SQLTypeInfo& getChildAt(size_t index) {
    CHECK_LT(index, getChildrenNum());
    return children[index];
  }

  const SQLTypeInfo& getChildAt(size_t index) const {
    CHECK_LT(index, getChildrenNum());
    return children[index];
  }

 private:
  SQLTypes type;     // type id
  SQLTypes subtype;  // element type of arrays or columns
  int dimension;     // VARCHAR/CHAR length or NUMERIC/DECIMAL precision or COLUMN_LIST
                     // length
  int scale;         // NUMERIC/DECIMAL scale
  bool notnull;      // nullable?  a hint, not used for type checking
  EncodingType compression;  // compression scheme
  int comp_param;            // compression parameter when applicable for certain schemes
  int size;                  // size of the type in bytes.  -1 for variable size
  std::vector<SQLTypeInfo> children;
  static std::string_view type_name[kSQLTYPE_LAST];
  static std::string_view comp_name[kENCODING_LAST];
  inline int get_storage_size() const {
    switch (type) {
      case kBOOLEAN:
        return sizeof(int8_t);
      case kTINYINT:
        return sizeof(int8_t);
      case kSMALLINT:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(int16_t);
          case kENCODING_FIXED:
          case kENCODING_SPARSE:
            return comp_param / 8;
          case kENCODING_RL:
          case kENCODING_DIFF:
            break;
          default:
            assert(false);
        }
        break;
      case kINT:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(int32_t);
          case kENCODING_FIXED:
          case kENCODING_SPARSE:
            return comp_param / 8;
          case kENCODING_RL:
          case kENCODING_DIFF:
            break;
          default:
            assert(false);
        }
        break;
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(int64_t);
          case kENCODING_FIXED:
          case kENCODING_SPARSE:
            return comp_param / 8;
          case kENCODING_RL:
          case kENCODING_DIFF:
            break;
          default:
            assert(false);
        }
        break;
      case kFLOAT:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(float);
          case kENCODING_FIXED:
          case kENCODING_RL:
          case kENCODING_DIFF:
          case kENCODING_SPARSE:
            assert(false);
            break;
          default:
            assert(false);
        }
        break;
      case kDOUBLE:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(double);
          case kENCODING_FIXED:
          case kENCODING_RL:
          case kENCODING_DIFF:
          case kENCODING_SPARSE:
            assert(false);
            break;
          default:
            assert(false);
        }
        break;
      case kDATE:
        return sizeof(int32_t);
      case kTIMESTAMP:
      case kTIME:
      case kINTERVAL_DAY_TIME:
      case kINTERVAL_YEAR_MONTH:
        switch (compression) {
          case kENCODING_NONE:
            return sizeof(int64_t);
          case kENCODING_FIXED:
            if (type == kTIMESTAMP && dimension > 0) {
              assert(false);  // disable compression for timestamp precisions
            }
            return comp_param / 8;
          case kENCODING_RL:
          case kENCODING_DIFF:
          case kENCODING_SPARSE:
            assert(false);
            break;
          case kENCODING_DATE_IN_DAYS:
            switch (comp_param) {
              case 0:
                return 4;  // Default date encoded in days is 32 bits
              case 16:
              case 32:
                return comp_param / 8;
              default:
                assert(false);
                break;
            }
          default:
            assert(false);
        }
        break;
      case kTEXT:
      case kVARCHAR:
      case kCHAR:
        if (compression == kENCODING_DICT) {
          return sizeof(int32_t);  // @TODO(wei) must check DictDescriptor
        }
        break;
      case kARRAY:
        // TODO: return size for fixlen arrays?
        break;
      case kCOLUMN:
      case kCOLUMN_LIST:
        break;
      default:
        break;
    }
    return -1;
  }
};

SQLTypes decimal_to_int_type(const SQLTypeInfo&);

inline SQLTypes get_int_type_by_size(size_t const nbytes) {
  switch (nbytes) {
    case 1:
      return kTINYINT;
    case 2:
      return kSMALLINT;
    case 4:
      return kINT;
    case 8:
      return kBIGINT;
    default:
      UNREACHABLE() << "Invalid number of bytes=" << nbytes;
      return {};
  }
}

inline std::ostream& operator<<(std::ostream& os, const SQLTypeInfo& ti) {
  os << ti.toString();
  return os;
}

#include <string_view>

Datum StringToDatum(const std::string_view s, SQLTypeInfo& ti);
std::string DatumToString(const Datum d, const SQLTypeInfo& ti);
int64_t extract_int_type_from_datum(const Datum datum, const SQLTypeInfo& ti);
double extract_fp_type_from_datum(const Datum datum, const SQLTypeInfo& ti);

bool DatumEqual(const Datum, const Datum, const SQLTypeInfo& ti);
int64_t convert_decimal_value_to_scale(const int64_t decimal_value,
                                       const SQLTypeInfo& type_info,
                                       const SQLTypeInfo& new_type_info);

#include "function/datetime/DateAdd.h"
#include "function/datetime/DateTruncate.h"
#include "function/datetime/ExtractFromTime.h"

inline SQLTypeInfo get_logical_type_info(const SQLTypeInfo& type_info) {
  EncodingType encoding = type_info.get_compression();
  if (encoding == kENCODING_DATE_IN_DAYS ||
      (encoding == kENCODING_FIXED && type_info.get_type() != kARRAY)) {
    encoding = kENCODING_NONE;
  }
  return SQLTypeInfo(type_info.get_type(),
                     type_info.get_dimension(),
                     type_info.get_scale(),
                     type_info.get_notnull(),
                     encoding,
                     type_info.get_comp_param(),
                     type_info.get_subtype());
}

inline SQLTypeInfo get_nullable_type_info(const SQLTypeInfo& type_info) {
  SQLTypeInfo nullable_type_info = type_info;
  nullable_type_info.set_notnull(false);
  return nullable_type_info;
}

inline SQLTypeInfo get_nullable_logical_type_info(const SQLTypeInfo& type_info) {
  SQLTypeInfo nullable_type_info = get_logical_type_info(type_info);
  return get_nullable_type_info(nullable_type_info);
}

using StringOffsetT = int32_t;
using ArrayOffsetT = int32_t;

inline int8_t* appendDatum(int8_t* buf, Datum d, const SQLTypeInfo& ti) {
  switch (ti.get_type()) {
    case kBOOLEAN:
      *(int8_t*)buf = d.boolval;
      return buf + sizeof(int8_t);
    case kNUMERIC:
    case kDECIMAL:
    case kBIGINT:
      *(int64_t*)buf = d.bigintval;
      return buf + sizeof(int64_t);
    case kINT:
      *(int32_t*)buf = d.intval;
      return buf + sizeof(int32_t);
    case kSMALLINT:
      *(int16_t*)buf = d.smallintval;
      return buf + sizeof(int16_t);
    case kTINYINT:
      *(int8_t*)buf = d.tinyintval;
      return buf + sizeof(int8_t);
    case kFLOAT:
      *(float*)buf = d.floatval;
      return buf + sizeof(float);
    case kDOUBLE:
      *(double*)buf = d.doubleval;
      return buf + sizeof(double);
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      *reinterpret_cast<int64_t*>(buf) = d.bigintval;
      return buf + sizeof(int64_t);
    default:
      return nullptr;
  }
}

inline auto generate_array_type(const SQLTypes subtype) {
  auto ti = SQLTypeInfo(kARRAY, false);
  ti.set_subtype(subtype);
  return ti;
}

inline auto generate_column_type(const SQLTypes subtype) {
  auto ti = SQLTypeInfo(kCOLUMN, false);
  ti.set_subtype(subtype);
  return ti;
}

inline auto generate_column_type(const SQLTypes subtype, EncodingType c, int p) {
  auto ti = SQLTypeInfo(kCOLUMN, false);
  ti.set_subtype(subtype);
  ti.set_compression(c);
  ti.set_comp_param(p);
  return ti;
}

inline auto generate_column_list_type(const SQLTypes subtype) {
  auto ti = SQLTypeInfo(kCOLUMN_LIST, false);
  ti.set_subtype(subtype);
  return ti;
}
