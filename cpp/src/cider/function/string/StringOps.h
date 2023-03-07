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

#pragma once

//#include "Logger/Logger.h"
#include "StringOpInfo.h"
#include "cider/CiderException.h"
#include "type/data/sqltypes.h"
#include "util/sqldefs.h"

#include <algorithm>
#include <bitset>
#include <cctype>
#include <cmath>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace StringOps_Namespace {

struct NullableStrType {
  NullableStrType(const std::string& str) : str(str), is_null(false) {}
  NullableStrType(const std::string_view sv) : str(sv), is_null(false) {}
  NullableStrType() : is_null(true) {}

  std::pair<std::string, bool> toPair() const { return {str, is_null}; }

  std::string str;
  bool is_null;
};

struct StringOp {
 public:
  StringOp(const SqlStringOpKind op_kind,
           const std::optional<std::string>& var_str_optional_literal)
      : op_kind_(op_kind)
      , return_ti_(SQLTypeInfo(kTEXT))
      , has_var_str_literal_(var_str_optional_literal.has_value())
      , var_str_literal_(!var_str_optional_literal.has_value()
                             ? NullableStrType()
                             : NullableStrType(var_str_optional_literal.value())) {}

  StringOp(const SqlStringOpKind op_kind,
           const SQLTypeInfo& return_ti,
           const std::optional<std::string>& var_str_optional_literal)
      : op_kind_(op_kind)
      , return_ti_(return_ti)
      , has_var_str_literal_(var_str_optional_literal.has_value())
      , var_str_literal_(!var_str_optional_literal.has_value()
                             ? NullableStrType()
                             : NullableStrType(var_str_optional_literal.value())) {}

  virtual const SQLTypeInfo& getReturnType() const { return return_ti_; }

  virtual NullableStrType operator()(std::string const&) const = 0;

  virtual NullableStrType operator()() const {
    CHECK(hasVarStringLiteral());
    if (var_str_literal_.is_null) {
      return var_str_literal_;
    }
    return operator()(var_str_literal_.str);
  }

  const std::string& getVarStringLiteral() const {
    CHECK(hasVarStringLiteral());
    return var_str_literal_.str;
  }

  bool hasVarStringLiteral() const { return has_var_str_literal_; }

  virtual ~StringOp() = default;

 protected:
  static std::regex generateRegex(const std::string& op_name,
                                  const std::string& regex_pattern,
                                  const std::string& regex_params,
                                  const bool supports_sub_matches);

  const SqlStringOpKind op_kind_;
  const SQLTypeInfo return_ti_;
  const bool has_var_str_literal_{false};
  const NullableStrType var_str_literal_;
};

struct TryStringCast : public StringOp {
 public:
  TryStringCast(const SQLTypeInfo& return_ti,
                const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::TRY_STRING_CAST, return_ti, var_str_optional_literal) {}

  NullableStrType operator()(const std::string& str) const override;
};

struct CharLength : public StringOp {
 public:
  CharLength(const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::CHAR_LENGTH,
                 SQLTypeInfo(kINT, true),
                 var_str_optional_literal) {}

  NullableStrType operator()(const std::string& str) const override;
};

struct Lower : public StringOp {
  Lower(const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::LOWER, var_str_optional_literal) {}

  NullableStrType operator()(const std::string& str) const override;
};

struct Upper : public StringOp {
  Upper(const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::UPPER, var_str_optional_literal) {}
  NullableStrType operator()(const std::string& str) const override;
};

inline std::bitset<256> build_char_bitmap(const std::string& chars_to_set) {
  std::bitset<256> char_bitmap;
  for (const auto& str_char : chars_to_set) {
    char_bitmap.set(str_char);
  }
  return char_bitmap;
}

struct InitCap : public StringOp {
  InitCap(const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::INITCAP, var_str_optional_literal)
      , delimiter_bitmap_(build_char_bitmap(InitCap::delimiter_chars)) {}

  NullableStrType operator()(const std::string& str) const override;

 private:
  static constexpr char const* delimiter_chars = R"(!?@"^#$&~_,.:;+-*%/|\[](){}<>)";
  const std::bitset<256> delimiter_bitmap_;
};

struct Reverse : public StringOp {
  Reverse(const std::optional<std::string>& var_str_optional_literal)
      : StringOp(SqlStringOpKind::REVERSE, var_str_optional_literal) {}

  NullableStrType operator()(const std::string& str) const override;
};

struct Repeat : public StringOp {
 public:
  Repeat(const std::optional<std::string>& var_str_optional_literal, const int64_t n)
      : StringOp(SqlStringOpKind::REPEAT, var_str_optional_literal)
      , n_(n >= 0 ? n : 0UL) {
    if (n < 0) {
      CIDER_THROW(CiderCompileException, "Number of repeats must be >= 0");
    }
  }

  NullableStrType operator()(const std::string& str) const override;

 private:
  const size_t n_;
};

struct Concat : public StringOp {
  Concat(const std::optional<std::string>& var_str_optional_literal,
         const std::string& str_literal,
         const bool reverse_order)
      : StringOp(reverse_order ? SqlStringOpKind::RCONCAT : SqlStringOpKind::CONCAT,
                 var_str_optional_literal)
      , str_literal_(str_literal)
      , reverse_order_(reverse_order) {}

  NullableStrType operator()(const std::string& str) const override;

  const std::string str_literal_;
  const bool reverse_order_;
};

struct Pad : public StringOp {
 public:
  enum class PadMode { LEFT, RIGHT };

  Pad(const std::optional<std::string>& var_str_optional_literal,
      const SqlStringOpKind op_kind,
      const int64_t padded_length,
      const std::string& padding_string)
      : StringOp(op_kind, var_str_optional_literal)
      , pad_mode_(Pad::op_kind_to_pad_mode(op_kind))
      , padded_length_(static_cast<size_t>(padded_length))
      , padding_string_(padding_string.empty() ? " " : padding_string)
      , padding_string_length_(padding_string.size())
      , padding_char_(padding_string.empty() ? ' ' : padding_string[0]) {}

  NullableStrType operator()(const std::string& str) const override;

 private:
  std::string lpad(const std::string& str) const;

  std::string rpad(const std::string& str) const;

  static PadMode op_kind_to_pad_mode(const SqlStringOpKind op_kind);

  const PadMode pad_mode_;
  const size_t padded_length_;
  const std::string padding_string_;
  const size_t padding_string_length_;
  const char padding_char_;
};

struct Trim : public StringOp {
 public:
  enum class TrimMode { LEFT, RIGHT, BOTH };

  Trim(const std::optional<std::string>& var_str_optional_literal,
       const SqlStringOpKind op_kind,
       const std::string& trim_chars)
      : StringOp(op_kind, var_str_optional_literal)
      , trim_mode_(Trim::op_kind_to_trim_mode(op_kind))
      , trim_char_bitmap_(build_char_bitmap(trim_chars.empty() ? " " : trim_chars)) {}

  NullableStrType operator()(const std::string& str) const override;

 private:
  static TrimMode op_kind_to_trim_mode(const SqlStringOpKind op_kind);

  const TrimMode trim_mode_;
  const std::bitset<256> trim_char_bitmap_;
};

struct Substring : public StringOp {
  // First constructor is for CALCITE SUBSTRING(str FROM start_pos),
  // which returns the substring of str from start_pos
  // until the end of the string
  // Note start_pos is 1-indexed, unless input is 0

  Substring(const std::optional<std::string>& var_str_optional_literal,
            const int64_t start)
      : StringOp(SqlStringOpKind::SUBSTRING, var_str_optional_literal)
      , start_(start > 0 ? start - 1 : start)
      , length_(std::string::npos) {}

  // Second constructor is for CALCITE
  // SUBSTRING(str FROM start_pos FOR length),
  // which copies from start_pos for length characters
  // Note start_pos is 1-indexed, unless input is 0
  Substring(const std::optional<std::string>& var_str_optional_literal,
            const int64_t start,
            const int64_t length)
      : StringOp(SqlStringOpKind::SUBSTRING, var_str_optional_literal)
      , start_(start > 0 ? start - 1 : start)
      , length_(static_cast<size_t>(length >= 0 ? length : 0)) {}

  NullableStrType operator()(const std::string& str) const override;

  // Make string_view version?
  const int64_t start_;
  const size_t length_;
};

struct Overlay : public StringOp {
  Overlay(const std::optional<std::string>& var_str_optional_literal,
          const std::string& insert_str,
          const int64_t start)
      : StringOp(SqlStringOpKind::OVERLAY, var_str_optional_literal)
      , insert_str_(insert_str)
      , start_(start > 0 ? start - 1 : start)
      , replacement_length_(insert_str_.size()) {}

  Overlay(const std::optional<std::string>& var_str_optional_literal,
          const std::string& insert_str,
          const int64_t start,
          const int64_t replacement_length)
      : StringOp(SqlStringOpKind::OVERLAY, var_str_optional_literal)
      , insert_str_(insert_str)
      , start_(start > 0 ? start - 1 : start)
      , replacement_length_(
            static_cast<size_t>(replacement_length >= 0 ? replacement_length : 0)) {}

  NullableStrType operator()(const std::string& base_str) const override;

  // Make string_view version?
  const std::string insert_str_;
  const int64_t start_;
  const size_t replacement_length_;
};

struct Replace : public StringOp {
  Replace(const std::optional<std::string>& var_str_optional_literal,
          const std::string& pattern_str,
          const std::string& replacement_str)
      : StringOp(SqlStringOpKind::REPLACE, var_str_optional_literal)
      , pattern_str_(pattern_str)
      , replacement_str_(replacement_str)
      , pattern_str_len_(pattern_str.size())
      , replacement_str_len_(replacement_str.size()) {}

  NullableStrType operator()(const std::string& str) const override;

  const std::string pattern_str_;
  const std::string replacement_str_;
  const size_t pattern_str_len_;
  const size_t replacement_str_len_;
};

struct SplitPart : public StringOp {
  SplitPart(const std::optional<std::string>& var_str_optional_literal,
            const std::string& delimiter,
            const int64_t limit,
            const int64_t split_part)
      : StringOp(SqlStringOpKind::SPLIT_PART, var_str_optional_literal)
      , delimiter_(delimiter)
      , split_part_(split_part == 0 ? 1UL : std::abs(split_part))
      , limit_(limit)
      , delimiter_length_(delimiter.size())
      , reverse_(split_part < 0) {
    if (limit_ <= 0) {
      CIDER_THROW(CiderCompileException, "split(): param 'limit' must be positive");
    }
  }

  SplitPart(const std::optional<std::string>& var_str_optional_literal,
            const std::string& delimiter,
            const int64_t split_part)
      : StringOp(SqlStringOpKind::SPLIT_PART, var_str_optional_literal)
      , delimiter_(delimiter)
      , split_part_(split_part == 0 ? 1UL : std::abs(split_part))
      , limit_(0)  // informal internal representation of 'no limit specified'
      , delimiter_length_(delimiter.size())
      , reverse_(split_part < 0) {}

  NullableStrType operator()(const std::string& str) const override;

  // Make string_view version?

  const std::string delimiter_;
  const size_t split_part_;
  const size_t limit_;
  const size_t delimiter_length_;
  const bool reverse_;
};

struct RegexpSubstr : public StringOp {
 public:
  RegexpSubstr(const std::optional<std::string>& var_str_optional_literal,
               const std::string& regex_pattern,
               const int64_t start_pos,
               const int64_t occurrence,
               const std::string& regex_params,
               const int64_t sub_match_group_idx)
      : StringOp(SqlStringOpKind::REGEXP_SUBSTR, var_str_optional_literal)
      , regex_pattern_str_(
            regex_pattern)  // for toString() as std::regex does not have str() method
      , regex_pattern_(
            StringOp::generateRegex("REGEXP_SUBSTR", regex_pattern, regex_params, true))
      , start_pos_(start_pos > 0 ? start_pos - 1 : start_pos)
      , occurrence_(occurrence > 0 ? occurrence - 1 : occurrence)
      , sub_match_info_(set_sub_match_info(regex_params, sub_match_group_idx)) {}

  NullableStrType operator()(const std::string& str) const override;

 private:
  static std::string get_sub_match(const std::smatch& match,
                                   const std::pair<bool, int64_t> sub_match_info);

  static std::pair<bool, int64_t> set_sub_match_info(const std::string& regex_pattern,
                                                     const int64_t sub_match_group_idx);

  const std::string regex_pattern_str_;
  const std::regex regex_pattern_;
  const int64_t start_pos_;
  const int64_t occurrence_;
  const std::pair<bool, int64_t> sub_match_info_;
};

struct RegexpReplace : public StringOp {
 public:
  RegexpReplace(const std::optional<std::string>& var_str_optional_literal,
                const std::string& regex_pattern,
                const std::string& replacement,
                const int64_t start_pos,
                const int64_t occurrence,
                const std::string& regex_params)
      : StringOp(SqlStringOpKind::REGEXP_REPLACE, var_str_optional_literal)
      , regex_pattern_str_(
            regex_pattern)  // for toString() as std::regex does not have str() method
      , regex_pattern_(
            StringOp::generateRegex("REGEXP_REPLACE", regex_pattern, regex_params, false))
      , replacement_(replacement)
      , start_pos_(start_pos > 0 ? start_pos - 1 : start_pos)
      , occurrence_(occurrence) {}

  NullableStrType operator()(const std::string& str) const override;

 private:
  static std::pair<size_t, size_t> get_nth_regex_match(const std::string& str,
                                                       const size_t start_pos,
                                                       const std::regex& regex_pattern,
                                                       const int64_t occurrence);

  const std::string regex_pattern_str_;
  const std::regex regex_pattern_;
  const std::string replacement_;
  const int64_t start_pos_;
  const int64_t occurrence_;
};

struct NullOp : public StringOp {
  NullOp(const std::optional<std::string>& var_str_optional_literal,
         const SqlStringOpKind op_kind)
      : StringOp(SqlStringOpKind::INVALID, var_str_optional_literal), op_kind_(op_kind) {}

  NullableStrType operator()(const std::string& str) const {
    return NullableStrType();  // null string
  }

  const SqlStringOpKind op_kind_;
};

std::unique_ptr<const StringOp> gen_string_op(const StringOpInfo& string_op_info);

std::pair<std::string, bool /* is null */> apply_string_op_to_literals(
    const StringOpInfo& string_op_info);

class StringOps {
 public:
  StringOps() : string_ops_(genStringOpsFromOpInfos({})), num_ops_(0UL) {}

  StringOps(const std::vector<StringOpInfo>& string_op_infos)
      : string_ops_(genStringOpsFromOpInfos(string_op_infos))
      , num_ops_(string_op_infos.size()) {}

  std::string operator()(const std::string& str) const;

  std::string_view operator()(const std::string_view sv, std::string& sv_storage) const;

  size_t size() const { return num_ops_; }

 private:
  std::vector<std::unique_ptr<const StringOp>> genStringOpsFromOpInfos(
      const std::vector<StringOpInfo>& string_op_infos) const;

  const std::vector<std::unique_ptr<const StringOp>> string_ops_;
  const size_t num_ops_;
};

}  // namespace StringOps_Namespace
