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

#include "CodeGenerator.h"
#include "Execute.h"

#include "exec/plan/parser/ParserNode.h"
#include "type/data/funcannotations.h"
#include "util/sqldefs.h"
#ifdef ENABLE_VELOX_FUNCTION
#include "../Cider/VeloxTypeConvertUtil.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/type/StringView.h"
#endif

#include <boost/locale/conversion.hpp>
#include "cider/CiderTypes.h"
#include "function/string/StringOps.h"

extern bool g_enable_watchdog;

extern "C" RUNTIME_EXPORT uint64_t string_decode(int8_t* chunk_iter_, int64_t pos) {
  auto chunk_iter = reinterpret_cast<ChunkIter*>(chunk_iter_);
  VarlenDatum vd;
  bool is_end;
  ChunkIter_get_nth(chunk_iter, pos, false, &vd, &is_end);
  CHECK(!is_end);
  return vd.is_null ? 0
                    : (reinterpret_cast<uint64_t>(vd.pointer) & 0xffffffffffff) |
                          (static_cast<uint64_t>(vd.length) << 48);
}

extern "C" RUNTIME_EXPORT uint64_t string_decode_cider(int8_t* byte_array, int64_t pos) {
  auto cider_byte_array = reinterpret_cast<CiderByteArray*>(byte_array);
  uint32_t len = cider_byte_array[pos].len;
  const uint8_t* ptr = cider_byte_array[pos].ptr;
  return len == 0 ? 0
                  : (reinterpret_cast<uint64_t>(ptr) & 0xffffffffffff) |
                        (static_cast<uint64_t>(len) << 48);
}

extern "C" RUNTIME_EXPORT uint64_t string_decompress(const int32_t string_id,
                                                     const int64_t string_dict_handle) {
  if (string_id == NULL_INT) {
    return 0;
  }
  auto string_dict_proxy =
      reinterpret_cast<const StringDictionaryProxy*>(string_dict_handle);
  auto string_bytes = string_dict_proxy->getStringBytes(string_id);
  CHECK(string_bytes.first);
  return (reinterpret_cast<uint64_t>(string_bytes.first) & 0xffffffffffff) |
         (static_cast<uint64_t>(string_bytes.second) << 48);
}

extern "C" RUNTIME_EXPORT int32_t string_compress(const int64_t ptr_and_len,
                                                  const int64_t string_dict_handle) {
  std::string raw_str(reinterpret_cast<char*>(extract_str_ptr_noinline(ptr_and_len)),
                      extract_str_len_noinline(ptr_and_len));
  auto string_dict_proxy =
      reinterpret_cast<const StringDictionaryProxy*>(string_dict_handle);
  return string_dict_proxy->getIdOfString(raw_str);
}

extern "C" RUNTIME_EXPORT int32_t
apply_string_ops_and_encode(const char* str_ptr,
                            const int32_t str_len,
                            const int64_t string_ops_handle,
                            const int64_t string_dict_handle) {
  std::string raw_str(str_ptr, str_len);
  auto string_ops =
      reinterpret_cast<const StringOps_Namespace::StringOps*>(string_ops_handle);
  auto string_dict_proxy = reinterpret_cast<StringDictionaryProxy*>(string_dict_handle);
  const auto result_str = string_ops->operator()(raw_str);
  if (result_str.empty()) {
    return inline_int_null_value<int32_t>();
  }
  return string_dict_proxy->getOrAddTransient(result_str);
}

extern "C" RUNTIME_EXPORT int64_t
apply_string_ops_and_encode_cider(const char* str_ptr,
                                  const int32_t str_len,
                                  const int64_t string_ops_handle,
                                  const int64_t string_hasher_handle) {
  std::string raw_str(str_ptr, str_len);
  auto string_ops =
      reinterpret_cast<const StringOps_Namespace::StringOps*>(string_ops_handle);
  auto string_hasher = reinterpret_cast<CiderStringHasher*>(string_hasher_handle);
  const auto result_str = string_ops->operator()(raw_str);

  // add this temp result to cache
  int64_t id = string_hasher->lookupIdByValue(
      CiderByteArray(result_str.length(), (const uint8_t*)result_str.c_str()));
  return id;
}

extern "C" RUNTIME_EXPORT int64_t
apply_string_ops_and_encode_cider_nullable(const char* str_ptr,
                                           const int32_t str_len,
                                           const int64_t string_ops_handle,
                                           const int64_t string_hasher_handle,
                                           bool is_null) {
  if (is_null) {
    return -1;
  }
  return apply_string_ops_and_encode_cider(
      str_ptr, str_len, string_ops_handle, string_hasher_handle);
}

extern "C" RUNTIME_EXPORT bool cider_check_string_id_is_null(const int64_t id) {
  return id == -1;
}
extern "C" RUNTIME_EXPORT char* cider_hasher_decode_str_ptr(
    const int64_t id,
    const int64_t string_hasher_handle) {
  CiderStringHasher* string_hasher =
      reinterpret_cast<CiderStringHasher*>(string_hasher_handle);
  CiderByteArray res = string_hasher->lookupValueById(id);
  return (char*)res.ptr;
}

extern "C" RUNTIME_EXPORT int32_t
cider_hasher_decode_str_len(const int64_t id, const int64_t string_hasher_handle) {
  auto string_hasher = reinterpret_cast<CiderStringHasher*>(string_hasher_handle);
  CiderByteArray res = string_hasher->lookupValueById(id);
  return res.len;
}

extern "C" RUNTIME_EXPORT int32_t lower_encoded(int32_t string_id,
                                                int64_t string_dict_proxy_address) {
  StringDictionaryProxy* string_dict_proxy =
      reinterpret_cast<StringDictionaryProxy*>(string_dict_proxy_address);
  auto str = string_dict_proxy->getString(string_id);
#ifdef ENABLE_VELOX_FUNCTION
  cider::CiderUDFOutputString result;
  facebook::velox::StringView sv(str.data(), str.size());
  facebook::velox::functions::stringImpl::lower<true>(result, sv);
  return string_dict_proxy->getOrAddTransient(result.data());
#else
  return string_dict_proxy->getOrAddTransient(boost::locale::to_lower(str));
#endif
}

#define DEF_APPLY_NUMERIC_STRING_OPS(value_type, value_name)                             \
  extern "C" RUNTIME_EXPORT ALWAYS_INLINE value_type                                     \
      apply_numeric_string_ops_##value_name(                                             \
          const char* str_ptr, const int32_t str_len, const int64_t string_ops_handle) { \
    std::string raw_str(str_ptr, str_len);                                               \
    auto string_ops =                                                                    \
        reinterpret_cast<const StringOps_Namespace::StringOps*>(string_ops_handle);      \
    const auto result_datum = string_ops->numericEval(raw_str);                          \
    return result_datum.value_name##val;                                                 \
  }

DEF_APPLY_NUMERIC_STRING_OPS(int8_t, bool)
DEF_APPLY_NUMERIC_STRING_OPS(int8_t, tinyint)
DEF_APPLY_NUMERIC_STRING_OPS(int16_t, smallint)
DEF_APPLY_NUMERIC_STRING_OPS(int32_t, int)
DEF_APPLY_NUMERIC_STRING_OPS(int64_t, bigint)
DEF_APPLY_NUMERIC_STRING_OPS(float, float)
DEF_APPLY_NUMERIC_STRING_OPS(double, double)

#undef DEF_APPLY_NUMERIC_STRING_OPS

llvm::Value* CodeGenerator::codegen(const Analyzer::CharLengthExpr* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  auto str_lv = codegen(expr->get_arg(), true, co);
  if (str_lv.size() != 3) {
    CHECK_EQ(size_t(1), str_lv.size());
    if (g_enable_watchdog) {
      CIDER_THROW(CiderWatchdogException,
                  "LENGTH / CHAR_LENGTH on dictionary-encoded strings would be slow");
    }
    str_lv.push_back(cgen_state_->emitCall("extract_str_ptr", {str_lv.front()}));
    str_lv.push_back(cgen_state_->emitCall("extract_str_len", {str_lv.front()}));
  }
  std::vector<llvm::Value*> charlength_args{str_lv[1], str_lv[2]};
  std::string fn_name("char_length");
  if (expr->get_calc_encoded_length()) {
    fn_name += "_encoded";
  }
  const bool is_nullable{!expr->get_arg()->get_type_info().get_notnull()};
  if (is_nullable) {
    fn_name += "_nullable";
    charlength_args.push_back(cgen_state_->inlineIntNull(expr->get_type_info()));
  }
  return expr->get_calc_encoded_length()
             ? cgen_state_->emitExternalCall(
                   fn_name, get_int_type(32, cgen_state_->context_), charlength_args)
             : cgen_state_->emitCall(fn_name, charlength_args);
}

llvm::Value* CodeGenerator::codegen(const Analyzer::KeyForStringExpr* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  auto str_lv = codegen(expr->get_arg(), true, co);
  CHECK_EQ(size_t(1), str_lv.size());
  return cgen_state_->emitCall("key_for_string_encoded", str_lv);
}

llvm::Value* CodeGenerator::codegen(const Analyzer::LowerExpr* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);

  auto str_id_lv = codegen(expr->get_arg(), true, co);
  CHECK_EQ(size_t(1), str_id_lv.size());

  const auto string_dictionary_proxy = executor()->getStringDictionaryProxy(
      expr->get_type_info().get_comp_param(), executor()->getRowSetMemoryOwner(), true);
  CHECK(string_dictionary_proxy);

  std::vector<llvm::Value*> args{
      str_id_lv[0],
      cgen_state_->llInt(reinterpret_cast<int64_t>(string_dictionary_proxy))};

  return cgen_state_->emitExternalCall(
      "lower_encoded", get_int_type(32, cgen_state_->context_), args);
}

std::vector<StringOps_Namespace::StringOpInfo> getStringOpInfos(
    const Analyzer::StringOper* expr) {
  std::vector<StringOps_Namespace::StringOpInfo> string_op_infos;
  StringOps_Namespace::StringOpInfo string_op_info(
      expr->get_kind(), expr->get_type_info(), expr->getLiteralArgs());
  string_op_infos.push_back(string_op_info);
  return string_op_infos;
}

const StringOps_Namespace::StringOps* getStringOps(
    const std::vector<StringOps_Namespace::StringOpInfo>& string_op_infos) {
  return new StringOps_Namespace::StringOps(string_op_infos);
}

llvm::Value* CodeGenerator::codegenPerRowStringOper(const Analyzer::StringOper* expr,
                                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK_GE(expr->getArity(), 1UL);

  const auto& expr_ti = expr->get_type_info();
  // Should probably CHECK we have a UOper cast to dict encoded to be consistent
  const auto primary_arg = remove_cast(expr->getArg(0));
  CHECK(primary_arg->get_type_info().is_none_encoded_string());
  auto primary_str_lv = codegen(primary_arg, true, co);
  CHECK_EQ(size_t(3), primary_str_lv.size());
  const auto string_op_infos = getStringOpInfos(expr);
  CHECK(string_op_infos.size());

  const auto string_ops = getStringOps(string_op_infos);
  const int64_t string_ops_handle = reinterpret_cast<int64_t>(string_ops);
  auto string_ops_handle_lv = cgen_state_->llInt(string_ops_handle);
  const auto& return_ti = expr->get_type_info();
  if (!return_ti.is_string()) {
    std::vector<llvm::Value*> string_oper_lvs{
        primary_str_lv[1], primary_str_lv[2], string_ops_handle_lv};
    const auto return_type = return_ti.get_type();
    std::string fn_call = "apply_numeric_string_ops_";
    switch (return_type) {
      case kBOOLEAN: {
        fn_call += "bool";
        break;
      }
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE: {
        fn_call += to_lower(toString(return_type));
        break;
      }
      case kNUMERIC:
      case kDECIMAL:
      case kTIME:
      case kTIMESTAMP:
      case kDATE: {
        fn_call += "bigint";
        break;
      }
      default: {
        throw std::runtime_error("Unimplemented type for string-to-numeric translation");
      }
    }
    const auto logical_size = return_ti.get_logical_size() * 8;
    auto llvm_return_type = return_ti.is_fp()
                                ? get_fp_type(logical_size, cgen_state_->context_)
                                : get_int_type(logical_size, cgen_state_->context_);
    return cgen_state_->emitExternalCall(fn_call, llvm_return_type, string_oper_lvs);
  }
  const int64_t dest_string_proxy_handle =
      reinterpret_cast<int64_t>(executor()->getCiderStringDictionaryProxy());
  auto dest_string_proxy_handle_lv = cgen_state_->llInt(dest_string_proxy_handle);
  std::vector<llvm::Value*> string_oper_lvs{primary_str_lv[1],
                                            primary_str_lv[2],
                                            string_ops_handle_lv,
                                            dest_string_proxy_handle_lv};

  return cgen_state_->emitExternalCall("apply_string_ops_and_encode",
                                       get_int_type(32, cgen_state_->context_),
                                       string_oper_lvs);
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenStringOpExpr(
    const Analyzer::StringOper* expr,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);

  CHECK_GE(expr->getArity(), 1UL);
  CHECK(expr->hasNoneEncodedTextArg());

  const auto& expr_ti = expr->get_type_info();
  // Should probably CHECK we have a UOper cast to dict encoded to be consistent
  const auto primary_arg = remove_cast(expr->getArg(0));
  CHECK(primary_arg->get_type_info().is_none_encoded_string());

  auto primary_str = codegen(primary_arg, co, true);  // Twovalue
  auto str_values = dynamic_cast<TwoValueColValues*>(primary_str.get());
  CHECK(str_values);

  const auto string_op_infos = getStringOpInfos(expr);
  CHECK(string_op_infos.size());
  const auto string_ops = getStringOps(string_op_infos);
  const int64_t string_ops_handle = reinterpret_cast<int64_t>(string_ops);
  auto string_ops_handle_lv = cgen_state_->llInt(string_ops_handle);
  const auto& return_ti = expr->get_type_info();
  if (!return_ti.is_string()) {
    CIDER_THROW(
        CiderCompileException,
        "For string op, non-string type return values is not supported currently.");
  }

  const int64_t cider_string_hasher_handle =
      reinterpret_cast<int64_t>(executor()->getCiderStringHasherHandle());
  auto cider_string_hasher_handle_lv = cgen_state_->llInt(cider_string_hasher_handle);
  std::string func_name = "apply_string_ops_and_encode_cider";

  std::vector<llvm::Value*> string_oper_lvs{str_values->getValueAt(0),
                                            str_values->getValueAt(1),
                                            string_ops_handle_lv,
                                            cider_string_hasher_handle_lv};
  if (str_values->getNull()) {
    func_name.append("_nullable");
    string_oper_lvs.push_back(str_values->getNull());
  }
  auto id = cgen_state_->emitExternalCall(
      func_name, get_int_type(64, cgen_state_->context_), string_oper_lvs);
  llvm::Value* res_null = nullptr;
  if (str_values->getNull()) {
    res_null = cgen_state_->emitExternalCall(
        "cider_check_string_id_is_null", get_int_type(1, cgen_state_->context_), {id});
  }
  llvm::Value* res_str_ptr =
      cgen_state_->emitExternalCall("cider_hasher_decode_str_ptr",
                                    get_int_ptr_type(8, cgen_state_->context_),
                                    {id, cider_string_hasher_handle_lv});
  llvm::Value* res_str_len =
      cgen_state_->emitExternalCall("cider_hasher_decode_str_len",
                                    get_int_type(32, cgen_state_->context_),
                                    {id, cider_string_hasher_handle_lv});

  return std::make_unique<TwoValueColValues>(res_str_ptr, res_str_len, res_null);
}

llvm::Value* CodeGenerator::codegen(const Analyzer::StringOper* expr,
                                    const CompilationOptions& co) {
  CHECK_GE(expr->getArity(), 1UL);
  if (expr->hasNoneEncodedTextArg()) {
    return codegenPerRowStringOper(expr, co);
  }
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenLikeExpr(
    const Analyzer::LikeExpr* expr,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (is_unnest(extract_cast_arg(expr->get_arg()))) {
    CIDER_THROW(CiderCompileException, "LIKE not supported for unnested expressions");
  }
  char escape_char{'\\'};
  if (expr->get_escape_expr()) {
    auto escape_char_expr =
        dynamic_cast<const Analyzer::Constant*>(expr->get_escape_expr());
    CHECK(escape_char_expr);
    CHECK(escape_char_expr->get_type_info().is_string());
    CHECK_EQ(size_t(1), escape_char_expr->get_constval().stringval->size());
    escape_char = (*escape_char_expr->get_constval().stringval)[0];
  }
  auto pattern = dynamic_cast<const Analyzer::Constant*>(expr->get_like_expr());
  CHECK(pattern);
  //  auto fast_dict_like_lv = codegenDictLike(expr->get_own_arg(),
  //                                           pattern,
  //                                           expr->get_is_ilike(),
  //                                           expr->get_is_simple(),
  //                                           escape_char,
  //                                           co);
  //  if (fast_dict_like_lv) {
  //    return fast_dict_like_lv;
  //  }
  const auto& ti = expr->get_arg()->get_type_info();
  CHECK(ti.is_string());
  if (g_enable_watchdog && ti.get_compression() != kENCODING_NONE) {
    CIDER_THROW(CiderWatchdogException,
                "Cannot do LIKE / ILIKE on this dictionary encoded column, its "
                "cardinality is too high");
  }
  auto str_lv = codegen(expr->get_arg(), co, true);
  MultipleValueColValues* str = dynamic_cast<MultipleValueColValues*>(str_lv.get());

  auto like_expr_arg_lvs = codegen(expr->get_like_expr(), co, true);
  MultipleValueColValues* like =
      dynamic_cast<MultipleValueColValues*>(like_expr_arg_lvs.get());

  const bool is_nullable{!expr->get_arg()->get_type_info().get_notnull()};
  llvm::Value* null = nullptr;

  std::vector<llvm::Value*> str_like_args{
      str->getValueAt(0), str->getValueAt(1), like->getValueAt(0), like->getValueAt(1)};
  std::string fn_name{expr->get_is_ilike() ? "string_ilike" : "string_like"};
  if (expr->get_is_simple()) {
    fn_name += "_simple";
  } else {
    str_like_args.push_back(cgen_state_->llInt(int8_t(escape_char)));
  }
  if (is_nullable) {
    null = str->getNull();
  }
  auto ret = std::make_unique<FixedSizeColValues>(
      cgen_state_->emitCall(fn_name, str_like_args), null);
  return ret;
}

llvm::Value* CodeGenerator::codegen(const Analyzer::LikeExpr* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (is_unnest(extract_cast_arg(expr->get_arg()))) {
    CIDER_THROW(CiderCompileException, "LIKE not supported for unnested expressions");
  }
  char escape_char{'\\'};
  if (expr->get_escape_expr()) {
    auto escape_char_expr =
        dynamic_cast<const Analyzer::Constant*>(expr->get_escape_expr());
    CHECK(escape_char_expr);
    CHECK(escape_char_expr->get_type_info().is_string());
    CHECK_EQ(size_t(1), escape_char_expr->get_constval().stringval->size());
    escape_char = (*escape_char_expr->get_constval().stringval)[0];
  }
  auto pattern = dynamic_cast<const Analyzer::Constant*>(expr->get_like_expr());
  CHECK(pattern);
  auto fast_dict_like_lv = codegenDictLike(expr->get_own_arg(),
                                           pattern,
                                           expr->get_is_ilike(),
                                           expr->get_is_simple(),
                                           escape_char,
                                           co);
  if (fast_dict_like_lv) {
    return fast_dict_like_lv;
  }
  const auto& ti = expr->get_arg()->get_type_info();
  CHECK(ti.is_string());
  if (g_enable_watchdog && ti.get_compression() != kENCODING_NONE) {
    CIDER_THROW(CiderWatchdogException,
                "Cannot do LIKE / ILIKE on this dictionary encoded column, its "
                "cardinality is too high");
  }
  auto str_lv = codegen(expr->get_arg(), true, co);
  if (str_lv.size() != 3) {
    CHECK_EQ(size_t(1), str_lv.size());
    str_lv.push_back(cgen_state_->emitCall("extract_str_ptr", {str_lv.front()}));
    str_lv.push_back(cgen_state_->emitCall("extract_str_len", {str_lv.front()}));
  }
  auto like_expr_arg_lvs = codegen(expr->get_like_expr(), true, co);
  CHECK_EQ(size_t(3), like_expr_arg_lvs.size());
  const bool is_nullable{!expr->get_arg()->get_type_info().get_notnull()};
  std::vector<llvm::Value*> str_like_args{
      str_lv[1], str_lv[2], like_expr_arg_lvs[1], like_expr_arg_lvs[2]};
  std::string fn_name{expr->get_is_ilike() ? "string_ilike" : "string_like"};
  if (expr->get_is_simple()) {
    fn_name += "_simple";
  } else {
    str_like_args.push_back(cgen_state_->llInt(int8_t(escape_char)));
  }
  if (is_nullable) {
    fn_name += "_nullable";
    str_like_args.push_back(cgen_state_->inlineIntNull(expr->get_type_info()));
  }
  return cgen_state_->emitCall(fn_name, str_like_args);
}

llvm::Value* CodeGenerator::codegenDictLike(
    const std::shared_ptr<Analyzer::Expr> like_arg,
    const Analyzer::Constant* pattern,
    const bool ilike,
    const bool is_simple,
    const char escape_char,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto cast_oper = std::dynamic_pointer_cast<Analyzer::UOper>(like_arg);
  if (!cast_oper) {
    return nullptr;
  }
  CHECK(cast_oper);
  CHECK_EQ(kCAST, cast_oper->get_optype());
  const auto dict_like_arg = cast_oper->get_own_operand();
  const auto& dict_like_arg_ti = dict_like_arg->get_type_info();
  if (!dict_like_arg_ti.is_string()) {
    CIDER_THROW(CiderCompileException,
                "Cast from " + dict_like_arg_ti.get_type_name() + " to " +
                    cast_oper->get_type_info().get_type_name() + " not supported");
  }
  CHECK_EQ(kENCODING_DICT, dict_like_arg_ti.get_compression());
  const auto sdp = executor()->getStringDictionaryProxy(
      dict_like_arg_ti.get_comp_param(), executor()->getRowSetMemoryOwner(), true);
  if (sdp->storageEntryCount() > 200000000) {
    return nullptr;
  }
  const auto& pattern_ti = pattern->get_type_info();
  CHECK(pattern_ti.is_string());
  CHECK_EQ(kENCODING_NONE, pattern_ti.get_compression());
  const auto& pattern_datum = pattern->get_constval();
  const auto& pattern_str = *pattern_datum.stringval;
  const auto matching_ids = sdp->getLike(pattern_str, ilike, is_simple, escape_char);
  // InIntegerSet requires 64-bit values
  std::vector<int64_t> matching_ids_64(matching_ids.size());
  std::copy(matching_ids.begin(), matching_ids.end(), matching_ids_64.begin());
  const auto in_values = std::make_shared<Analyzer::InIntegerSet>(
      dict_like_arg, matching_ids_64, dict_like_arg_ti.get_notnull());
  return codegen(in_values.get(), co);
}

namespace {

std::vector<int32_t> get_compared_ids(const StringDictionaryProxy* dict,
                                      const SQLOps compare_operator,
                                      const std::string& pattern) {
  std::vector<int> ret;
  switch (compare_operator) {
    case kLT:
      ret = dict->getCompare(pattern, "<");
      break;
    case kLE:
      ret = dict->getCompare(pattern, "<=");
      break;
    case kEQ:
    case kBW_EQ:
      ret = dict->getCompare(pattern, "=");
      break;
    case kGT:
      ret = dict->getCompare(pattern, ">");
      break;
    case kGE:
      ret = dict->getCompare(pattern, ">=");
      break;
    case kNE:
      ret = dict->getCompare(pattern, "<>");
      break;
    default:
      CIDER_THROW(CiderCompileException, "unsuported operator for string comparision");
  }
  return ret;
}
}  // namespace

llvm::Value* CodeGenerator::codegenDictStrCmp(const std::shared_ptr<Analyzer::Expr> lhs,
                                              const std::shared_ptr<Analyzer::Expr> rhs,
                                              const SQLOps compare_operator,
                                              const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  auto rhs_cast_oper = std::dynamic_pointer_cast<const Analyzer::UOper>(rhs);
  auto lhs_cast_oper = std::dynamic_pointer_cast<const Analyzer::UOper>(lhs);
  auto rhs_col_var = std::dynamic_pointer_cast<const Analyzer::ColumnVar>(rhs);
  auto lhs_col_var = std::dynamic_pointer_cast<const Analyzer::ColumnVar>(lhs);
  std::shared_ptr<const Analyzer::UOper> cast_oper;
  std::shared_ptr<const Analyzer::ColumnVar> col_var;
  auto compare_opr = compare_operator;
  if (lhs_col_var && rhs_col_var) {
    if (lhs_col_var->get_type_info().get_comp_param() ==
        rhs_col_var->get_type_info().get_comp_param()) {
      if (compare_operator == kEQ || compare_operator == kNE) {
        // TODO (vraj): implement compare between two dictionary encoded columns which
        // share a dictionary
        return nullptr;
      }
    }
    // TODO (vraj): implement compare between two dictionary encoded columns which don't
    // shared dictionary
    CIDER_THROW(CiderCompileException,
                "Decoding two Dictionary encoded columns will be slow");
  } else if (lhs_col_var && rhs_cast_oper) {
    cast_oper.swap(rhs_cast_oper);
    col_var.swap(lhs_col_var);
  } else if (lhs_cast_oper && rhs_col_var) {
    cast_oper.swap(lhs_cast_oper);
    col_var.swap(rhs_col_var);
    switch (compare_operator) {
      case kLT:
        compare_opr = kGT;
        break;
      case kLE:
        compare_opr = kGE;
        break;
      case kGT:
        compare_opr = kLT;
        break;
      case kGE:
        compare_opr = kLE;
      default:
        break;
    }
  }
  if (!cast_oper || !col_var) {
    return nullptr;
  }
  CHECK_EQ(kCAST, cast_oper->get_optype());

  const auto const_expr =
      dynamic_cast<Analyzer::Constant*>(cast_oper->get_own_operand().get());
  if (!const_expr) {
    // Analyzer casts dictionary encoded columns to none encoded if there is a comparison
    // between two encoded columns. Which we currently do not handle.
    return nullptr;
  }
  const auto& const_val = const_expr->get_constval();

  const auto col_ti = col_var->get_type_info();
  CHECK(col_ti.is_string());
  CHECK_EQ(kENCODING_DICT, col_ti.get_compression());
  const auto sdp = executor()->getStringDictionaryProxy(
      col_ti.get_comp_param(), executor()->getRowSetMemoryOwner(), true);

  if (sdp->storageEntryCount() > 200000000) {
    CIDER_THROW(CiderCompileException, "Cardinality for string dictionary is too high");
  }

  const auto& pattern_str = *const_val.stringval;
  const auto matching_ids = get_compared_ids(sdp, compare_opr, pattern_str);

  // InIntegerSet requires 64-bit values
  std::vector<int64_t> matching_ids_64(matching_ids.size());
  std::copy(matching_ids.begin(), matching_ids.end(), matching_ids_64.begin());

  const auto in_values = std::make_shared<Analyzer::InIntegerSet>(
      col_var, matching_ids_64, col_ti.get_notnull());
  return codegen(in_values.get(), co);
}

llvm::Value* CodeGenerator::codegen(const Analyzer::RegexpExpr* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (is_unnest(extract_cast_arg(expr->get_arg()))) {
    CIDER_THROW(CiderCompileException, "REGEXP not supported for unnested expressions");
  }
  char escape_char{'\\'};
  if (expr->get_escape_expr()) {
    auto escape_char_expr =
        dynamic_cast<const Analyzer::Constant*>(expr->get_escape_expr());
    CHECK(escape_char_expr);
    CHECK(escape_char_expr->get_type_info().is_string());
    CHECK_EQ(size_t(1), escape_char_expr->get_constval().stringval->size());
    escape_char = (*escape_char_expr->get_constval().stringval)[0];
  }
  auto pattern = dynamic_cast<const Analyzer::Constant*>(expr->get_pattern_expr());
  CHECK(pattern);
  auto fast_dict_pattern_lv =
      codegenDictRegexp(expr->get_own_arg(), pattern, escape_char, co);
  if (fast_dict_pattern_lv) {
    return fast_dict_pattern_lv;
  }
  const auto& ti = expr->get_arg()->get_type_info();
  CHECK(ti.is_string());
  if (g_enable_watchdog && ti.get_compression() != kENCODING_NONE) {
    CIDER_THROW(CiderWatchdogException,
                "Cannot do REGEXP_LIKE on this dictionary encoded column, its "
                "cardinality is too high");
  }
  auto str_lv = codegen(expr->get_arg(), true, co);
  if (str_lv.size() != 3) {
    CHECK_EQ(size_t(1), str_lv.size());
    str_lv.push_back(cgen_state_->emitCall("extract_str_ptr", {str_lv.front()}));
    str_lv.push_back(cgen_state_->emitCall("extract_str_len", {str_lv.front()}));
  }
  auto regexp_expr_arg_lvs = codegen(expr->get_pattern_expr(), true, co);
  CHECK_EQ(size_t(3), regexp_expr_arg_lvs.size());
  const bool is_nullable{!expr->get_arg()->get_type_info().get_notnull()};
  std::vector<llvm::Value*> regexp_args{
      str_lv[1], str_lv[2], regexp_expr_arg_lvs[1], regexp_expr_arg_lvs[2]};
  std::string fn_name("regexp_like");
  regexp_args.push_back(cgen_state_->llInt(int8_t(escape_char)));
  if (is_nullable) {
    fn_name += "_nullable";
    regexp_args.push_back(cgen_state_->inlineIntNull(expr->get_type_info()));
    return cgen_state_->emitExternalCall(
        fn_name, get_int_type(8, cgen_state_->context_), regexp_args);
  }
  return cgen_state_->emitExternalCall(
      fn_name, get_int_type(1, cgen_state_->context_), regexp_args);
}

llvm::Value* CodeGenerator::codegenDictRegexp(
    const std::shared_ptr<Analyzer::Expr> pattern_arg,
    const Analyzer::Constant* pattern,
    const char escape_char,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto cast_oper = std::dynamic_pointer_cast<Analyzer::UOper>(pattern_arg);
  if (!cast_oper) {
    return nullptr;
  }
  CHECK(cast_oper);
  CHECK_EQ(kCAST, cast_oper->get_optype());
  const auto dict_regexp_arg = cast_oper->get_own_operand();
  const auto& dict_regexp_arg_ti = dict_regexp_arg->get_type_info();
  CHECK(dict_regexp_arg_ti.is_string());
  CHECK_EQ(kENCODING_DICT, dict_regexp_arg_ti.get_compression());
  const auto comp_param = dict_regexp_arg_ti.get_comp_param();
  const auto sdp = executor()->getStringDictionaryProxy(
      comp_param, executor()->getRowSetMemoryOwner(), true);
  if (sdp->storageEntryCount() > 15000000) {
    return nullptr;
  }
  const auto& pattern_ti = pattern->get_type_info();
  CHECK(pattern_ti.is_string());
  CHECK_EQ(kENCODING_NONE, pattern_ti.get_compression());
  const auto& pattern_datum = pattern->get_constval();
  const auto& pattern_str = *pattern_datum.stringval;
  const auto matching_ids = sdp->getRegexpLike(pattern_str, escape_char);
  // InIntegerSet requires 64-bit values
  std::vector<int64_t> matching_ids_64(matching_ids.size());
  std::copy(matching_ids.begin(), matching_ids.end(), matching_ids_64.begin());
  const auto in_values = std::make_shared<Analyzer::InIntegerSet>(
      dict_regexp_arg, matching_ids_64, dict_regexp_arg_ti.get_notnull());
  return codegen(in_values.get(), co);
}
