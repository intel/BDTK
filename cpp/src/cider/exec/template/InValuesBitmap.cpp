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

#include "InValuesBitmap.h"
#include "CodeGenerator.h"
#include "Execute.h"
#include "GroupByAndAggregate.h"
#include "exec/plan/parser/ParserNode.h"
#include "function/scalar/RuntimeFunctions.h"
#include "util/Logger.h"
#include "util/checked_alloc.h"

#include <boost/multiprecision/cpp_int.hpp>
#include <limits>

using checked_int64_t = boost::multiprecision::number<
    boost::multiprecision::cpp_int_backend<64,
                                           64,
                                           boost::multiprecision::signed_magnitude,
                                           boost::multiprecision::checked,
                                           void>>;

InValuesBitmap::InValuesBitmap(const std::vector<int64_t>& values,
                               const int64_t null_val,
                               const Data_Namespace::MemoryLevel memory_level,
                               const int device_count,
                               BufferProvider* buffer_provider)
    : rhs_has_null_(false)
    , null_val_(null_val)
    , memory_level_(memory_level)
    , device_count_(device_count)
    , buffer_provider_(buffer_provider) {
  CHECK_EQ(Data_Namespace::CPU_LEVEL, memory_level_);
  if (values.empty()) {
    return;
  }
  min_val_ = std::numeric_limits<int64_t>::max();
  max_val_ = std::numeric_limits<int64_t>::min();
  for (const auto value : values) {
    if (value == null_val) {
      rhs_has_null_ = true;
      continue;
    }
    if (value < min_val_) {
      min_val_ = value;
    }
    if (value > max_val_) {
      max_val_ = value;
    }
  }
  if (max_val_ < min_val_) {
    CHECK_EQ(std::numeric_limits<int64_t>::max(), min_val_);
    CHECK_EQ(std::numeric_limits<int64_t>::min(), max_val_);
    CHECK(rhs_has_null_);
    return;
  }
  const int64_t MAX_BITMAP_BITS{8 * 1000 * 1000 * 1000LL};
  const auto bitmap_sz_bits =
      static_cast<int64_t>(checked_int64_t(max_val_) - min_val_ + 1);
  if (bitmap_sz_bits > MAX_BITMAP_BITS) {
    CIDER_THROW(CiderCompileException, "Failed To Create Bitmap");
  }
  const auto bitmap_sz_bytes = bitmap_bits_to_bytes(bitmap_sz_bits);
  auto cpu_bitset = static_cast<int8_t*>(checked_calloc(bitmap_sz_bytes, 1));
  for (const auto value : values) {
    if (value == null_val) {
      continue;
    }
    agg_count_distinct_bitmap(reinterpret_cast<int64_t*>(&cpu_bitset), value, min_val_);
  }
  CHECK_EQ(1, device_count_);
  bitsets_.push_back(cpu_bitset);
}

InValuesBitmap::~InValuesBitmap() {
  if (bitsets_.empty()) {
    return;
  }
  CHECK_EQ(size_t(1), bitsets_.size());
  free(bitsets_.front());
}

std::unique_ptr<CodegenColValues> InValuesBitmap::codegen(llvm::Value* needle,
                                                          llvm::Value* null,
                                                          Executor* executor) const {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  std::vector<std::shared_ptr<const Analyzer::Constant>> constants_owned;
  std::vector<const Analyzer::Constant*> constants;
  for (const auto bitset : bitsets_) {
    const int64_t bitset_handle = reinterpret_cast<int64_t>(bitset);
    const auto bitset_handle_literal = std::dynamic_pointer_cast<Analyzer::Constant>(
        Parser::IntLiteral::analyzeValue(bitset_handle));
    CHECK(bitset_handle_literal);
    CHECK_EQ(kENCODING_NONE, bitset_handle_literal->get_type_info().get_compression());
    constants_owned.push_back(bitset_handle_literal);
    constants.push_back(bitset_handle_literal.get());
  }
  const auto needle_i64 = executor->cgen_state_->castToTypeIn(needle, 64);
  if (bitsets_.empty()) {
    return std::make_unique<FixedSizeColValues>(
        executor->cgen_state_->emitCall("bit_is_set_cider",
                                        {executor->cgen_state_->llInt(int64_t(0)),
                                         needle_i64,
                                         executor->cgen_state_->llInt(int64_t(0)),
                                         executor->cgen_state_->llInt(int64_t(0))}),
        null);
  }
  CodeGenerator code_generator(executor);
  const auto bitset_handle_lvs =
      code_generator.codegenHoistedConstants(constants, kENCODING_NONE, 0);
  CHECK_EQ(size_t(1), bitset_handle_lvs.size());
  return std::make_unique<FixedSizeColValues>(
      executor->cgen_state_->emitCall(
          "bit_is_set_cider",
          {executor->cgen_state_->castToTypeIn(bitset_handle_lvs.front(), 64),
           needle_i64,
           executor->cgen_state_->llInt(min_val_),
           executor->cgen_state_->llInt(max_val_)}),
      null);
}

llvm::Value* InValuesBitmap::codegen(llvm::Value* needle, Executor* executor) const {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  std::vector<std::shared_ptr<const Analyzer::Constant>> constants_owned;
  std::vector<const Analyzer::Constant*> constants;
  for (const auto bitset : bitsets_) {
    const int64_t bitset_handle = reinterpret_cast<int64_t>(bitset);
    const auto bitset_handle_literal = std::dynamic_pointer_cast<Analyzer::Constant>(
        Parser::IntLiteral::analyzeValue(bitset_handle));
    CHECK(bitset_handle_literal);
    CHECK_EQ(kENCODING_NONE, bitset_handle_literal->get_type_info().get_compression());
    constants_owned.push_back(bitset_handle_literal);
    constants.push_back(bitset_handle_literal.get());
  }
  const auto needle_i64 = executor->cgen_state_->castToTypeIn(needle, 64);
  const auto null_bool_val =
      static_cast<int8_t>(inline_int_null_val(SQLTypeInfo(kBOOLEAN, false)));
  if (bitsets_.empty()) {
    return executor->cgen_state_->emitCall("bit_is_set",
                                           {executor->cgen_state_->llInt(int64_t(0)),
                                            needle_i64,
                                            executor->cgen_state_->llInt(int64_t(0)),
                                            executor->cgen_state_->llInt(int64_t(0)),
                                            executor->cgen_state_->llInt(null_val_),
                                            executor->cgen_state_->llInt(null_bool_val)});
  }
  CodeGenerator code_generator(executor);
  const auto bitset_handle_lvs =
      code_generator.codegenHoistedConstants(constants, kENCODING_NONE, 0);
  CHECK_EQ(size_t(1), bitset_handle_lvs.size());
  return executor->cgen_state_->emitCall(
      "bit_is_set",
      {executor->cgen_state_->castToTypeIn(bitset_handle_lvs.front(), 64),
       needle_i64,
       executor->cgen_state_->llInt(min_val_),
       executor->cgen_state_->llInt(max_val_),
       executor->cgen_state_->llInt(null_val_),
       executor->cgen_state_->llInt(null_bool_val)});
}

bool InValuesBitmap::isEmpty() const {
  return bitsets_.empty();
}

bool InValuesBitmap::hasNull() const {
  return rhs_has_null_;
}
