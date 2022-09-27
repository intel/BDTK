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

#include "CiderAggregateCodeGenerator.h"
#include "exec/template/AutomaticIRMetadataGuard.h"
#include "type/plan/Analyzer.h"
#include "util/SqlTypesLayout.h"

llvm::Value* AggregateCodeGenerator::castToIntPtrTyIn(llvm::Value* val,
                                                      const size_t width) const {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(val->getType()->isPointerTy());

  const auto val_ptr_type = static_cast<llvm::PointerType*>(val->getType());
  const auto val_type = val_ptr_type->getElementType();
  size_t val_width = 0;
  if (val_type->isIntegerTy()) {
    val_width = val_type->getIntegerBitWidth();
  } else {
    if (val_type->isFloatTy()) {
      val_width = 32;
    } else {
      CHECK(val_type->isDoubleTy());
      val_width = 64;
    }
  }
  CHECK_LT(size_t(0), val_width);
  if (width == val_width) {
    return val;
  }
  return cgen_state_->ir_builder_.CreateBitCast(
      val, llvm::PointerType::get(get_int_type(width, cgen_state_->context_), 0));
}

std::unique_ptr<AggregateCodeGenerator> SimpleAggregateCodeGenerator::Make(
    const std::string& base_fname,
    TargetInfo target_info,
    size_t slot_size,
    CgenState* cgen_state) {
  CHECK(cgen_state);
  auto generator = std::make_unique<SimpleAggregateCodeGenerator>();

  generator->base_fname_ = "cider_" + base_fname;
  generator->target_info_ = target_info;
  generator->slot_size_ = slot_size;
  generator->cgen_state_ = cgen_state;

  auto target_type = get_compact_type(target_info);
  auto arg_type = target_info.is_agg ? target_info.agg_arg_type : target_type;

  CHECK(target_info.is_agg || base_fname == "agg_id");
  if (target_info.is_agg) {
    CHECK(kAVG == target_info.agg_kind || kMIN == target_info.agg_kind ||
          kMAX == target_info.agg_kind || kSUM == target_info.agg_kind);
  }

  generator->target_width_ = get_bit_width(target_info.sql_type);
  generator->arg_width_ = target_info.is_agg ? get_bit_width(target_info.agg_arg_type)
                                             : generator->target_width_;
  CHECK(generator->arg_width_ <= generator->target_width_);
  CHECK(generator->target_width_ <= (slot_size << 3));

  if (target_type.is_fp()) {
    // Check float_float case.
    CHECK(arg_type.is_fp());
    if (32 == generator->arg_width_ && 32 == generator->target_width_ && 8 == slot_size) {
      generator->is_float_float_ = true;
      generator->base_fname_ += "_floatSpec";
    } else {
      switch (generator->target_width_) {
        case 32:
          generator->base_fname_ += "_float";
          break;
        case 64:
          generator->base_fname_ += "_double";
          break;
      }
      generator->is_float_float_ = false;
    }
    generator->target_width_ = (slot_size << 3);
  } else if (target_type.is_integer()) {
    generator->is_float_float_ = false;
    // Integer round up to slot size.
    generator->target_width_ = (slot_size << 3);
    switch (generator->target_width_) {
      case 32:
        generator->base_fname_ += "_int32";
        break;
      case 64:
        generator->base_fname_ += "_int64";
        break;
    }
  } else {
    CIDER_THROW(CiderCompileException,
                "Unsuppored data type for SimpleAggregateCodeGenerator.");
  }

  if (target_info.skip_null_val) {
    generator->base_fname_ += "_nullable";
  }

  return generator;
}

void SimpleAggregateCodeGenerator::codegen(CodegenColValues* input,
                                           llvm::Value* output_buffer,
                                           llvm::Value* index,
                                           llvm::Value* output_null_buffer) const {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(output_buffer);
  CHECK(index);

  FixedSizeColValues* args = dynamic_cast<FixedSizeColValues*>(input);
  if (nullptr == args) {
    CIDER_THROW(CiderCompileException,
                "SimpleAggregateCodeGenerator only support fixed-sized data now.");
  }
  auto value = args->getValue(), is_null = args->getNull();
  if (!is_float_float_) {
    value = cgen_state_->castToTypeIn(value, target_width_);
  }
  output_buffer = castToIntPtrTyIn(output_buffer, target_width_);
  index = cgen_state_->castToTypeIn(index, 64);

  std::vector<llvm::Value*> fun_args = {output_buffer, index, value};

  if (target_info_.skip_null_val) {
    CHECK(output_null_buffer);
    CHECK(is_null);
    output_null_buffer = castToIntPtrTyIn(output_null_buffer, 8);
    fun_args.push_back(output_null_buffer);
    fun_args.push_back(is_null);
  }

  cgen_state_->emitCall(base_fname_, fun_args);
}

std::unique_ptr<AggregateCodeGenerator> ProjectIDCodeGenerator::Make(
    const std::string& base_fname,
    TargetInfo target_info,
    CgenState* cgen_state) {
  CHECK(cgen_state);
  CHECK(base_fname == "agg_id");

  auto generator = std::make_unique<ProjectIDCodeGenerator>();

  generator->base_fname_ = "cider_" + base_fname + "_proj";
  generator->target_info_ = target_info;
  generator->cgen_state_ = cgen_state;

  generator->target_width_ = get_bit_width(target_info.sql_type);
  generator->slot_size_ = (generator->target_width_ >> 3);
  generator->arg_width_ = generator->target_width_;

  if (target_info.sql_type.is_fp()) {
    // Check float_float case.
    switch (generator->target_width_) {
      case 32:
        generator->base_fname_ += "_float";
        break;
      case 64:
        generator->base_fname_ += "_double";
        break;
    }
  } else if (target_info.sql_type.is_integer()) {
    switch (generator->target_width_) {
      case 8:
        generator->base_fname_ += "_int8";
        break;
      case 16:
        generator->base_fname_ += "_int16";
        break;
      case 32:
        generator->base_fname_ += "_int32";
        break;
      case 64:
        generator->base_fname_ += "_int64";
        break;
    }
  } else {
    throw std::runtime_error("Unsuppored data type for ProjectIDCodeGenerator.");
  }

  if (target_info.skip_null_val) {
    generator->base_fname_ += "_nullable";
  }

  return generator;
}

std::unique_ptr<AggregateCodeGenerator> CountAggregateCodeGenerator::Make(
    const std::string& base_fname,
    TargetInfo target_info,
    size_t slot_size,
    CgenState* cgen_state,
    bool has_arg) {
  CHECK(cgen_state);
  auto generator = std::make_unique<CountAggregateCodeGenerator>();

  generator->base_fname_ = "cider_" + base_fname;
  generator->target_info_ = target_info;
  generator->slot_size_ = slot_size;
  generator->cgen_state_ = cgen_state;
  generator->has_arg_ = has_arg;

  auto target_type = get_compact_type(target_info);

  CHECK(target_info.is_agg);
  CHECK(kCOUNT == target_info.agg_kind || kAVG == target_info.agg_kind);

  generator->target_width_ = get_bit_width(target_info.sql_type);
  CHECK(generator->target_width_ <= (slot_size << 3));
  CHECK(target_type.is_integer() || kAVG == target_info.agg_kind);

  if (!target_info.is_distinct) {
    // Integer round up to slot size.
    generator->target_width_ = (slot_size << 3);
    switch (generator->target_width_) {
      case 32:
        generator->base_fname_ += "_int32";
        break;
      case 64:
        generator->base_fname_ += "_int64";
        break;
    }

    if (target_info.skip_null_val && has_arg) {
      generator->base_fname_ += "_nullable";
    }
  } else {
    CIDER_THROW(CiderCompileException, "Distinct COUNT is not supported now.");
  }

  return generator;
}

void CountAggregateCodeGenerator::codegen(CodegenColValues* input,
                                          llvm::Value* output_buffer,
                                          llvm::Value* index,
                                          llvm::Value* output_null_buffer) const {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(output_buffer);
  CHECK(index);

  std::vector<llvm::Value*> func_args = {castToIntPtrTyIn(output_buffer, target_width_),
                                         cgen_state_->castToTypeIn(index, 64)};
  if (!target_info_.is_distinct) {
    if (target_info_.skip_null_val && has_arg_) {
      if (NullableColValues* args = dynamic_cast<NullableColValues*>(input)) {
        if (auto is_null = args->getNull()) {
          func_args.push_back(is_null);
        } else {
          CIDER_THROW(CiderCompileException, "Argument of Count is nullptr.");
        }
      } else {
        CIDER_THROW(CiderCompileException,
                    "Argument of Count is not a NullableColValues.");
      }
    }
    cgen_state_->emitCall(base_fname_, func_args);
  } else {
    CIDER_THROW(CiderCompileException, "Distinct COUNT is not supported now.");
  }
}
