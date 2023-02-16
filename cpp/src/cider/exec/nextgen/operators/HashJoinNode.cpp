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
#include "exec/nextgen/operators/HashJoinNode.h"

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "type/plan/Expr.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;
using namespace context;

constexpr auto left_table_id = 100;

// TODO(qiuyang): will extract as a util class and migrate ColumnToRowReader
class BuildTableReader {
 public:
  BuildTableReader(utils::JITExprValue& buffer_values,
                   ExprPtr& expr,
                   JITValuePointer& index)
      : buffer_values_(buffer_values), expr_(expr), index_(index) {}

  void read() {
    switch (expr_->get_type_info().get_type()) {
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
      case kDATE:
      case kTIME:
      case kTIMESTAMP:
        readFixSizedTypeCol();
        break;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        readVariableSizeTypeCol();
        break;
      default:
        LOG(ERROR) << "Unsupported data type in BuildTableReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void readVariableSizeTypeCol() {
    utils::VarSizeJITExprValue varsize_values(buffer_values_);
    auto data_buffer = varsize_values.getValue();

    auto& func = data_buffer->getParentJITFunction();
    // offset buffer
    auto offset_pointer =
        varsize_values.getLength()->castPointerSubType(JITTypeTag::INT32);
    auto len = offset_pointer[index_ + 1] - offset_pointer[index_];
    auto cur_offset = offset_pointer[index_];
    // data buffer
    auto value_pointer = data_buffer->castPointerSubType(JITTypeTag::INT8);
    auto row_data = value_pointer + cur_offset;  // still char*

    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), len, row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{varsize_values.getNull().get(), index_.get()}}});
      expr_->set_expr_value(row_null_data, len, row_data);
    }
  }

  void readFixSizedTypeCol() {
    utils::FixSizeJITExprValue fixsize_values(buffer_values_);
    auto data_buffer = fixsize_values.getValue();
    auto& func = data_buffer->getParentJITFunction();
    auto row_data = getFixSizeRowData(func, fixsize_values);
    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_values.getNull().get(), index_.get()}}});

      expr_->set_expr_value(row_null_data, row_data);
    }
  }

  JITValuePointer getFixSizeRowData(JITFunction& func,
                                    utils::FixSizeJITExprValue& fixsize_val) {
    if (expr_->get_type_info().get_type() == kBOOLEAN) {
      auto row_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_set",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_val.getValue().get(), index_.get()}}});
      return row_data;
    } else {
      JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
      // data buffer decoder
      auto data_pointer = fixsize_val.getValue()->castPointerSubType(tag);
      auto row_data = data_pointer[index_];
      return row_data;
    }
  }

 private:
  utils::JITExprValue& buffer_values_;
  ExprPtr& expr_;
  JITValuePointer index_;
};

// TODO(qiuyang): extract as a util class
class ExprDefaultValueSetter {
 public:
  ExprDefaultValueSetter(ExprPtr& expr,
                         CodegenContext& context,
                         std::map<ExprPtr, std::vector<JITValuePointer>>& expr_map)
      : expr_(expr), context_(context), expr_map_(expr_map) {}

  void setDefault() {
    switch (expr_->get_type_info().get_type()) {
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
      case kDATE:
      case kTIME:
      case kTIMESTAMP:
        setDefalutFixSizedTypeCol();
        break;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        setDefalutVariableSizeTypeCol();
        break;
      default:
        LOG(ERROR) << "Unsupported data type in BuildTableReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void setDefalutFixSizedTypeCol() {
    auto func = context_.getJITFunction();
    JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
    auto null_init = func->createVariable(JITTypeTag::BOOL, "fixed_null_init", true);
    auto val_init = func->createVariable(tag, "fixed_val_init", 0);
    *null_init = func->createLiteral(JITTypeTag::BOOL, true);
    *val_init = func->createLiteral(tag, 0l);
    expr_->set_expr_value(null_init, val_init);
    expr_map_.insert({expr_, {null_init, val_init}});
  }

  void setDefalutVariableSizeTypeCol() {
    // FIXME(qiuyang): how to set VariableSizeTypeCol default value
    auto func = context_.getJITFunction();
    JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
    auto null_init = func->createVariable(JITTypeTag::BOOL, "variable_null_init", true);
    auto val_init = func->createVariable(tag, "variable_val_init", 'a');
    auto length_init = func->createVariable(JITTypeTag::INT32, "length_init", 0);
    *null_init = func->createLiteral(JITTypeTag::BOOL, true);
    *length_init = func->createLiteral(JITTypeTag::INT32, 0);
    *val_init = func->createStringLiteral("");
    expr_->set_expr_value(null_init, length_init, val_init);
    expr_map_.insert({expr_, {null_init, length_init, val_init}});
  }

 private:
  ExprPtr& expr_;
  context::CodegenContext& context_;
  std::map<ExprPtr, std::vector<JITValuePointer>>& expr_map_;
};

TranslatorPtr HashJoinNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<HashJoinTranslator>(shared_from_this(), succ);
}

void HashJoinTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

// traverse join_quals expr tree to get join key value and null vector
void traverse(ExprPtr expr,
              jitlib::JITFunctionPointer func,
              std::vector<JITValuePointer>& keys,
              std::vector<JITValuePointer>& nulls,
              context::CodegenContext& context) {
  if (Analyzer::ColumnVar* col_var = dynamic_cast<Analyzer::ColumnVar*>(expr.get())) {
    // FIXME (qiuyang):: 100 is not always used as left table id.
    if (col_var->get_table_id() == left_table_id) {
      // TODO(qiuyang): hashjoin only support FixSizetype join key now
      utils::FixSizeJITExprValue jit_value(expr->codegen(context));
      keys.emplace_back(jit_value.getValue());
      // null vector handle
      if (!expr->get_type_info().get_notnull()) {
        nulls.emplace_back(jit_value.getNull());
      }
    }
  } else {
    auto children = expr->get_children_reference();
    for (auto child : children) {
      traverse(*child, func, keys, nulls, context);
    }
  }
}

void joinProbe(context::CodegenContext& context,
               jitlib::JITFunctionPointer& func,
               JITValuePointer& join_res_buffer,
               JITValuePointer& row_index,
               JITValuePointer& join_res_len,
               std::map<ExprPtr, size_t>& build_table_map,
               JoinType& join_type,
               const std::map<ExprPtr, std::vector<JITValuePointer>>& expr_map =
                   std::map<ExprPtr, std::vector<JITValuePointer>>()) {
  auto res_array = func->emitRuntimeFunctionCall(
      "extract_join_res_array",
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::POINTER,
          .ret_sub_type = JITTypeTag::INT8,
          .params_vector = {join_res_buffer.get(), row_index.get()}});

  auto res_row_id = func->emitRuntimeFunctionCall(
      "extract_join_row_id",
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {join_res_buffer.get(), row_index.get()}});

  std::map<ExprPtr, size_t>::iterator iter;
  for (iter = build_table_map.begin(); iter != build_table_map.end(); iter++) {
    auto expr = iter->first;
    auto build_idx = func->createLiteral(JITTypeTag::INT64, iter->second);
    auto child_arrow_array = func->emitRuntimeFunctionCall(
        "extract_arrow_array_child",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .ret_sub_type = JITTypeTag::VOID,
                                  .params_vector = {res_array.get(), build_idx.get()}});

    int64_t buffer_num = utils::getBufferNum(expr->get_type_info().get_type());
    utils::JITExprValue buffer_values(buffer_num, JITExprValueType::BATCH);
    for (int64_t i = 0; i < buffer_num; ++i) {
      auto buffer_idx = func->createLiteral(JITTypeTag::INT64, i);
      auto array_buffer = func->emitRuntimeFunctionCall(
          "extract_arrow_array_buffer",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::POINTER,
              .ret_sub_type = JITTypeTag::VOID,
              .params_vector = {child_arrow_array.get(), buffer_idx.get()}});

      buffer_values.append(array_buffer);
    }
    BuildTableReader reader(buffer_values, expr, res_row_id);
    reader.read();

    // in LLVM code, two parallel basicBlock cannot call each other's internally defined
    // local variables, so the pre-defined variables are used here to get the result after
    // read
    if (join_type == JoinType::LEFT) {
      auto& expr_val = expr->get_expr_value();
      auto vector = expr_map.at(expr);
      for (int i = 0; i < vector.size(); ++i) {
        vector[i] = *expr_val[i];
        expr_val[i].replace(vector[i]);
      }
    }
  }
}

void HashJoinTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto join_quals = dynamic_cast<HashJoinNode*>(node_.get())->getJoinQuals();

  std::vector<JITValuePointer> keys;
  std::vector<JITValuePointer> nulls;
  for (int i = 0; i < join_quals.size(); ++i) {
    traverse(join_quals[i], func, keys, nulls, context);
  }

  // open up a section of buffer to reserve the join result
  auto join_res_buffer = context.registerBuffer(
      0,
      "join_res_buffer",
      [](context::Buffer* buf) { memset(buf->getBuffer(), 0, buf->getCapacity()); },
      false);

  // pack join key values(support only one key now)
  auto key_value = func->packJITValues<8>(keys);
  // pack join key nulls
  auto key_null = func->packJITValues<1>(nulls);
  // register hashtable
  auto hashtable = context.registerHashTable("hash_table");

  // TODO(qiuyang) : hashtable will be a base class pointer
  auto join_res_len = func->emitRuntimeFunctionCall(
      "look_up_value_by_key",
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {
              hashtable.get(), key_value.get(), key_null.get(), join_res_buffer.get()}});

  auto build_table_map = dynamic_cast<HashJoinNode*>(node_.get())->getBuildTableMap();
  auto row_index = func->createVariable(JITTypeTag::INT64, "row_index", 0l);
  *row_index = func->createLiteral(JITTypeTag::INT64, 0l);
  auto join_type = dynamic_cast<HashJoinNode*>(node_.get())->getJoinType();
  std::map<ExprPtr, size_t>::iterator iter;

  auto loop_builder = func->createLoopBuilder();

  switch (join_type) {
    case JoinType::INNER:
      loop_builder
          ->condition([&row_index, &join_res_len]() { return row_index < join_res_len; })
          ->loop([&](LoopBuilder*) {
            joinProbe(context,
                      func,
                      join_res_buffer,
                      row_index,
                      join_res_len,
                      build_table_map,
                      join_type);
          })
          ->update([&]() {
            successor_->consume(context);
            row_index = row_index + 1l;
          })
          ->build();
      break;
    case JoinType::LEFT:
      func->createIfBuilder()
          ->condition([&]() { return join_res_len != 0l; })
          ->ifTrue([&] { row_index = row_index + 1l; })
          ->build();
      loop_builder
          ->condition([&row_index, &join_res_len]() { return row_index <= join_res_len; })
          ->loop([&](LoopBuilder*) {
            context.setHasOuterJoin(true);
            std::map<ExprPtr, std::vector<JITValuePointer>> expr_map;
            for (iter = build_table_map.begin(); iter != build_table_map.end(); iter++) {
              auto expr = iter->first;
              ExprDefaultValueSetter setter(expr, context, expr_map);
              setter.setDefault();
            }

            loop_builder->loopContinue(join_res_len == 0l);
            row_index = row_index - 1l;
            joinProbe(context,
                      func,
                      join_res_buffer,
                      row_index,
                      join_res_len,
                      build_table_map,
                      join_type,
                      expr_map);
            row_index = row_index + 1l;
          })
          ->update([&]() {
            successor_->consume(context);
            row_index = row_index + 1l;
          })
          ->build();
      break;
    default:
      break;
  }
}
}  // namespace cider::exec::nextgen::operators
