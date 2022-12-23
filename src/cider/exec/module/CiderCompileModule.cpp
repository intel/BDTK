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

#include "cider/CiderCompileModule.h"
#include <memory>
#include "CiderCompilationResultImpl.h"
#include "cider/batch/ScalarBatch.h"
#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/template/Execute.h"
#include "type/schema/CiderSchemaProvider.h"
#include "util/measure.h"
#include "util/memory/CiderArrowDataProvider.h"
#include "util/memory/CiderBatchDataProvider.h"

namespace {

CompilationOptions CiderCompilationOptionToCo(CiderCompilationOption& cco) {
  auto co = CompilationOptions::defaults();
  co.hoist_literals = cco.hoist_literals;
  co.with_dynamic_watchdog = cco.with_dynamic_watchdog;
  co.allow_lazy_fetch = cco.allow_lazy_fetch;
  co.filter_on_deleted_column = cco.filter_on_deleted_column;
  co.use_cider_groupby_hash = cco.use_cider_groupby_hash;
  co.use_default_col_range = cco.use_default_col_range;
  co.use_cider_data_format = cco.use_cider_data_format;
  co.needs_error_check = cco.needs_error_check;
  co.use_nextgen_compiler = cco.use_nextgen_compiler;

  return co;
}

ExecutionOptions CiderExecutionOptionToEo(CiderExecutionOption& ceo) {
  auto eo = ExecutionOptions::defaults();
  eo.output_columnar_hint = ceo.output_columnar_hint;
  eo.allow_multifrag = ceo.allow_multifrag;
  eo.just_explain = ceo.just_explain;
  eo.allow_loop_joins = ceo.allow_loop_joins;
  eo.with_watchdog = ceo.with_watchdog;
  eo.jit_debug = ceo.jit_debug;
  eo.just_validate = ceo.just_validate;
  eo.with_dynamic_watchdog = ceo.with_dynamic_watchdog;
  eo.dynamic_watchdog_time_limit = ceo.dynamic_watchdog_time_limit;
  eo.find_push_down_candidates = ceo.find_push_down_candidates;
  eo.just_calcite_explain = ceo.just_calcite_explain;
  eo.allow_runtime_query_interrupt = ceo.allow_runtime_query_interrupt;
  eo.running_query_interrupt_freq = ceo.running_query_interrupt_freq;
  eo.pending_query_interrupt_freq = ceo.pending_query_interrupt_freq;

  return eo;
}
}  // namespace

CiderCompilationResult::CiderCompilationResult() {
  impl_.reset(new Impl());
}

CiderCompilationResult::~CiderCompilationResult() {}

std::string CiderCompilationResult::getIR() const {
  return impl_->getIR();
}

std::vector<int8_t> CiderCompilationResult::getHoistLiteral() const {
  return impl_->getHoistLiteral();
}

void* CiderCompilationResult::func() const {
  return impl_->func();
}

std::shared_ptr<CiderTableSchema> CiderCompilationResult::getOutputCiderTableSchema()
    const {
  return impl_->getOutputCiderTableSchema();
}

QueryType CiderCompilationResult::getQueryType() const {
  return impl_->getQueryType();
}

class CiderCompileModule::Impl {
 public:
  explicit Impl(std::shared_ptr<CiderAllocator> allocator) : allocator_(allocator) {
    id_ = std::hash<std::thread::id>{}(std::this_thread::get_id());
    executor_ = Executor::getExecutor(id_, nullptr, nullptr);
    ciderStringDictionaryProxy_ = initStringDictionaryProxy(allocator);
    executor_->setCiderStringDictionaryProxy(ciderStringDictionaryProxy_.get());
  }
  ~Impl() {}

  std::shared_ptr<CiderCompilationResult> compile(const substrait::Plan& plan,
                                                  CiderCompilationOption cco,
                                                  CiderExecutionOption ceo) {
    INJECT_TIMER(CiderCompileModule_Compile);
    auto co = CiderCompilationOptionToCo(cco);
    auto eo = CiderExecutionOptionToEo(ceo);
    translator_ = std::make_shared<generator::SubstraitToRelAlgExecutionUnit>(plan);
    ra_exe_unit_ =
        std::make_shared<RelAlgExecutionUnit>(translator_->createRelAlgExecutionUnit());

    auto ciderCompilationResult = std::make_shared<CiderCompilationResult>();
    ciderCompilationResult->impl_->co = co;
    ciderCompilationResult->impl_->rel_alg_exe_unit_ = ra_exe_unit_;
    ciderCompilationResult->impl_->outputSchema_ =
        translator_->getOutputCiderTableSchema();

    if (co.use_nextgen_compiler) {
      cider::jitlib::CompilationOptions jit_co;
      jit_co.needs_error_check = co.needs_error_check;
      jit_co.dump_ir = true;
      ciderCompilationResult->impl_->codegen_ctx_ =
          cider::exec::nextgen::compile(*ra_exe_unit_, jit_co);
      return ciderCompilationResult;
    }

    // if this is a join query and don't feed a valid build table, throw exception
    if (!ra_exe_unit_->join_quals.empty()) {
      if (co.use_cider_data_format &&
          (build_table_.isMoved() || build_table_.getChildrenNum() == 0)) {
        CIDER_THROW(CiderCompileException, "Join query must feed a valid build table!");
      } else if (!co.use_cider_data_format && build_table_.row_num() == 0) {
        CIDER_THROW(CiderCompileException, "Join query must feed a valid build table!");
      }
    }
    if (co.use_default_col_range) {
      setDefaultColRangeCache(co.use_cider_data_format);
    }
    auto table_schemas = translator_->getInputCiderTableSchema();
    auto table_infos = buildInputTableInfo(table_schemas, co.use_cider_data_format);
    executor_->setSchemaProvider(std::make_shared<CiderSchemaProvider>(table_schemas));
    const bool allow_lazy_fetch = co.allow_lazy_fetch;
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
    const size_t max_groups_buffer_entry_guess = cco.max_groups_buffer_entry_guess;
    const int8_t crt_min_byte_width = cco.crt_min_byte_width;
    const bool has_cardinality_estimation = cco.has_cardinality_estimation;
    ColumnCacheMap column_cache;
    CompilationResult compilation_result;
    std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;
    DataProvider* ciderDataProvider;
    if (co.use_cider_data_format) {
      ciderDataProvider = new CiderArrowDataProvider(build_table_);
    } else {
      ciderDataProvider = new CiderBatchDataProvider(build_table_);
    }
    std::tie(compilation_result, query_mem_desc) =
        executor_->compileWorkUnit(table_infos,
                                   *ra_exe_unit_,
                                   co,
                                   eo,
                                   allow_lazy_fetch,
                                   row_set_mem_owner,
                                   max_groups_buffer_entry_guess,
                                   crt_min_byte_width,
                                   has_cardinality_estimation,
                                   ciderDataProvider,
                                   column_cache);
    ciderCompilationResult->impl_->compilation_result_ = compilation_result;
    ciderCompilationResult->impl_->query_mem_desc_ = std::move(query_mem_desc);
    ciderCompilationResult->impl_->hoist_literals_ = co.hoist_literals;
    ciderCompilationResult->impl_->hoist_buf =
        executor_->serializeLiterals(compilation_result.literal_values, 0);
    ciderCompilationResult->impl_->build_table_ = std::move(build_table_);
    ciderCompilationResult->impl_->ciderStringDictionaryProxy_ =
        ciderStringDictionaryProxy_;
    delete ciderDataProvider;
    return ciderCompilationResult;
  }

  std::shared_ptr<CiderCompilationResult> compile(
      const std::vector<substrait::Expression*> exprs,
      const substrait::NamedStruct& schema,
      const std::vector<
          substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
          func_infos,
      const generator::ExprType& expr_type,
      CiderCompilationOption cco,
      CiderExecutionOption ceo) {
    for (auto expr : exprs) {
      CHECK(expr->has_scalar_function());
    }
    CHECK_NE(schema.struct_().types_size(), 0);
    // Wraps the expreesion to RelAlgExecutionUnit
    translator_ = std::make_shared<generator::SubstraitToRelAlgExecutionUnit>();
    ra_exe_unit_ =
        translator_->createRelAlgExecutionUnit(exprs, schema, func_infos, expr_type);
    // Builds input table schema based on substrait schema
    auto table_schema = {buildInputCiderTableSchema(schema)};
    auto table_infos = buildInputTableInfo(table_schema, cco.use_cider_data_format);
    executor_->setSchemaProvider(std::make_shared<CiderSchemaProvider>(table_schema));
    auto co = CiderCompilationOptionToCo(cco);
    if (co.use_default_col_range) {
      setDefaultColRangeCache(co.use_cider_data_format);
    }
    auto eo = CiderExecutionOptionToEo(ceo);
    const bool allow_lazy_fetch = co.allow_lazy_fetch;
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
    const size_t max_groups_buffer_entry_guess = cco.max_groups_buffer_entry_guess;
    const int8_t crt_min_byte_width = cco.crt_min_byte_width;
    const bool has_cardinality_estimation = cco.has_cardinality_estimation;
    ColumnCacheMap column_cache;
    CompilationResult compilation_result;
    std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;
    std::tie(compilation_result, query_mem_desc) =
        executor_->compileWorkUnit(table_infos,
                                   *ra_exe_unit_,
                                   co,
                                   eo,
                                   allow_lazy_fetch,
                                   row_set_mem_owner,
                                   max_groups_buffer_entry_guess,
                                   crt_min_byte_width,
                                   has_cardinality_estimation,
                                   nullptr,
                                   column_cache);
    auto ciderCompilationResult = std::make_shared<CiderCompilationResult>();
    ciderCompilationResult->impl_->compilation_result_ = compilation_result;
    ciderCompilationResult->impl_->query_mem_desc_ = std::move(query_mem_desc);
    ciderCompilationResult->impl_->hoist_literals_ = co.hoist_literals;
    ciderCompilationResult->impl_->hoist_buf =
        executor_->serializeLiterals(compilation_result.literal_values, 0);
    ciderCompilationResult->impl_->outputSchema_ =
        translator_->getOutputCiderTableSchema();
    ciderCompilationResult->impl_->rel_alg_exe_unit_ = ra_exe_unit_;
    return ciderCompilationResult;
  }

  std::shared_ptr<CiderCompilationResult> compile(
      const RelAlgExecutionUnit& ra_exe_unit,
      const std::vector<InputTableInfo>& table_infos,
      CiderCompilationOption cco,
      CiderExecutionOption ceo) {
    auto co = CiderCompilationOptionToCo(cco);
    auto eo = CiderExecutionOptionToEo(ceo);
    ra_exe_unit_ = std::make_shared<RelAlgExecutionUnit>(ra_exe_unit);

    if (co.use_default_col_range) {
      setDefaultColRangeCache(co.use_cider_data_format);
    }

    const bool allow_lazy_fetch = co.allow_lazy_fetch;
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
    const size_t max_groups_buffer_entry_guess = cco.max_groups_buffer_entry_guess;
    const int8_t crt_min_byte_width = cco.crt_min_byte_width;
    const bool has_cardinality_estimation = cco.has_cardinality_estimation;
    ColumnCacheMap column_cache;
    CompilationResult compilation_result;
    std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;
    std::tie(compilation_result, query_mem_desc) =
        executor_->compileWorkUnit(table_infos,
                                   *ra_exe_unit_,
                                   co,
                                   eo,
                                   allow_lazy_fetch,
                                   row_set_mem_owner,
                                   max_groups_buffer_entry_guess,
                                   crt_min_byte_width,
                                   has_cardinality_estimation,
                                   nullptr,
                                   column_cache);
    auto ciderCompilationResult = std::make_shared<CiderCompilationResult>();
    ciderCompilationResult->impl_->compilation_result_ = compilation_result;
    ciderCompilationResult->impl_->query_mem_desc_ = std::move(query_mem_desc);
    ciderCompilationResult->impl_->hoist_literals_ = co.hoist_literals;
    ciderCompilationResult->impl_->hoist_buf =
        executor_->serializeLiterals(compilation_result.literal_values, 0);
    ciderCompilationResult->impl_->rel_alg_exe_unit_ = ra_exe_unit_;
    return ciderCompilationResult;
  }

  std::shared_ptr<CiderCompilationResult> compile(
      const RelAlgExecutionUnit& ra_exe_unit,
      const std::vector<InputTableInfo>& table_infos,
      std::shared_ptr<CiderTableSchema> schema,
      CiderCompilationOption cco,
      CiderExecutionOption ceo) {
    auto co = CiderCompilationOptionToCo(cco);
    auto eo = CiderExecutionOptionToEo(ceo);
    ra_exe_unit_ = std::make_shared<RelAlgExecutionUnit>(ra_exe_unit);

    if (co.use_default_col_range) {
      setDefaultColRangeCache(co.use_cider_data_format);
    }

    const bool allow_lazy_fetch = co.allow_lazy_fetch;
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
    const size_t max_groups_buffer_entry_guess = cco.max_groups_buffer_entry_guess;
    const int8_t crt_min_byte_width = cco.crt_min_byte_width;
    const bool has_cardinality_estimation = cco.has_cardinality_estimation;
    ColumnCacheMap column_cache;
    CompilationResult compilation_result;
    std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;
    std::tie(compilation_result, query_mem_desc) =
        executor_->compileWorkUnit(table_infos,
                                   *ra_exe_unit_,
                                   co,
                                   eo,
                                   allow_lazy_fetch,
                                   row_set_mem_owner,
                                   max_groups_buffer_entry_guess,
                                   crt_min_byte_width,
                                   has_cardinality_estimation,
                                   nullptr,
                                   column_cache);
    auto ciderCompilationResult = std::make_shared<CiderCompilationResult>();
    ciderCompilationResult->impl_->compilation_result_ = compilation_result;
    ciderCompilationResult->impl_->query_mem_desc_ = std::move(query_mem_desc);
    ciderCompilationResult->impl_->hoist_literals_ = co.hoist_literals;
    ciderCompilationResult->impl_->hoist_buf =
        executor_->serializeLiterals(compilation_result.literal_values, 0);
    ciderCompilationResult->impl_->outputSchema_ = schema;
    ciderCompilationResult->impl_->rel_alg_exe_unit_ = ra_exe_unit_;
    return ciderCompilationResult;
  }

  void getArrowMinMaxAndResetNull(std::unique_ptr<CiderBatch> child_batch,
                                  const int64_t row_num,
                                  const ::substrait::Type& type,
                                  int64_t* min,
                                  int64_t* max,
                                  const int8_t* null_buff) {
    if (type.has_i64()) {
      int64_t* buffer =
          child_batch->asMutable<ScalarBatch<int64_t>>()->getMutableRawData();
      initColRangeAndResetNull<int64_t>(buffer, row_num, min, max, null_buff);
    } else if (type.has_i32()) {
      int32_t* buffer =
          child_batch->asMutable<ScalarBatch<int32_t>>()->getMutableRawData();
      initColRangeAndResetNull<int32_t>(buffer, row_num, min, max, null_buff);
    }
  }

  template <class T>
  void initColRangeAndResetNull(T* buffer,
                                const int64_t row_num,
                                int64_t* min,
                                int64_t* max,
                                const int8_t* null_buff) {
    bool init_flag = false;
    for (int i = 0; i < row_num; i++) {
      if (null_buff && CiderBitUtils::isBitSetAt((uint8_t*)null_buff, i)) {
        if (!init_flag) {
          *min = buffer[i];
          *max = buffer[i];
          init_flag = true;
        }
        if (buffer[i] < *min)
          *min = buffer[i];
        if (buffer[i] > *max)
          *max = buffer[i];
      }
    }
    // reset null value outside the col_range to avoid build in hashtable in next
    for (int i = 0; i < row_num; i++) {
      if (null_buff && !CiderBitUtils::isBitSetAt((uint8_t*)null_buff, i)) {
        buffer[i] = *min - 1;
      }
    }
  }

  void getMinMax(const int8_t* buf,
                 const int64_t row_num,
                 const ::substrait::Type& type,
                 int64_t* min,
                 int64_t* max) {
    if (type.has_i64()) {
      const int64_t* buffer = (const int64_t*)buf;
      *min = buffer[0];
      *max = buffer[0];
      for (int i = 1; i < row_num; i++) {
        if (buffer[i] < *min)
          *min = buffer[i];
        if (buffer[i] > *max)
          *max = buffer[i];
      }
    } else if (type.has_i32()) {
      const int32_t* buffer = (const int32_t*)buf;
      *min = buffer[0];
      *max = buffer[0];
      for (int i = 1; i < row_num; i++) {
        if (buffer[i] < *min)
          *min = buffer[i];
        if (buffer[i] > *max)
          *max = buffer[i];
      }
    }
  }

  bool isSubtraitIntegerType(const ::substrait::Type& type) {
    return type.has_i32() || type.has_i64();
  }

  // This method will generate a fake ColRange unless join case, which will generate
  // actual build table col range since we have full input data via feedBuildTable API,
  // this could help codegen.
  void setDefaultColRangeCache(bool use_cider_data_format) {
    if (!translator_) {
      // don't have a valid translator_
      // TODO(jikunshang/BigPYJ1151): remove this, seems only used via
      // CiderCompileModule::compile(void* ra_exe_unit_, ...) API, which should be
      // deprecated.
      setDefaultColRangeCacheWoSchema();
      return;
    }
    AggregatedColRange col_range;
    auto table_schemas = translator_->getInputCiderTableSchema();
    CHECK_LE(table_schemas.size(), 2);  // only support 2 tables join
    for (auto i = 0; i < table_schemas.size(); i++) {
      auto& table_schema = table_schemas[i];
      auto col_count = table_schema.getColumnCount();
      for (int j = 0; j < col_count; j++) {
        PhysicalInput physicalInput = {j, i + 100};
        int64_t min = 0;
        int64_t max = -1;
        // join table and integer type, find the real min/max value
        if (i >= 1 && isSubtraitIntegerType(table_schema.getColumnTypeById(j))) {
          if (use_cider_data_format) {
            if (auto child_batch = build_table_.getChildAt(j);
                build_table_.getLength() > 0 && child_batch) {
              getArrowMinMaxAndResetNull(
                  std::move(child_batch),
                  build_table_.getLength(),
                  table_schema.getColumnTypeById(j),
                  &min,
                  &max,
                  reinterpret_cast<const int8_t*>(child_batch->getNulls()));
            }
          } else {
            if (build_table_.row_num() > 0 && build_table_.column(j)) {
              getMinMax(build_table_.column(j),
                        build_table_.row_num(),
                        table_schema.getColumnTypeById(j),
                        &min,
                        &max);
            }
          }
        }
        auto expression_range = buildExpressionRange(
            generator::getSQLTypeInfo(table_schema.getColumnTypeById(j)).get_type(),
            min,
            max);
        col_range.setColRange(physicalInput, expression_range);
      }
    }
    executor_->setColRangeCache(col_range);
  }

  void setDefaultColRangeCacheWoSchema() {
    AggregatedColRange col_range;
    for (auto& group_key : ra_exe_unit_->groupby_exprs) {
      if (!group_key) {
        continue;
      }
      auto expressRange =
          buildExpressionRange(group_key->get_type_info().get_type(), 1, 0);
      if (auto group_key_col = std::reinterpret_pointer_cast<Analyzer::Var>(group_key)) {
        PhysicalInput physicalInput = {group_key_col->get_column_id(),
                                       group_key_col->get_table_id()};
        col_range.setColRange(physicalInput, expressRange);
      } else if (auto group_key_col =
                     std::reinterpret_pointer_cast<Analyzer::ColumnVar>(group_key)) {
        PhysicalInput physicalInput = {group_key_col->get_column_id(),
                                       group_key_col->get_table_id()};
        col_range.setColRange(physicalInput, expressRange);
      } else {
        LOG(ERROR) << "Not supported expr for col range.";
      }
    }
    executor_->setColRangeCache(col_range);
  }

  static ExpressionRange buildExpressionRange(SQLTypes type, int64_t min, int64_t max) {
    switch (type) {
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kTEXT:
      case kBOOLEAN:
      case kDECIMAL:
        return ExpressionRange::makeIntRange(min, max, 0, false);
      case kFLOAT:
        return ExpressionRange::makeFloatRange(min, max, false);
      case kDOUBLE:
        return ExpressionRange::makeDoubleRange(min, max, false);
      default:
        return ExpressionRange::makeInvalidRange();
    }
  }

  void feedBuildTable(CiderBatch&& build_table) { build_table_ = std::move(build_table); }

 private:
  Executor::ExecutorId id_ = Executor::UNITARY_EXECUTOR_ID;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<RelAlgExecutionUnit> ra_exe_unit_;
  std::shared_ptr<generator::SubstraitToRelAlgExecutionUnit> translator_;
  CiderBatch build_table_;
  std::shared_ptr<StringDictionaryProxy> ciderStringDictionaryProxy_;
  std::shared_ptr<CiderAllocator> allocator_;

  std::vector<InputTableInfo> buildInputTableInfo(
      const std::vector<CiderTableSchema>& tableSchemas,
      bool use_cider_data_format) {
    std::vector<InputTableInfo> query_infos;
    const int db_id = 100;
    // Note that we only consider single join here, so use faked table id 100
    const int table_id = 100;
    // seems only this row num will be used for building join hash table only, so we set
    // row num to build table row num
    int row_num = 20;
    if (use_cider_data_format) {
      if (!build_table_.isMoved() && build_table_.getLength() > 0) {
        row_num = build_table_.getLength();
      }
    } else {
      if (build_table_.row_num() > 0) {
        row_num = build_table_.row_num();
      }
    }
    for (int i = 0; i < tableSchemas.size(); i++) {
      Fragmenter_Namespace::FragmentInfo fi_0;
      fi_0.fragmentId = 0;
      fi_0.shadowNumTuples = row_num;
      fi_0.physicalTableId = table_id + i;
      fi_0.setPhysicalNumTuples(row_num);

      Fragmenter_Namespace::TableInfo ti_0;
      ti_0.fragments = {fi_0};
      ti_0.setPhysicalNumTuples(row_num);

      InputTableInfo iti_0{db_id, table_id + i, ti_0};

      query_infos.push_back(iti_0);
    }

    return query_infos;
  }
  std::shared_ptr<StringDictionaryProxy> initStringDictionaryProxy(
      std::shared_ptr<CiderAllocator> allocator) {
    std::shared_ptr<StringDictionaryProxy> stringDictionaryProxy;
    const DictRef dict_ref(-1, 1);
    std::shared_ptr<StringDictionary> tsd =
        std::make_shared<StringDictionary>(dict_ref, "", allocator, false, true);
    stringDictionaryProxy.reset(new StringDictionaryProxy(tsd, 0, 0));

    return stringDictionaryProxy;
  }
  CiderTableSchema buildInputCiderTableSchema(const substrait::NamedStruct& schema) {
    std::vector<std::string> names;
    for (auto name : schema.names()) {
      names.push_back(name);
    }
    std::vector<substrait::Type> types;
    for (auto type : schema.struct_().types()) {
      types.push_back(type);
    }
    CiderTableSchema input_schema(names, types, "");
    return input_schema;
  }
};

CiderCompileModule::CiderCompileModule(std::shared_ptr<CiderAllocator> allocator) {
  impl_.reset(new Impl(allocator));
}

CiderCompileModule::~CiderCompileModule() {}

std::shared_ptr<CiderCompileModule> CiderCompileModule::Make(
    std::shared_ptr<CiderAllocator> allocator) {
  return std::make_shared<CiderCompileModule>(allocator);
}

std::shared_ptr<CiderCompilationResult> CiderCompileModule::compile(
    const substrait::Plan& plan,
    CiderCompilationOption cco,
    CiderExecutionOption ceo) {
  return impl_->compile(plan, cco, ceo);
}

std::shared_ptr<CiderCompilationResult> CiderCompileModule::compile(
    const std::vector<substrait::Expression*> exprs,
    const substrait::NamedStruct& schema,
    const std::vector<
        substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*> func_infos,
    const generator::ExprType& expr_type,
    CiderCompilationOption cco,
    CiderExecutionOption ceo) {
  return impl_->compile(exprs, schema, func_infos, expr_type, cco, ceo);
}

std::shared_ptr<CiderCompilationResult> CiderCompileModule::compile(
    void* ra_exe_unit_,
    void* table_infos_,
    CiderCompilationOption cco,
    CiderExecutionOption ceo) {
  RelAlgExecutionUnit ra_exe_unit = *(RelAlgExecutionUnit*)ra_exe_unit_;
  std::vector<InputTableInfo> table_infos = *(std::vector<InputTableInfo>*)table_infos_;

  return impl_->compile(ra_exe_unit, table_infos, cco, ceo);
}

// TODO: to be removed if test framework ready
std::shared_ptr<CiderCompilationResult> CiderCompileModule::compile(
    void* ra_exe_unit_,
    void* table_infos_,
    std::shared_ptr<CiderTableSchema> schema,
    CiderCompilationOption cco,
    CiderExecutionOption ceo) {
  RelAlgExecutionUnit ra_exe_unit = *(RelAlgExecutionUnit*)ra_exe_unit_;
  std::vector<InputTableInfo> table_infos = *(std::vector<InputTableInfo>*)table_infos_;

  return impl_->compile(ra_exe_unit, table_infos, schema, cco, ceo);
}

void CiderCompileModule::feedBuildTable(CiderBatch&& build_table) {
  impl_->feedBuildTable(std::move(build_table));
}
