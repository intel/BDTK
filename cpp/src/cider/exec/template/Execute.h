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

#ifndef QUERYENGINE_EXECUTE_H
#define QUERYENGINE_EXECUTE_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include <llvm/IR/Argument.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Value.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Transforms/Utils/ValueMapper.h>
#include <rapidjson/document.h>

// #include "exec/operator/aggregate/CiderAggHashTable.h"
#include "exec/template/AggregatedColRange.h"
#include "exec/template/BufferCompaction.h"
#include "exec/template/CartesianProduct.h"
#include "exec/template/CgenState.h"
#include "exec/template/CodeCache.h"
#include "exec/template/CodegenColValues.h"
#include "exec/template/ColumnFetcher.h"
#include "exec/template/CompilationOptions.h"
#include "exec/template/DateTimeUtils.h"
#include "exec/template/GroupByAndAggregate.h"
#include "exec/template/PlanState.h"
#include "exec/template/QueryPlanDagCache.h"
#include "exec/template/RelAlgExecutionUnit.h"
#include "exec/template/StringDictionaryGenerations.h"
#include "exec/template/TableGenerations.h"
#include "exec/template/TargetMetaInfo.h"
#include "exec/template/WindowContext.h"
#include "exec/template/common/descriptors/QueryCompilationDescriptor.h"
#include "exec/template/operator/join/JoinLoop.h"
#include "exec/template/operator/join/hashtable/HashJoin.h"
#include "util/memory/BufferProvider.h"
#include "util/memory/DataProvider.h"

#include "type/data/funcannotations.h"
#include "type/data/string/LruCache.hpp"
#include "type/data/string/StringDictionary.h"
#include "type/data/string/StringDictionaryProxy.h"
#include "type/schema/SchemaProvider.h"
#include "util/Logger.h"
#include "util/MemoryInfo.h"
#include "util/mapd_shared_mutex.h"
#include "util/measure.h"
#include "util/memory/Chunk/Chunk.h"
#include "util/thread_count.h"
#include "util/toString.h"

class QueryMemoryDescriptor;
using QuerySessionId = std::string;
using CurrentQueryStatus = std::pair<QuerySessionId, std::string>;
using InterruptFlagMap = std::map<QuerySessionId, bool>;
class QuerySessionStatus {
  // A class that is used to describe the query session's info
 public:
  /* todo(yoonmin): support more query status
   * i.e., RUNNING_SORT, RUNNING_CARD_EST, CLEANUP, ... */
  enum QueryStatus {
    UNDEFINED = 0,
    PENDING_QUEUE,
    PENDING_EXECUTOR,
    RUNNING_QUERY_KERNEL,
    RUNNING_REDUCTION,
    RUNNING_IMPORTER
  };

  QuerySessionStatus(const QuerySessionId& query_session,
                     const std::string& query_str,
                     const std::string& submitted_time)
      : query_session_(query_session)
      , executor_id_(0)
      , query_str_(query_str)
      , submitted_time_(submitted_time)
      , query_status_(QueryStatus::UNDEFINED) {}
  QuerySessionStatus(const QuerySessionId& query_session,
                     const size_t executor_id,
                     const std::string& query_str,
                     const std::string& submitted_time)
      : query_session_(query_session)
      , executor_id_(executor_id)
      , query_str_(query_str)
      , submitted_time_(submitted_time)
      , query_status_(QueryStatus::UNDEFINED) {}
  QuerySessionStatus(const QuerySessionId& query_session,
                     const size_t executor_id,
                     const std::string& query_str,
                     const std::string& submitted_time,
                     const QuerySessionStatus::QueryStatus& query_status)
      : query_session_(query_session)
      , executor_id_(executor_id)
      , query_str_(query_str)
      , submitted_time_(submitted_time)
      , query_status_(query_status) {}

  const QuerySessionId getQuerySession() { return query_session_; }
  const std::string getQueryStr() { return query_str_; }
  const size_t getExecutorId() { return executor_id_; }
  const std::string& getQuerySubmittedTime() { return submitted_time_; }
  const QuerySessionStatus::QueryStatus getQueryStatus() { return query_status_; }
  void setQueryStatus(const QuerySessionStatus::QueryStatus& status) {
    query_status_ = status;
  }
  void setExecutorId(const size_t executor_id) { executor_id_ = executor_id; }

 private:
  const QuerySessionId query_session_;
  size_t executor_id_;
  const std::string query_str_;
  const std::string submitted_time_;
  // Currently we use three query status:
  // 1) PENDING_IN_QUEUE: a task is submitted to the dispatch_queue but hangs due to no
  // existing worker (= executor) 2) PENDING_IN_EXECUTOR: a task is assigned to the
  // specific executor but waits to get the resource to run 3) RUNNING: a task is assigned
  // to the specific executor and its execution has been successfully started
  // 4) RUNNING_REDUCTION: a task is in the reduction phase
  QuerySessionStatus::QueryStatus query_status_;
};
using QuerySessionMap =
    std::map<const QuerySessionId, std::map<std::string, QuerySessionStatus>>;

extern bool is_rt_udf_module_present(bool cpu_only = false);

class ColumnFetcher;
class Executor;

inline llvm::Value* get_arg_by_name(llvm::Function* func, const std::string& name) {
  for (auto& arg : func->args()) {
    if (arg.getName() == name) {
      return &arg;
    }
  }
  CHECK(false);
  return nullptr;
}

inline uint32_t log2_bytes(const uint32_t bytes) {
  switch (bytes) {
    case 1:
      return 0;
    case 2:
      return 1;
    case 4:
      return 2;
    case 8:
      return 3;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("bytes is {}", bytes));
  }
}

inline const Analyzer::Expr* extract_cast_arg(const Analyzer::Expr* expr) {
  const auto cast_expr = dynamic_cast<const Analyzer::UOper*>(expr);
  if (!cast_expr || cast_expr->get_optype() != kCAST) {
    return expr;
  }
  return cast_expr->get_operand();
}

inline std::string numeric_type_name(const SQLTypeInfo& ti,
                                     bool is_arrow_format = false) {
  CHECK(ti.is_integer() || ti.is_decimal() || ti.is_boolean() || ti.is_time() ||
        ti.is_fp() || (ti.is_string() && ti.get_compression() == kENCODING_DICT) ||
        ti.is_timeinterval());
  if (ti.is_integer() || ti.is_decimal() || ti.is_boolean() || ti.is_time() ||
      ti.is_string() || ti.is_timeinterval()) {
    return "int" + std::to_string(get_bit_width(ti, is_arrow_format)) + "_t";
  }
  return ti.get_type() == kDOUBLE ? "double" : "float";
}

inline const TemporaryTable& get_temporary_table(const TemporaryTables* temporary_tables,
                                                 const int table_id) {
  CHECK_LT(table_id, 0);
  const auto it = temporary_tables->find(table_id);
  CHECK(it != temporary_tables->end());
  return it->second;
}

inline const SQLTypeInfo get_column_type(const int col_id,
                                         const int table_id,
                                         ColumnInfoPtr col_info,
                                         const TemporaryTables* temporary_tables) {
  CHECK(col_info || temporary_tables);
  if (col_info) {
    CHECK_EQ(col_id, col_info->column_id);
    CHECK_EQ(table_id, col_info->table_id);
    return col_info->type;
  }
  const auto& temp = get_temporary_table(temporary_tables, table_id);
  return temp.getColType(col_id);
}

// TODO(alex): Adjust interfaces downstream and make this not needed.
inline std::vector<Analyzer::Expr*> get_exprs_not_owned(
    const std::vector<std::shared_ptr<Analyzer::Expr>>& exprs) {
  std::vector<Analyzer::Expr*> exprs_not_owned;
  for (const auto& expr : exprs) {
    exprs_not_owned.push_back(expr.get());
  }
  return exprs_not_owned;
}

class ExtensionFunction;

class QueryCompilationDescriptor;

class Executor {
  static_assert(sizeof(float) == 4 && sizeof(double) == 8,
                "Host hardware not supported, unexpected size of float / double.");
  static_assert(sizeof(time_t) == 8,
                "Host hardware not supported, 64-bit time support is required.");

 public:
  using ExecutorId = size_t;
  static const ExecutorId UNITARY_EXECUTOR_ID = 0;
  static const ExecutorId INVALID_EXECUTOR_ID = SIZE_MAX;

  Executor(const ExecutorId id,
           DataProvider* data_provider,
           BufferProvider* buffer_provider,
           const std::string& debug_dir,
           const std::string& debug_file);

  ~Executor() { extension_modules_.clear(); }

  static std::shared_ptr<Executor> getExecutor(const ExecutorId id,
                                               DataProvider* data_provider = nullptr,
                                               BufferProvider* buffer_provider = nullptr,
                                               const std::string& debug_dir = "",
                                               const std::string& debug_file = "");

  static void nukeCacheOfExecutors() {
    mapd_unique_lock<mapd_shared_mutex> flush_lock(
        execute_mutex_);  // don't want native code to vanish while executing
    mapd_unique_lock<mapd_shared_mutex> lock(executors_cache_mutex_);
    (decltype(executors_){}).swap(executors_);
  }

  static size_t getArenaBlockSize();

  static void addUdfIrToModule(const std::string& udf_ir_filename);

  /**
   * Returns pointer to the intermediate tables vector currently stored by this
   * executor.
   */
  const TemporaryTables* getTemporaryTables() const;

  /**
   * Returns a string dictionary proxy using the currently active row set memory owner.
   */
  StringDictionaryProxy* getStringDictionaryProxy(const int dict_id,
                                                  const bool with_generation) const {
    CHECK(row_set_mem_owner_);
    return getStringDictionaryProxy(dict_id, row_set_mem_owner_, with_generation);
  }

  void setCiderStringDictionaryProxy(StringDictionaryProxy* ciderStringDictionaryProxy) {
    ciderStringDictionaryProxy_ = ciderStringDictionaryProxy;
  }

  StringDictionaryProxy* getCiderStringDictionaryProxy() {
    CHECK(ciderStringDictionaryProxy_);
    return ciderStringDictionaryProxy_;
  }

  StringDictionaryProxy* getStringDictionaryProxy(
      const int dictId,
      const std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
      const bool with_generation) const;

  enum class ExtModuleKinds {
    template_module,    // RuntimeFunctions.bc
    udf_cpu_module,     // Load-time UDFs for CPU execution
    rt_udf_cpu_module,  // Run-time UDF/UDTFs for CPU execution
  };
  // Globally available mapping of extension module sources. Not thread-safe.
  std::map<ExtModuleKinds, std::string> extension_module_sources;
  void initialize_extension_module_sources();

  // Convenience functions for retrieving executor-local extension modules, thread-safe:
  const std::unique_ptr<llvm::Module>& get_rt_module() const {
    return get_extension_module(ExtModuleKinds::template_module);
  }

  void update_extension_modules(bool update_runtime_modules_only = false);
  llvm::LLVMContext& getContext() { return *context_.get(); }

  bool containsLeftDeepOuterJoin() const {
    return cgen_state_->contains_left_deep_outer_join_;
  }

  SchemaProviderPtr getSchemaProvider() const { return schema_provider_; }
  void setSchemaProvider(SchemaProviderPtr provider) { schema_provider_ = provider; }

  int getDatabaseId() const { return db_id_; }
  void setDatabaseId(int db_id) { db_id_ = db_id; }

  DataProvider* getDataProvider() const {
    CHECK(data_provider_);
    return data_provider_;
  }

  BufferProvider* getBufferProvider() const {
    // remove null check.
    // CHECK(buffer_provider_);
    return buffer_provider_;
  }

  const std::shared_ptr<RowSetMemoryOwner> getRowSetMemoryOwner() const;

  Fragmenter_Namespace::TableInfo getTableInfo(const int table_id) const;

  const TableGeneration& getTableGeneration(const int table_id) const;

  ExpressionRange getColRange(const PhysicalInput&) const;

  size_t getNumBytesForFetchedRow(const std::set<int>& table_ids_to_fetch) const;

  bool hasLazyFetchColumns(const std::vector<Analyzer::Expr*>& target_exprs) const;

  void registerActiveModule(void* module, const int device_id) const;
  void unregisterActiveModule(void* module, const int device_id) const;
  void interrupt(const QuerySessionId& query_session = "",
                 const QuerySessionId& interrupt_session = "");
  // only for testing usage
  void enableRuntimeQueryInterrupt(const double runtime_query_check_freq,
                                   const unsigned pending_query_check_freq) const;

  static const size_t high_scan_limit{32000000};

  void addTransientStringLiterals(
      const RelAlgExecutionUnit& ra_exe_unit,
      const std::shared_ptr<RowSetMemoryOwner>& row_set_mem_owner);

 private:
  void clearMetaInfoCache();

  // Generate code for a window function target.
  llvm::Value* codegenWindowFunction(const size_t target_index,
                                     const CompilationOptions& co);

  // Generate code for an aggregate window function target.
  llvm::Value* codegenWindowFunctionAggregate(const CompilationOptions& co);

  // The aggregate state requires a state reset when starting a new partition. Generate
  // the new partition check and return the continuation basic block.
  llvm::BasicBlock* codegenWindowResetStateControlFlow();

  // Generate code for initializing the state of a window aggregate.
  void codegenWindowFunctionStateInit(llvm::Value* aggregate_state);

  // Generates the required calls for an aggregate window function and returns the final
  // result.
  llvm::Value* codegenWindowFunctionAggregateCalls(llvm::Value* aggregate_state,
                                                   const CompilationOptions& co);

  // The AVG window function requires some post-processing: the sum is divided by count
  // and the result is stored back for the current row.
  void codegenWindowAvgEpilogue(llvm::Value* crt_val,
                                llvm::Value* window_func_null_val,
                                llvm::Value* multiplicity_lv);

  // Generates code which loads the current aggregate value for the window context.
  llvm::Value* codegenAggregateWindowState();

  llvm::Value* aggregateWindowStatePtr();

  using PerFragmentCallBack =
      std::function<void(ResultSetPtr, const Fragmenter_Namespace::FragmentInfo&)>;

  std::unordered_map<int, const Analyzer::BinOper*> getInnerTabIdToJoinCond() const;

 public:  // Temporary, ask saman about this
  static std::pair<int64_t, int32_t> reduceResults(const SQLAgg agg,
                                                   const SQLTypeInfo& ti,
                                                   const int64_t agg_init_val,
                                                   const int8_t out_byte_width,
                                                   const int64_t* out_vec,
                                                   const size_t out_vec_sz,
                                                   const bool is_group_by,
                                                   const bool float_argument_input);

  static void addCodeToCache(const CodeCacheKey&,
                             std::shared_ptr<CompilationContext>,
                             llvm::Module*,
                             CodeCache&);

 private:
  std::vector<int64_t> getJoinHashTablePtrs(const int device_id);
  std::vector<llvm::Value*> inlineHoistedLiterals();

 public:
  std::tuple<CompilationResult, std::unique_ptr<QueryMemoryDescriptor>> compileWorkUnit(
      const std::vector<InputTableInfo>& query_infos,
      const RelAlgExecutionUnit& ra_exe_unit,
      const CompilationOptions& co,
      const ExecutionOptions& eo,
      const bool allow_lazy_fetch,
      std::shared_ptr<RowSetMemoryOwner>,
      const size_t max_groups_buffer_entry_count,
      const int8_t crt_min_byte_width,
      const bool has_cardinality_estimation,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache);

 private:
  std::vector<JoinLoop> buildJoinLoops(RelAlgExecutionUnit& ra_exe_unit,
                                       const CompilationOptions& co,
                                       const ExecutionOptions& eo,
                                       const std::vector<InputTableInfo>& query_infos,
                                       DataProvider* data_provider,
                                       ColumnCacheMap& column_cache);

  // Create a callback which hoists left hand side filters above the join for left
  // joins, eliminating extra computation of the probe and matches if the row does not
  // pass the filters
  JoinLoop::HoistedFiltersCallback buildHoistLeftHandSideFiltersCb(
      const RelAlgExecutionUnit& ra_exe_unit,
      const size_t level_idx,
      const int inner_table_id,
      const CompilationOptions& co);
  // Builds a join hash table for the provided conditions on the current level.
  // Returns null iff on failure and provides the reasons in `fail_reasons`.
  std::shared_ptr<HashJoin> buildCurrentLevelHashTable(
      const JoinCondition& current_level_join_conditions,
      size_t level_idx,
      RelAlgExecutionUnit& ra_exe_unit,
      const CompilationOptions& co,
      const std::vector<InputTableInfo>& query_infos,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache,
      std::vector<std::string>& fail_reasons);
  void redeclareFilterFunction();
  llvm::Value* addJoinLoopIterator(const std::vector<llvm::Value*>& prev_iters,
                                   const size_t level_idx);
  void codegenJoinLoops(const std::vector<JoinLoop>& join_loops,
                        const RelAlgExecutionUnit& ra_exe_unit,
                        GroupByAndAggregate& group_by_and_aggregate,
                        llvm::Function* query_func,
                        llvm::BasicBlock* entry_bb,
                        const QueryMemoryDescriptor& query_mem_desc,
                        const CompilationOptions& co,
                        const ExecutionOptions& eo);
  bool compileBody(const RelAlgExecutionUnit& ra_exe_unit,
                   GroupByAndAggregate& group_by_and_aggregate,
                   const QueryMemoryDescriptor& query_mem_desc,
                   const CompilationOptions& co);

  void createErrorCheckControlFlow(llvm::Function* query_func,
                                   bool run_with_dynamic_watchdog,
                                   bool run_with_allowing_runtime_interrupt,
                                   const std::vector<InputTableInfo>& input_table_infos);

  void insertErrorCodeChecker(llvm::Function* query_func,
                              bool hoist_literals,
                              bool allow_runtime_query_interrupt);

  void preloadFragOffsets(const std::vector<InputDescriptor>& input_descs,
                          const std::vector<InputTableInfo>& query_infos);

  struct JoinHashTableOrError {
    std::shared_ptr<HashJoin> hash_table;
    std::string fail_reason;
  };

  JoinHashTableOrError buildHashTableForQualifier(
      const std::shared_ptr<Analyzer::BinOper>& qual_bin_oper,
      const std::vector<InputTableInfo>& query_infos,
      const MemoryLevel memory_level,
      const JoinType join_type,
      const HashType preferred_hash_type,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache,
      const HashTableBuildDagMap& hashtable_build_dag_map,
      const RegisteredQueryHint& query_hint,
      const TableIdToNodeMap& table_id_to_node_map);
  void nukeOldState(const bool allow_lazy_fetch,
                    const std::vector<InputTableInfo>& query_infos,
                    const RelAlgExecutionUnit* ra_exe_unit);

  std::shared_ptr<CompilationContext> optimizeAndCodegenCPU(
      llvm::Function*,
      llvm::Function*,
      const std::unordered_set<llvm::Function*>&,
      const CompilationOptions&);

  struct GroupColLLVMValue {
    llvm::Value* translated_value;
    llvm::Value* original_value;
  };

  GroupColLLVMValue groupByColumnCodegen(Analyzer::Expr* group_by_col,
                                         const size_t col_width,
                                         const CompilationOptions&,
                                         const bool translate_null_val,
                                         const int64_t translated_null_val,
                                         DiamondCodegen&,
                                         std::stack<llvm::BasicBlock*>&,
                                         const bool thread_mem_shared,
                                         llvm::Argument* groups_buffer = nullptr);

  std::unique_ptr<FixedSizeColValues> groupByColumnCodegen(
      Analyzer::Expr* group_by_col,
      const size_t col_width,
      const CompilationOptions& co,
      llvm::Argument* groups_buffer = nullptr);

  llvm::Value* castToFP(llvm::Value*,
                        SQLTypeInfo const& from_ti,
                        SQLTypeInfo const& to_ti);
  llvm::Value* castToIntPtrTyIn(llvm::Value* val, const size_t bit_width);

  AggregatedColRange computeColRangesCache(
      const std::unordered_set<InputColDescriptor>& col_descs);
  StringDictionaryGenerations computeStringDictionaryGenerations(
      const std::unordered_set<InputColDescriptor>& col_descs);
  TableGenerations computeTableGenerations(std::unordered_set<int> phys_table_ids);

 public:
  void setupCaching(DataProvider* data_provider,
                    const std::unordered_set<InputColDescriptor>& col_descs,
                    const std::unordered_set<int>& phys_table_ids);

  void setColRangeCache(const AggregatedColRange& aggregated_col_range) {
    agg_col_range_cache_ = aggregated_col_range;
  }

  ExecutorId getExecutorId() const { return executor_id_; };

  QuerySessionId& getCurrentQuerySession(mapd_shared_lock<mapd_shared_mutex>& read_lock);

  QuerySessionStatus::QueryStatus getQuerySessionStatus(
      const QuerySessionId& candidate_query_session,
      mapd_shared_lock<mapd_shared_mutex>& read_lock);

  bool checkCurrentQuerySession(const std::string& candidate_query_session,
                                mapd_shared_lock<mapd_shared_mutex>& read_lock);
  void invalidateRunningQuerySession(mapd_unique_lock<mapd_shared_mutex>& write_lock);
  bool addToQuerySessionList(const QuerySessionId& query_session,
                             const std::string& query_str,
                             const std::string& submitted,
                             const size_t executor_id,
                             const QuerySessionStatus::QueryStatus query_status,
                             mapd_unique_lock<mapd_shared_mutex>& write_lock);
  bool removeFromQuerySessionList(const QuerySessionId& query_session,
                                  const std::string& submitted_time_str,
                                  mapd_unique_lock<mapd_shared_mutex>& write_lock);
  void setQuerySessionAsInterrupted(const QuerySessionId& query_session,
                                    mapd_unique_lock<mapd_shared_mutex>& write_lock);
  bool checkIsQuerySessionInterrupted(const std::string& query_session,
                                      mapd_shared_lock<mapd_shared_mutex>& read_lock);
  bool checkIsQuerySessionEnrolled(const QuerySessionId& query_session,
                                   mapd_shared_lock<mapd_shared_mutex>& read_lock);
  bool updateQuerySessionStatusWithLock(
      const QuerySessionId& query_session,
      const std::string& submitted_time_str,
      const QuerySessionStatus::QueryStatus updated_query_status,
      mapd_unique_lock<mapd_shared_mutex>& write_lock);
  bool updateQuerySessionExecutorAssignment(
      const QuerySessionId& query_session,
      const std::string& submitted_time_str,
      const size_t executor_id,
      mapd_unique_lock<mapd_shared_mutex>& write_lock);
  std::vector<QuerySessionStatus> getQuerySessionInfo(
      const QuerySessionId& query_session,
      mapd_shared_lock<mapd_shared_mutex>& read_lock);

  mapd_shared_mutex& getSessionLock();
  CurrentQueryStatus attachExecutorToQuerySession(
      const QuerySessionId& query_session_id,
      const std::string& query_str,
      const std::string& query_submitted_time);
  void checkPendingQueryStatus(const QuerySessionId& query_session);
  void clearQuerySessionStatus(const QuerySessionId& query_session,
                               const std::string& submitted_time_str);
  void updateQuerySessionStatus(const QuerySessionId& query_session,
                                const std::string& submitted_time_str,
                                const QuerySessionStatus::QueryStatus new_query_status);
  void enrollQuerySession(const QuerySessionId& query_session,
                          const std::string& query_str,
                          const std::string& submitted_time_str,
                          const size_t executor_id,
                          const QuerySessionStatus::QueryStatus query_session_status);
  // get a set of executor ids that a given session has fired regardless of
  // each executor's status: pending or running
  const std::vector<size_t> getExecutorIdsRunningQuery(
      const QuerySessionId& interrupt_session) const;
  // check whether the current session that this executor manages is interrupted
  // while performing non-kernel time task
  bool checkNonKernelTimeInterrupted() const;

  // true when we have matched cardinality, and false otherwise
  using CachedCardinality = std::pair<bool, size_t>;
  void addToCardinalityCache(const std::string& cache_key, const size_t cache_value);
  CachedCardinality getCachedCardinality(const std::string& cache_key);

  mapd_shared_mutex& getDataRecyclerLock();
  QueryPlanDagCache& getQueryPlanDagCache();
  JoinColumnsInfo getJoinColumnsInfo(const Analyzer::Expr* join_expr,
                                     JoinColumnSide target_side,
                                     bool extract_only_col_id);

  std::vector<int8_t> serializeLiterals(
      const std::unordered_map<int, CgenState::LiteralValues>& literals,
      const int device_id);

  // CgenStateManager uses RAII pattern to ensure that recursive code
  // generation (e.g. as in multi-step multi-subqueries) uses a new
  // CgenState instance for each recursion depth while restoring the
  // old CgenState instances when returning from recursion.
  class CgenStateManager {
   public:
    CgenStateManager(Executor& executor);
    CgenStateManager(Executor& executor,
                     const bool allow_lazy_fetch,
                     const std::vector<InputTableInfo>& query_infos,
                     const RelAlgExecutionUnit* ra_exe_unit);
    ~CgenStateManager();

   private:
    Executor& executor_;
    std::chrono::steady_clock::time_point lock_queue_clock_;
    std::lock_guard<std::mutex> lock_;
    std::unique_ptr<CgenState> cgen_state_;
  };

 private:
  std::shared_ptr<CompilationContext> getCodeFromCache(const CodeCacheKey&,
                                                       const CodeCache&);

  static size_t align(const size_t off_in, const size_t alignment) {
    size_t off = off_in;
    if (off % alignment != 0) {
      off += (alignment - off % alignment);
    }
    return off;
  }

  std::unique_ptr<CgenState> cgen_state_;
  mutable std::mutex str_hasher_mutex_;

  const std::unique_ptr<llvm::Module>& get_extension_module(ExtModuleKinds kind) const {
    auto it = extension_modules_.find(kind);
    if (it != extension_modules_.end()) {
      return it->second;
    }
    static const std::unique_ptr<llvm::Module> empty;
    return empty;
  }
  std::map<ExtModuleKinds, std::unique_ptr<llvm::Module>> extension_modules_;

  class FetchCacheAnchor {
   public:
    FetchCacheAnchor(CgenState* cgen_state)
        : cgen_state_(cgen_state), saved_fetch_cache(cgen_state_->fetch_cache_) {}
    ~FetchCacheAnchor() { cgen_state_->fetch_cache_.swap(saved_fetch_cache); }

   private:
    CgenState* cgen_state_;
    std::unordered_map<int, std::vector<llvm::Value*>> saved_fetch_cache;
  };

  std::unique_ptr<PlanState> plan_state_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  std::unique_ptr<llvm::LLVMContext> context_;
  // indicates whether this executor has been interrupted
  std::atomic<bool> interrupted_;

  mutable std::mutex str_dict_mutex_;

  mutable std::unique_ptr<llvm::TargetMachine> nvptx_target_machine_;

  CodeCache cpu_code_cache_;

  static const size_t baseline_threshold{
      1000000};  // if a perfect hash needs more entries, use baseline
  static const size_t code_cache_size{1000};

  const std::string debug_dir_;
  const std::string debug_file_;

  const ExecutorId executor_id_;
  SchemaProviderPtr schema_provider_;
  int db_id_ = -1;
  DataProvider* data_provider_;
  BufferProvider* buffer_provider_;
  const TemporaryTables* temporary_tables_;
  TableIdToNodeMap table_id_to_node_map_;

  int64_t kernel_queue_time_ms_ = 0;
  int64_t compilation_queue_time_ms_ = 0;

  StringDictionaryProxy* ciderStringDictionaryProxy_ = nullptr;

  // Singleton instance used for an execution unit which is a project with window
  // functions.
  std::unique_ptr<WindowProjectNodeContext> window_project_node_context_owned_;
  // The active window function.
  WindowFunctionContext* active_window_function_{nullptr};

  mutable InputTableInfoCache input_table_info_cache_;
  AggregatedColRange agg_col_range_cache_;
  TableGenerations table_generations_;
  static mapd_shared_mutex executor_session_mutex_;
  // a query session that this executor manages
  QuerySessionId current_query_session_;
  // a pair of <QuerySessionId, interrupted_flag>
  static InterruptFlagMap queries_interrupt_flag_;
  // a pair of <QuerySessionId, query_session_status>
  static QuerySessionMap queries_session_map_;
  static std::map<int, std::shared_ptr<Executor>> executors_;

  // SQL queries take a shared lock, exclusive options (cache clear, memory clear) take
  // a write lock
  static mapd_shared_mutex execute_mutex_;

  struct ExecutorMutexHolder {
    mapd_shared_lock<mapd_shared_mutex> shared_lock;
    mapd_unique_lock<mapd_shared_mutex> unique_lock;
  };
  inline ExecutorMutexHolder acquireExecuteMutex() {
    ExecutorMutexHolder ret;
    if (executor_id_ == Executor::UNITARY_EXECUTOR_ID) {
      // Only one unitary executor can run at a time
      ret.unique_lock = mapd_unique_lock<mapd_shared_mutex>(execute_mutex_);
    } else {
      ret.shared_lock = mapd_shared_lock<mapd_shared_mutex>(execute_mutex_);
    }
    return ret;
  }

  static mapd_shared_mutex executors_cache_mutex_;

  static QueryPlanDagCache query_plan_dag_cache_;
  const QueryPlanHash INVALID_QUERY_PLAN_HASH{std::hash<std::string>{}(EMPTY_QUERY_PLAN)};
  static mapd_shared_mutex recycler_mutex_;
  static std::unordered_map<std::string, size_t> cardinality_cache_;

 public:
  static const int32_t ERR_DIV_BY_ZERO{1};
  static const int32_t ERR_OUT_OF_SLOTS{3};
  static const int32_t ERR_UNSUPPORTED_SELF_JOIN{4};
  static const int32_t ERR_OUT_OF_CPU_MEM{6};
  static const int32_t ERR_OVERFLOW_OR_UNDERFLOW{7};
  static const int32_t ERR_OUT_OF_TIME{9};
  static const int32_t ERR_INTERRUPTED{10};
  static const int32_t ERR_COLUMNAR_CONVERSION_NOT_SUPPORTED{11};
  static const int32_t ERR_TOO_MANY_LITERALS{12};
  static const int32_t ERR_STRING_CONST_IN_RESULTSET{13};
  static const int32_t ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES{15};
  static const int32_t ERR_WIDTH_BUCKET_INVALID_ARGUMENT{16};

  static std::mutex compilation_mutex_;
  static std::mutex kernel_mutex_;

  friend class BaselineJoinHashTable;
  friend class CodeGenerator;
  friend class ColumnFetcher;
  friend struct DiamondCodegen;  // cgen_state_
  friend class KernelSubtask;
  friend class HashJoin;  // cgen_state_
  friend class OverlapsJoinHashTable;
  friend class RangeJoinHashTable;
  friend class GroupByAndAggregate;
  friend class QueryCompilationDescriptor;
  friend class QueryMemoryDescriptor;
  friend class QueryMemoryInitializer;
  friend class QueryExecutionContext;
  friend class InValuesBitmap;
  friend class LeafAggregator;
  friend class PerfectJoinHashTable;
  friend class QueryRewriter;
  friend class PendingExecutionClosure;
  friend class TableFunctionCompilationContext;
  friend class TableFunctionExecutionContext;
  friend struct TargetExprCodegenBuilder;
  friend struct TargetExprCodegen;
  friend class WindowProjectNodeContext;
  friend class CiderHashJoin;  // cgen_state_
};

inline std::string get_null_check_suffix(const SQLTypeInfo& lhs_ti,
                                         const SQLTypeInfo& rhs_ti) {
  if (lhs_ti.get_notnull() && rhs_ti.get_notnull()) {
    return "";
  }
  std::string null_check_suffix{"_nullable"};
  if (lhs_ti.get_notnull()) {
    CHECK(!rhs_ti.get_notnull());
    null_check_suffix += "_rhs";
  } else if (rhs_ti.get_notnull()) {
    CHECK(!lhs_ti.get_notnull());
    null_check_suffix += "_lhs";
  }
  return null_check_suffix;
}

inline bool is_unnest(const Analyzer::Expr* expr) {
  return dynamic_cast<const Analyzer::UOper*>(expr) &&
         static_cast<const Analyzer::UOper*>(expr)->get_optype() == kUNNEST;
}

inline bool is_constructed_point(const Analyzer::Expr* expr) {
  auto uoper = dynamic_cast<const Analyzer::UOper*>(expr);
  auto oper = (uoper && uoper->get_optype() == kCAST) ? uoper->get_operand() : expr;
  auto arr = dynamic_cast<const Analyzer::ArrayExpr*>(oper);
  return (arr && arr->isLocalAlloc() && arr->get_type_info().is_fixlen_array());
}

bool is_trivial_loop_join(const std::vector<InputTableInfo>& query_infos,
                          const RelAlgExecutionUnit& ra_exe_unit);

const Analyzer::Expr* remove_cast_to_int(const Analyzer::Expr* expr);

#endif  // QUERYENGINE_EXECUTE_H
