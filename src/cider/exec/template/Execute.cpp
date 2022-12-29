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

#include "exec/template/Execute.h"

#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <chrono>
#include <ctime>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <thread>

#include "exec/plan/parser/ParserNode.h"
#include "exec/template/AggregateUtils.h"
#include "exec/template/AggregatedColRange.h"
#include "exec/template/CodeGenerator.h"
#include "exec/template/ColumnFetcher.h"
#include "exec/template/DynamicWatchdog.h"
#include "exec/template/ExpressionRewrite.h"
#include "exec/template/ExternalCacheInvalidators.h"
#include "exec/template/InPlaceSort.h"
#include "exec/template/JsonAccessors.h"
#include "exec/template/OutputBufferInitialization.h"
#include "exec/template/QueryDispatchQueue.h"
#include "exec/template/QueryRewrite.h"
#include "exec/template/QueryTemplateGenerator.h"
#include "exec/template/SpeculativeTopN.h"
#include "exec/template/StringDictionaryGenerations.h"
#include "exec/template/TransientStringLiteralsVisitor.h"
#include "exec/template/operator/join/EquiJoinCondition.h"
#include "exec/template/operator/join/hashtable/BaselineJoinHashTable.h"
#include "exec/template/operator/join/hashtable/OverlapsJoinHashTable.h"
#include "function/scalar/RuntimeFunctions.h"
#include "robin_hood.h"
#include "util/TypedDataAccessors.h"
#include "util/checked_alloc.h"
#include "util/filesystem/cider_path.h"
#include "util/measure.h"
#include "util/memory/DictDescriptor.h"
#include "util/misc.h"
#include "util/scope.h"
#include "util/threading.h"

bool g_enable_watchdog{false};
bool g_enable_dynamic_watchdog{false};
bool g_enable_cpu_sub_tasks{false};
size_t g_cpu_sub_task_size{500'000};
bool g_enable_filter_function{true};
unsigned g_dynamic_watchdog_time_limit{10000};
bool g_allow_cpu_retry{true};
bool g_allow_query_step_cpu_retry{true};
bool g_null_div_by_zero{false};
bool g_inf_div_by_zero{false};
unsigned g_trivial_loop_join_threshold{1000};
bool g_from_table_reordering{true};
bool g_inner_join_fragment_skipping{true};
extern bool g_enable_smem_group_by;
extern std::unique_ptr<llvm::Module> udf_cpu_module;
bool g_enable_filter_push_down{false};
float g_filter_push_down_low_frac{-1.0f};
float g_filter_push_down_high_frac{-1.0f};
size_t g_filter_push_down_passing_row_ubound{0};
bool g_enable_columnar_output{false};
bool g_enable_left_join_filter_hoisting{true};
bool g_optimize_row_initialization{true};
bool g_enable_overlaps_hashjoin{true};
bool g_enable_distance_rangejoin{true};
bool g_enable_hashjoin_many_to_many{false};
size_t g_overlaps_max_table_size_bytes{1024 * 1024 * 1024};
double g_overlaps_target_entries_per_bin{1.3};
bool g_strip_join_covered_quals{false};
size_t g_constrained_by_in_threshold{10};
size_t g_default_max_groups_buffer_entry_guess{16384};
size_t g_big_group_threshold{g_default_max_groups_buffer_entry_guess};
bool g_enable_window_functions{true};
bool g_enable_table_functions{false};
size_t g_max_memory_allocation_size{2000000000};  // set to max slab size
size_t g_min_memory_allocation_size{
    256};  // minimum memory allocation required for projection query output buffer
           // without pre-flight count
bool g_enable_bump_allocator{false};
double g_bump_allocator_step_reduction{0.75};
bool g_enable_direct_columnarization{true};
bool g_enable_experimental_string_functions{false};
bool g_enable_lazy_fetch{true};
bool g_enable_runtime_query_interrupt{true};
bool g_enable_non_kernel_time_query_interrupt{true};
bool g_use_estimator_result_cache{true};
unsigned g_pending_query_interrupt_freq{1000};
double g_running_query_interrupt_freq{0.1};
bool g_enable_smem_grouped_non_count_agg{
    true};  // enable use of shared memory when performing group-by with select non-count
            // aggregates
bool g_is_test_env{false};  // operating under a unit test environment. Currently only
                            // limits the allocation for the output buffer arena
                            // and data recycler test
size_t g_enable_parallel_linearization{
    10000};  // # rows that we are trying to linearize varlen col in parallel
bool g_enable_data_recycler{true};
bool g_use_hashtable_cache{true};
size_t g_hashtable_cache_total_bytes{size_t(1) << 32};
size_t g_max_cacheable_hashtable_size_bytes{size_t(1) << 31};

size_t g_approx_quantile_buffer{1000};
size_t g_approx_quantile_centroids{300};

bool g_enable_automatic_ir_metadata{true};

extern bool g_cache_string_hash;
bool g_enable_multifrag_rs{false};

extern std::unique_ptr<llvm::Module> read_llvm_module_from_bc_file(
    const std::string& udf_ir_filename,
    llvm::LLVMContext& ctx);
extern std::unique_ptr<llvm::Module> read_llvm_module_from_ir_file(
    const std::string& udf_ir_filename,
    llvm::LLVMContext& ctx);
extern std::unique_ptr<llvm::Module> read_llvm_module_from_ir_string(
    const std::string& udf_ir_string,
    llvm::LLVMContext& ctx);

const int32_t Executor::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES;

Executor::Executor(const ExecutorId executor_id,
                   DataProvider* data_provider,
                   BufferProvider* buffer_provider,
                   const std::string& debug_dir,
                   const std::string& debug_file)
    : executor_id_(executor_id)
    , context_(new llvm::LLVMContext())
    , cgen_state_(new CgenState({}, false, this))
    , cpu_code_cache_(code_cache_size)
    , debug_dir_(debug_dir)
    , debug_file_(debug_file)
    , data_provider_(data_provider)
    , buffer_provider_(buffer_provider)
    , temporary_tables_(nullptr)
    , stringHasher_(new CiderStringHasher())
    , input_table_info_cache_(this) {
  initialize_extension_module_sources();
  update_extension_modules();
}

void Executor::initialize_extension_module_sources() {
  if (extension_module_sources.find(Executor::ExtModuleKinds::template_module) ==
      extension_module_sources.end()) {
    auto root_path = cider::get_root_abs_path();
    auto template_path = root_path + "/function/RuntimeFunctions.bc";
    CHECK(boost::filesystem::exists(template_path));
    extension_module_sources[Executor::ExtModuleKinds::template_module] = template_path;
  }
}

void Executor::update_extension_modules(bool update_runtime_modules_only) {
  auto read_module = [&](Executor::ExtModuleKinds module_kind,
                         const std::string& source) {
    /*
      source can be either a filename of a LLVM IR
      or LLVM BC source, or a string containing
      LLVM IR code.
     */
    CHECK(!source.empty());
    switch (module_kind) {
      case Executor::ExtModuleKinds::template_module:
      case Executor::ExtModuleKinds::udf_cpu_module: {
        return read_llvm_module_from_ir_file(source, getContext());
      }
      case Executor::ExtModuleKinds::rt_udf_cpu_module: {
        return read_llvm_module_from_ir_string(source, getContext());
      }
      default: {
        UNREACHABLE();
        return std::unique_ptr<llvm::Module>();
      }
    }
  };
  auto update_module = [&](Executor::ExtModuleKinds module_kind,
                           bool erase_not_found = false) {
    auto it = extension_module_sources.find(module_kind);
    if (it != extension_module_sources.end()) {
      auto llvm_module = read_module(module_kind, it->second);
      if (llvm_module) {
        extension_modules_[module_kind] = std::move(llvm_module);
      } else if (erase_not_found) {
        extension_modules_.erase(module_kind);
      } else {
        if (extension_modules_.find(module_kind) == extension_modules_.end()) {
          LOG(WARNING) << "Failed to update " << ::toString(module_kind)
                       << " LLVM module. The module will be unavailable.";
        } else {
          LOG(WARNING) << "Failed to update " << ::toString(module_kind)
                       << " LLVM module. Using the existing module.";
        }
      }
    } else {
      if (erase_not_found) {
        extension_modules_.erase(module_kind);
      } else {
        if (extension_modules_.find(module_kind) == extension_modules_.end()) {
          LOG(WARNING) << "Source of " << ::toString(module_kind)
                       << " LLVM module is unavailable. The module will be unavailable.";
        } else {
          LOG(WARNING) << "Source of " << ::toString(module_kind)
                       << " LLVM module is unavailable. Using the existing module.";
        }
      }
    }
  };

  if (!update_runtime_modules_only) {
    // required compile-time modules, their requirements are enforced
    // by Executor::initialize_extension_module_sources():
    update_module(Executor::ExtModuleKinds::template_module);
    // load-time modules, these are optional:
    update_module(Executor::ExtModuleKinds::udf_cpu_module, true);
  }
  // run-time modules, these are optional and erasable:
  update_module(Executor::ExtModuleKinds::rt_udf_cpu_module, true);
}

// Used by StubGenerator::generateStub
Executor::CgenStateManager::CgenStateManager(Executor& executor)
    : executor_(executor)
    , lock_queue_clock_(timer_start())
    , lock_(executor_.compilation_mutex_)
    , cgen_state_(std::move(executor_.cgen_state_))  // store old CgenState instance
{
  executor_.compilation_queue_time_ms_ += timer_stop(lock_queue_clock_);
  executor_.cgen_state_.reset(new CgenState(0, false, &executor));
}

Executor::CgenStateManager::CgenStateManager(
    Executor& executor,
    const bool allow_lazy_fetch,
    const std::vector<InputTableInfo>& query_infos,
    const RelAlgExecutionUnit* ra_exe_unit)
    : executor_(executor)
    , lock_queue_clock_(timer_start())
    , lock_(executor_.compilation_mutex_)
    , cgen_state_(std::move(executor_.cgen_state_))  // store old CgenState instance
{
  executor_.compilation_queue_time_ms_ += timer_stop(lock_queue_clock_);
  // nukeOldState creates new CgenState and PlanState instances for
  // the subsequent code generation.  It also resets
  // kernel_queue_time_ms_ and compilation_queue_time_ms_ that we do
  // not currently restore.. should we accumulate these timings?
  executor_.nukeOldState(allow_lazy_fetch, query_infos, ra_exe_unit);
}

Executor::CgenStateManager::~CgenStateManager() {
  // prevent memory leak from hoisted literals
  for (auto& p : executor_.cgen_state_->row_func_hoisted_literals_) {
    auto inst = llvm::dyn_cast<llvm::LoadInst>(p.first);
    if (inst && inst->getNumUses() == 0 && inst->getParent() == nullptr) {
      // The llvm::Value instance stored in p.first is created by the
      // CodeGenerator::codegenHoistedConstantsPlaceholders method.
      p.first->deleteValue();
    }
  }
  executor_.cgen_state_->row_func_hoisted_literals_.clear();

  // move generated StringDictionaryTranslationMgrs and InValueBitmaps
  // to the old CgenState instance as the execution of the generated
  // code uses these bitmaps

  for (auto& bm : executor_.cgen_state_->in_values_bitmaps_) {
    cgen_state_->moveInValuesBitmap(bm);
  }
  // Delete worker module that may have been set by
  // set_module_shallow_copy. If QueryMustRunOnCpu is thrown, the
  // worker module is not instantiated, so the worker module needs to
  // be deleted conditionally [see "Managing LLVM modules" comment in
  // CgenState.h]:
  if (executor_.cgen_state_->module_) {
    executor_.cgen_state_->module_ = nullptr;
    //    delete executor_.cgen_state_->module_;
  }

  // restore the old CgenState instance
  executor_.cgen_state_.reset(cgen_state_.release());
}

std::shared_ptr<Executor> Executor::getExecutor(const ExecutorId executor_id,
                                                DataProvider* data_provider,
                                                BufferProvider* buffer_provider,
                                                const std::string& debug_dir,
                                                const std::string& debug_file) {
  mapd_unique_lock<mapd_shared_mutex> write_lock(executors_cache_mutex_);
  auto it = executors_.find(executor_id);
  if (it != executors_.end()) {
    return it->second;
  }

  auto executor = std::make_shared<Executor>(
      executor_id, data_provider, buffer_provider, debug_dir, debug_file);
  CHECK(executors_.insert(std::make_pair(executor_id, executor)).second);
  return executor;
}

size_t Executor::getArenaBlockSize() {
  return g_is_test_env ? 100000000 : (1UL << 32) + kArenaBlockOverhead;
}

StringDictionaryProxy* Executor::getStringDictionaryProxy(
    const int dict_id_in,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    const bool with_generation) const {
  CHECK(row_set_mem_owner);
  std::lock_guard<std::mutex> lock(
      str_dict_mutex_);  // TODO: can we use RowSetMemOwner state mutex here?
  return row_set_mem_owner->getOrAddStringDictProxy(db_id_, dict_id_in, with_generation);
}

CiderStringHasher* Executor::getCiderStringHasherHandle() const {
  return stringHasher_;
}

StringDictionaryProxy* RowSetMemoryOwner::getOrAddStringDictProxy(
    const int db_id,
    const int dict_id_in,
    const bool with_generation) {
  const int dict_id{dict_id_in < 0 ? REGULAR_DICT(dict_id_in) : dict_id_in};
  CHECK(data_provider_);
  const auto dd = data_provider_->getDictMetadata(db_id, dict_id);
  if (dd) {
    CHECK(dd->stringDict);
    CHECK_LE(dd->dictNBits, 32);
    const int64_t generation =
        with_generation ? string_dictionary_generations_.getGeneration(dict_id) : -1;
    return addStringDict(dd->stringDict, dict_id, generation);
  }
  CHECK_EQ(0, dict_id);
  if (!lit_str_dict_proxy_) {
    const DictRef dict_ref(-1, 1);
    std::shared_ptr<StringDictionary> tsd = std::make_shared<StringDictionary>(
        dict_ref, "", allocator_, false, true, g_cache_string_hash);
    lit_str_dict_proxy_.reset(new StringDictionaryProxy(
        tsd, 0, 0));  // use 0 string_dict_id to denote literal proxy
  }
  return lit_str_dict_proxy_.get();
}

quantile::TDigest* RowSetMemoryOwner::nullTDigest(double const q) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return t_digests_
      .emplace_back(std::make_unique<quantile::TDigest>(
          q, this, g_approx_quantile_buffer, g_approx_quantile_centroids))
      .get();
}

const std::shared_ptr<RowSetMemoryOwner> Executor::getRowSetMemoryOwner() const {
  return row_set_mem_owner_;
}

const TemporaryTables* Executor::getTemporaryTables() const {
  return temporary_tables_;
}

Fragmenter_Namespace::TableInfo Executor::getTableInfo(const int table_id) const {
  return input_table_info_cache_.getTableInfo(table_id);
}

const TableGeneration& Executor::getTableGeneration(const int table_id) const {
  return table_generations_.getGeneration(table_id);
}

ExpressionRange Executor::getColRange(const PhysicalInput& phys_input) const {
  return agg_col_range_cache_.getColRange(phys_input);
}

size_t Executor::getNumBytesForFetchedRow(const std::set<int>& table_ids_to_fetch) const {
  size_t num_bytes = 0;
  if (!plan_state_) {
    return 0;
  }
  for (const auto& fetched_col : plan_state_->columns_to_fetch_) {
    int table_id = fetched_col.getTableId();
    if (table_ids_to_fetch.count(table_id) == 0) {
      continue;
    }

    if (table_id < 0) {
      num_bytes += 8;
    } else {
      const auto& ti = fetched_col.getType();
      const auto sz = ti.get_type() == kTEXT && ti.get_compression() == kENCODING_DICT
                          ? 4
                          : ti.get_size();
      if (sz < 0) {
        // for varlen types, only account for the pointer/size for each row, for now
        num_bytes += 16;
      } else {
        num_bytes += sz;
      }
    }
  }
  return num_bytes;
}

bool Executor::hasLazyFetchColumns(
    const std::vector<Analyzer::Expr*>& target_exprs) const {
  CHECK(plan_state_);
  for (const auto target_expr : target_exprs) {
    if (plan_state_->isLazyFetchColumn(target_expr)) {
      return true;
    }
  }
  return false;
}

void Executor::clearMetaInfoCache() {
  input_table_info_cache_.clear();
  agg_col_range_cache_.clear();
  table_generations_.clear();
}

std::vector<int8_t> Executor::serializeLiterals(
    const std::unordered_map<int, CgenState::LiteralValues>& literals,
    const int device_id) {
  if (literals.empty()) {
    return {};
  }
  const auto dev_literals_it = literals.find(device_id);
  CHECK(dev_literals_it != literals.end());
  const auto& dev_literals = dev_literals_it->second;
  size_t lit_buf_size{0};
  std::vector<std::string> real_strings;
  std::vector<std::vector<double>> double_array_literals;
  std::vector<std::vector<int8_t>> align64_int8_array_literals;
  std::vector<std::vector<int32_t>> int32_array_literals;
  std::vector<std::vector<int8_t>> align32_int8_array_literals;
  std::vector<std::vector<int8_t>> int8_array_literals;
  for (const auto& lit : dev_literals) {
    lit_buf_size = CgenState::addAligned(lit_buf_size, CgenState::literalBytes(lit));
    if (lit.which() == 7) {
      const auto p = boost::get<std::string>(&lit);
      CHECK(p);
      real_strings.push_back(*p);
    } else if (lit.which() == 8) {
      const auto p = boost::get<std::vector<double>>(&lit);
      CHECK(p);
      double_array_literals.push_back(*p);
    } else if (lit.which() == 9) {
      const auto p = boost::get<std::vector<int32_t>>(&lit);
      CHECK(p);
      int32_array_literals.push_back(*p);
    } else if (lit.which() == 10) {
      const auto p = boost::get<std::vector<int8_t>>(&lit);
      CHECK(p);
      int8_array_literals.push_back(*p);
    } else if (lit.which() == 11) {
      const auto p = boost::get<std::pair<std::vector<int8_t>, int>>(&lit);
      CHECK(p);
      if (p->second == 64) {
        align64_int8_array_literals.push_back(p->first);
      } else if (p->second == 32) {
        align32_int8_array_literals.push_back(p->first);
      } else {
        CHECK(false);
      }
    }
  }
  if (lit_buf_size > static_cast<size_t>(std::numeric_limits<int16_t>::max())) {
    CIDER_THROW(CiderCompileException, "Too many literals in the query");
  }
  int16_t crt_real_str_off = lit_buf_size;
  for (const auto& real_str : real_strings) {
    CHECK_LE(real_str.size(), static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += real_str.size();
  }
  if (double_array_literals.size() > 0) {
    lit_buf_size = align(lit_buf_size, sizeof(double));
  }
  int16_t crt_double_arr_lit_off = lit_buf_size;
  for (const auto& double_array_literal : double_array_literals) {
    CHECK_LE(double_array_literal.size(),
             static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += double_array_literal.size() * sizeof(double);
  }
  if (align64_int8_array_literals.size() > 0) {
    lit_buf_size = align(lit_buf_size, sizeof(uint64_t));
  }
  int16_t crt_align64_int8_arr_lit_off = lit_buf_size;
  for (const auto& align64_int8_array_literal : align64_int8_array_literals) {
    CHECK_LE(align64_int8_array_literals.size(),
             static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += align64_int8_array_literal.size();
  }
  if (int32_array_literals.size() > 0) {
    lit_buf_size = align(lit_buf_size, sizeof(int32_t));
  }
  int16_t crt_int32_arr_lit_off = lit_buf_size;
  for (const auto& int32_array_literal : int32_array_literals) {
    CHECK_LE(int32_array_literal.size(),
             static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += int32_array_literal.size() * sizeof(int32_t);
  }
  if (align32_int8_array_literals.size() > 0) {
    lit_buf_size = align(lit_buf_size, sizeof(int32_t));
  }
  int16_t crt_align32_int8_arr_lit_off = lit_buf_size;
  for (const auto& align32_int8_array_literal : align32_int8_array_literals) {
    CHECK_LE(align32_int8_array_literals.size(),
             static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += align32_int8_array_literal.size();
  }
  int16_t crt_int8_arr_lit_off = lit_buf_size;
  for (const auto& int8_array_literal : int8_array_literals) {
    CHECK_LE(int8_array_literal.size(),
             static_cast<size_t>(std::numeric_limits<int16_t>::max()));
    lit_buf_size += int8_array_literal.size();
  }
  unsigned crt_real_str_idx = 0;
  unsigned crt_double_arr_lit_idx = 0;
  unsigned crt_align64_int8_arr_lit_idx = 0;
  unsigned crt_int32_arr_lit_idx = 0;
  unsigned crt_align32_int8_arr_lit_idx = 0;
  unsigned crt_int8_arr_lit_idx = 0;
  std::vector<int8_t> serialized(lit_buf_size);
  size_t off{0};
  for (const auto& lit : dev_literals) {
    const auto lit_bytes = CgenState::literalBytes(lit);
    off = CgenState::addAligned(off, lit_bytes);
    switch (lit.which()) {
      case 0: {
        const auto p = boost::get<int8_t>(&lit);
        CHECK(p);
        serialized[off - lit_bytes] = *p;
        break;
      }
      case 1: {
        const auto p = boost::get<int16_t>(&lit);
        CHECK(p);
        memcpy(&serialized[off - lit_bytes], p, lit_bytes);
        break;
      }
      case 2: {
        const auto p = boost::get<int32_t>(&lit);
        CHECK(p);
        memcpy(&serialized[off - lit_bytes], p, lit_bytes);
        break;
      }
      case 3: {
        const auto p = boost::get<int64_t>(&lit);
        CHECK(p);
        memcpy(&serialized[off - lit_bytes], p, lit_bytes);
        break;
      }
      case 4: {
        const auto p = boost::get<float>(&lit);
        CHECK(p);
        memcpy(&serialized[off - lit_bytes], p, lit_bytes);
        break;
      }
      case 5: {
        const auto p = boost::get<double>(&lit);
        CHECK(p);
        memcpy(&serialized[off - lit_bytes], p, lit_bytes);
        break;
      }
      case 6: {
        const auto p = boost::get<std::pair<std::string, int>>(&lit);
        CHECK(p);
        const auto str_id =
            g_enable_experimental_string_functions
                ? getStringDictionaryProxy(p->second, row_set_mem_owner_, true)
                      ->getOrAddTransient(p->first)
                : getStringDictionaryProxy(p->second, row_set_mem_owner_, true)
                      ->getIdOfString(p->first);
        memcpy(&serialized[off - lit_bytes], &str_id, lit_bytes);
        break;
      }
      case 7: {
        const auto p = boost::get<std::string>(&lit);
        CHECK(p);
        int32_t off_and_len = crt_real_str_off << 16;
        const auto& crt_real_str = real_strings[crt_real_str_idx];
        off_and_len |= static_cast<int16_t>(crt_real_str.size());
        memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
        memcpy(&serialized[crt_real_str_off], crt_real_str.data(), crt_real_str.size());
        ++crt_real_str_idx;
        crt_real_str_off += crt_real_str.size();
        break;
      }
      case 8: {
        const auto p = boost::get<std::vector<double>>(&lit);
        CHECK(p);
        int32_t off_and_len = crt_double_arr_lit_off << 16;
        const auto& crt_double_arr_lit = double_array_literals[crt_double_arr_lit_idx];
        int32_t len = crt_double_arr_lit.size();
        CHECK_EQ((len >> 16), 0);
        off_and_len |= static_cast<int16_t>(len);
        int32_t double_array_bytesize = len * sizeof(double);
        memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
        memcpy(&serialized[crt_double_arr_lit_off],
               crt_double_arr_lit.data(),
               double_array_bytesize);
        ++crt_double_arr_lit_idx;
        crt_double_arr_lit_off += double_array_bytesize;
        break;
      }
      case 9: {
        const auto p = boost::get<std::vector<int32_t>>(&lit);
        CHECK(p);
        int32_t off_and_len = crt_int32_arr_lit_off << 16;
        const auto& crt_int32_arr_lit = int32_array_literals[crt_int32_arr_lit_idx];
        int32_t len = crt_int32_arr_lit.size();
        CHECK_EQ((len >> 16), 0);
        off_and_len |= static_cast<int16_t>(len);
        int32_t int32_array_bytesize = len * sizeof(int32_t);
        memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
        memcpy(&serialized[crt_int32_arr_lit_off],
               crt_int32_arr_lit.data(),
               int32_array_bytesize);
        ++crt_int32_arr_lit_idx;
        crt_int32_arr_lit_off += int32_array_bytesize;
        break;
      }
      case 10: {
        const auto p = boost::get<std::vector<int8_t>>(&lit);
        CHECK(p);
        int32_t off_and_len = crt_int8_arr_lit_off << 16;
        const auto& crt_int8_arr_lit = int8_array_literals[crt_int8_arr_lit_idx];
        int32_t len = crt_int8_arr_lit.size();
        CHECK_EQ((len >> 16), 0);
        off_and_len |= static_cast<int16_t>(len);
        int32_t int8_array_bytesize = len;
        memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
        memcpy(&serialized[crt_int8_arr_lit_off],
               crt_int8_arr_lit.data(),
               int8_array_bytesize);
        ++crt_int8_arr_lit_idx;
        crt_int8_arr_lit_off += int8_array_bytesize;
        break;
      }
      case 11: {
        const auto p = boost::get<std::pair<std::vector<int8_t>, int>>(&lit);
        CHECK(p);
        if (p->second == 64) {
          int32_t off_and_len = crt_align64_int8_arr_lit_off << 16;
          const auto& crt_align64_int8_arr_lit =
              align64_int8_array_literals[crt_align64_int8_arr_lit_idx];
          int32_t len = crt_align64_int8_arr_lit.size();
          CHECK_EQ((len >> 16), 0);
          off_and_len |= static_cast<int16_t>(len);
          int32_t align64_int8_array_bytesize = len;
          memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
          memcpy(&serialized[crt_align64_int8_arr_lit_off],
                 crt_align64_int8_arr_lit.data(),
                 align64_int8_array_bytesize);
          ++crt_align64_int8_arr_lit_idx;
          crt_align64_int8_arr_lit_off += align64_int8_array_bytesize;
        } else if (p->second == 32) {
          int32_t off_and_len = crt_align32_int8_arr_lit_off << 16;
          const auto& crt_align32_int8_arr_lit =
              align32_int8_array_literals[crt_align32_int8_arr_lit_idx];
          int32_t len = crt_align32_int8_arr_lit.size();
          CHECK_EQ((len >> 16), 0);
          off_and_len |= static_cast<int16_t>(len);
          int32_t align32_int8_array_bytesize = len;
          memcpy(&serialized[off - lit_bytes], &off_and_len, lit_bytes);
          memcpy(&serialized[crt_align32_int8_arr_lit_off],
                 crt_align32_int8_arr_lit.data(),
                 align32_int8_array_bytesize);
          ++crt_align32_int8_arr_lit_idx;
          crt_align32_int8_arr_lit_off += align32_int8_array_bytesize;
        } else {
          CHECK(false);
        }
        break;
      }
      default:
        CHECK(false);
    }
  }
  return serialized;
}

// TODO(alex): remove or split
std::pair<int64_t, int32_t> Executor::reduceResults(const SQLAgg agg,
                                                    const SQLTypeInfo& ti,
                                                    const int64_t agg_init_val,
                                                    const int8_t out_byte_width,
                                                    const int64_t* out_vec,
                                                    const size_t out_vec_sz,
                                                    const bool is_group_by,
                                                    const bool float_argument_input) {
  switch (agg) {
    case kAVG:
    case kSUM:
      if (0 != agg_init_val) {
        if (ti.is_integer() || ti.is_decimal() || ti.is_time() || ti.is_boolean()) {
          int64_t agg_result = agg_init_val;
          for (size_t i = 0; i < out_vec_sz; ++i) {
            agg_sum_skip_val(&agg_result, out_vec[i], agg_init_val);
          }
          return {agg_result, 0};
        } else {
          CHECK(ti.is_fp());
          switch (out_byte_width) {
            case 4: {
              int agg_result = static_cast<int32_t>(agg_init_val);
              for (size_t i = 0; i < out_vec_sz; ++i) {
                agg_sum_float_skip_val(
                    &agg_result,
                    *reinterpret_cast<const float*>(may_alias_ptr(&out_vec[i])),
                    *reinterpret_cast<const float*>(may_alias_ptr(&agg_init_val)));
              }
              const int64_t converted_bin =
                  float_argument_input
                      ? static_cast<int64_t>(agg_result)
                      : float_to_double_bin(static_cast<int32_t>(agg_result), true);
              return {converted_bin, 0};
              break;
            }
            case 8: {
              int64_t agg_result = agg_init_val;
              for (size_t i = 0; i < out_vec_sz; ++i) {
                agg_sum_double_skip_val(
                    &agg_result,
                    *reinterpret_cast<const double*>(may_alias_ptr(&out_vec[i])),
                    *reinterpret_cast<const double*>(may_alias_ptr(&agg_init_val)));
              }
              return {agg_result, 0};
              break;
            }
            default:
              CHECK(false);
          }
        }
      }
      if (ti.is_integer() || ti.is_decimal() || ti.is_time()) {
        int64_t agg_result = 0;
        for (size_t i = 0; i < out_vec_sz; ++i) {
          agg_result += out_vec[i];
        }
        return {agg_result, 0};
      } else {
        CHECK(ti.is_fp());
        switch (out_byte_width) {
          case 4: {
            float r = 0.;
            for (size_t i = 0; i < out_vec_sz; ++i) {
              r += *reinterpret_cast<const float*>(may_alias_ptr(&out_vec[i]));
            }
            const auto float_bin = *reinterpret_cast<const int32_t*>(may_alias_ptr(&r));
            const int64_t converted_bin =
                float_argument_input ? float_bin : float_to_double_bin(float_bin, true);
            return {converted_bin, 0};
          }
          case 8: {
            double r = 0.;
            for (size_t i = 0; i < out_vec_sz; ++i) {
              r += *reinterpret_cast<const double*>(may_alias_ptr(&out_vec[i]));
            }
            return {*reinterpret_cast<const int64_t*>(may_alias_ptr(&r)), 0};
          }
          default:
            CHECK(false);
        }
      }
      break;
    case kCOUNT: {
      uint64_t agg_result = 0;
      for (size_t i = 0; i < out_vec_sz; ++i) {
        const uint64_t out = static_cast<uint64_t>(out_vec[i]);
        agg_result += out;
      }
      return {static_cast<int64_t>(agg_result), 0};
    }
    case kMIN: {
      if (ti.is_integer() || ti.is_decimal() || ti.is_time() || ti.is_boolean()) {
        int64_t agg_result = agg_init_val;
        for (size_t i = 0; i < out_vec_sz; ++i) {
          agg_min_skip_val(&agg_result, out_vec[i], agg_init_val);
        }
        return {agg_result, 0};
      } else {
        switch (out_byte_width) {
          case 4: {
            int32_t agg_result = static_cast<int32_t>(agg_init_val);
            for (size_t i = 0; i < out_vec_sz; ++i) {
              agg_min_float_skip_val(
                  &agg_result,
                  *reinterpret_cast<const float*>(may_alias_ptr(&out_vec[i])),
                  *reinterpret_cast<const float*>(may_alias_ptr(&agg_init_val)));
            }
            const int64_t converted_bin =
                float_argument_input
                    ? static_cast<int64_t>(agg_result)
                    : float_to_double_bin(static_cast<int32_t>(agg_result), true);
            return {converted_bin, 0};
          }
          case 8: {
            int64_t agg_result = agg_init_val;
            for (size_t i = 0; i < out_vec_sz; ++i) {
              agg_min_double_skip_val(
                  &agg_result,
                  *reinterpret_cast<const double*>(may_alias_ptr(&out_vec[i])),
                  *reinterpret_cast<const double*>(may_alias_ptr(&agg_init_val)));
            }
            return {agg_result, 0};
          }
          default:
            CHECK(false);
        }
      }
    }
    case kMAX:
      if (ti.is_integer() || ti.is_decimal() || ti.is_time() || ti.is_boolean()) {
        int64_t agg_result = agg_init_val;
        for (size_t i = 0; i < out_vec_sz; ++i) {
          agg_max_skip_val(&agg_result, out_vec[i], agg_init_val);
        }
        return {agg_result, 0};
      } else {
        switch (out_byte_width) {
          case 4: {
            int32_t agg_result = static_cast<int32_t>(agg_init_val);
            for (size_t i = 0; i < out_vec_sz; ++i) {
              agg_max_float_skip_val(
                  &agg_result,
                  *reinterpret_cast<const float*>(may_alias_ptr(&out_vec[i])),
                  *reinterpret_cast<const float*>(may_alias_ptr(&agg_init_val)));
            }
            const int64_t converted_bin =
                float_argument_input ? static_cast<int64_t>(agg_result)
                                     : float_to_double_bin(agg_result, !ti.get_notnull());
            return {converted_bin, 0};
          }
          case 8: {
            int64_t agg_result = agg_init_val;
            for (size_t i = 0; i < out_vec_sz; ++i) {
              agg_max_double_skip_val(
                  &agg_result,
                  *reinterpret_cast<const double*>(may_alias_ptr(&out_vec[i])),
                  *reinterpret_cast<const double*>(may_alias_ptr(&agg_init_val)));
            }
            return {agg_result, 0};
          }
          default:
            CHECK(false);
        }
      }
    case kSINGLE_VALUE: {
      int64_t agg_result = agg_init_val;
      for (size_t i = 0; i < out_vec_sz; ++i) {
        if (out_vec[i] != agg_init_val) {
          if (agg_result == agg_init_val) {
            agg_result = out_vec[i];
          } else if (out_vec[i] != agg_result) {
            return {agg_result, Executor::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES};
          }
        }
      }
      return {agg_result, 0};
    }
    case kSAMPLE: {
      int64_t agg_result = agg_init_val;
      for (size_t i = 0; i < out_vec_sz; ++i) {
        if (out_vec[i] != agg_init_val) {
          agg_result = out_vec[i];
          break;
        }
      }
      return {agg_result, 0};
    }
    default:
      CHECK(false);
  }
  CIDER_THROW(CiderUnsupportedException, fmt::format("agg is {}", agg));
}

namespace {

TemporaryTable get_separate_results(
    std::vector<std::pair<ResultSetPtr, std::vector<size_t>>>& results_per_device) {
  std::vector<ResultSetPtr> results;
  results.reserve(results_per_device.size());
  for (auto& r : results_per_device) {
    results.emplace_back(r.first);
  }
  return TemporaryTable(std::move(results));
}

}  // namespace

bool couldUseParallelReduce(const QueryMemoryDescriptor& desc) {
  if (desc.getQueryDescriptionType() == QueryDescriptionType::NonGroupedAggregate &&
      desc.getCountDistinctDescriptorsSize()) {
    return true;
  }

  if (desc.getQueryDescriptionType() == QueryDescriptionType::GroupByPerfectHash) {
    return true;
  }

  return false;
}

namespace {

// Compute a very conservative entry count for the output buffer entry count using no
// other information than the number of tuples in each table and multiplying them
// together.
size_t compute_buffer_entry_guess(const std::vector<InputTableInfo>& query_infos) {
  using Fragmenter_Namespace::FragmentInfo;
  // Check for overflows since we're multiplying potentially big table sizes.
  using checked_size_t = boost::multiprecision::number<
      boost::multiprecision::cpp_int_backend<64,
                                             64,
                                             boost::multiprecision::unsigned_magnitude,
                                             boost::multiprecision::checked,
                                             void>>;
  checked_size_t max_groups_buffer_entry_guess = 1;
  for (const auto& query_info : query_infos) {
    CHECK(!query_info.info.fragments.empty());
    auto it = std::max_element(query_info.info.fragments.begin(),
                               query_info.info.fragments.end(),
                               [](const FragmentInfo& f1, const FragmentInfo& f2) {
                                 return f1.getNumTuples() < f2.getNumTuples();
                               });
    max_groups_buffer_entry_guess *= it->getNumTuples();
  }
  // Cap the rough approximation to 100M entries, it's unlikely we can do a great job for
  // baseline group layout with that many entries anyway.
  constexpr size_t max_groups_buffer_entry_guess_cap = 100000000;
  try {
    return std::min(static_cast<size_t>(max_groups_buffer_entry_guess),
                    max_groups_buffer_entry_guess_cap);
  } catch (...) {
    return max_groups_buffer_entry_guess_cap;
  }
}

std::string get_table_name(int db_id,
                           const InputDescriptor& input_desc,
                           const SchemaProvider& schema_provider) {
  const auto source_type = input_desc.getSourceType();
  if (source_type == InputSourceType::TABLE) {
    const auto tinfo = schema_provider.getTableInfo(db_id, input_desc.getTableId());
    CHECK(tinfo);
    return tinfo->name;
  } else {
    return "$TEMPORARY_TABLE" + std::to_string(-input_desc.getTableId());
  }
}

inline size_t getDeviceBasedScanLimit(const int device_count) {
  return Executor::high_scan_limit;
}

}  // namespace

bool is_trivial_loop_join(const std::vector<InputTableInfo>& query_infos,
                          const RelAlgExecutionUnit& ra_exe_unit) {
  if (ra_exe_unit.input_descs.size() < 2) {
    return false;
  }

  // We only support loop join at the end of folded joins
  // where ra_exe_unit.input_descs.size() > 2 for now.
  const auto inner_table_id = ra_exe_unit.input_descs.back().getTableId();

  std::optional<size_t> inner_table_idx;
  for (size_t i = 0; i < query_infos.size(); ++i) {
    if (query_infos[i].table_id == inner_table_id) {
      inner_table_idx = i;
      break;
    }
  }
  CHECK(inner_table_idx);
  return query_infos[*inner_table_idx].info.getNumTuples() <=
         g_trivial_loop_join_threshold;
}

namespace {

template <typename T>
std::vector<std::string> expr_container_to_string(const T& expr_container) {
  std::vector<std::string> expr_strs;
  for (const auto& expr : expr_container) {
    if (!expr) {
      expr_strs.emplace_back("NULL");
    } else {
      expr_strs.emplace_back(expr->toString());
    }
  }
  return expr_strs;
}

template <>
std::vector<std::string> expr_container_to_string(
    const std::list<Analyzer::OrderEntry>& expr_container) {
  std::vector<std::string> expr_strs;
  for (const auto& expr : expr_container) {
    expr_strs.emplace_back(expr.toString());
  }
  return expr_strs;
}

std::string sort_algorithm_to_string(const SortAlgorithm algorithm) {
  switch (algorithm) {
    case SortAlgorithm::Default:
      return "ResultSet";
    case SortAlgorithm::SpeculativeTopN:
      return "Speculative Top N";
    case SortAlgorithm::StreamingTopN:
      return "Streaming Top N";
  }
  UNREACHABLE();
  return "";
}
}  // namespace

std::string ra_exec_unit_desc_for_caching(const RelAlgExecutionUnit& ra_exe_unit) {
  // todo(yoonmin): replace a cache key as a DAG representation of a query plan
  // instead of ra_exec_unit description if possible
  std::ostringstream os;
  for (const auto& input_col_desc : ra_exe_unit.input_col_descs) {
    os << input_col_desc->getTableId() << "," << input_col_desc->getColId() << ","
       << input_col_desc->getNestLevel();
  }
  if (!ra_exe_unit.simple_quals.empty()) {
    for (const auto& qual : ra_exe_unit.simple_quals) {
      if (qual) {
        os << qual->toString() << ",";
      }
    }
  }
  if (!ra_exe_unit.quals.empty()) {
    for (const auto& qual : ra_exe_unit.quals) {
      if (qual) {
        os << qual->toString() << ",";
      }
    }
  }
  if (!ra_exe_unit.join_quals.empty()) {
    for (size_t i = 0; i < ra_exe_unit.join_quals.size(); i++) {
      const auto& join_condition = ra_exe_unit.join_quals[i];
      os << std::to_string(i) << ::toString(join_condition.type);
      for (const auto& qual : join_condition.quals) {
        if (qual) {
          os << qual->toString() << ",";
        }
      }
    }
  }
  if (!ra_exe_unit.groupby_exprs.empty()) {
    for (const auto& qual : ra_exe_unit.groupby_exprs) {
      if (qual) {
        os << qual->toString() << ",";
      }
    }
  }
  for (const auto& expr : ra_exe_unit.target_exprs) {
    if (expr) {
      os << expr->toString() << ",";
    }
  }
  os << ::toString(ra_exe_unit.estimator == nullptr);
  os << std::to_string(ra_exe_unit.scan_limit);
  return os.str();
}

std::ostream& operator<<(std::ostream& os, const RelAlgExecutionUnit& ra_exe_unit) {
  auto query_plan_dag =
      ra_exe_unit.query_plan_dag == EMPTY_QUERY_PLAN ? "N/A" : ra_exe_unit.query_plan_dag;
  os << "\n\tExtracted Query Plan Dag: " << query_plan_dag;
  os << "\n\tTable/Levels: ";
  for (const auto& input_descs : ra_exe_unit.input_descs) {
    os << input_descs.toString();
  }
  os << "\n\tTable/Col/Levels: ";
  for (const auto& input_col_desc : ra_exe_unit.input_col_descs) {
    os << "(" << input_col_desc->getTableId() << ", " << input_col_desc->getColId()
       << ", " << input_col_desc->getNestLevel() << ") ";
  }
  if (!ra_exe_unit.simple_quals.empty()) {
    os << "\n\tSimple Quals: "
       << boost::algorithm::join(expr_container_to_string(ra_exe_unit.simple_quals),
                                 ", ");
  }
  if (!ra_exe_unit.quals.empty()) {
    os << "\n\tQuals: "
       << boost::algorithm::join(expr_container_to_string(ra_exe_unit.quals), ", ");
  }
  if (!ra_exe_unit.join_quals.empty()) {
    os << "\n\tJoin Quals: ";
    for (size_t i = 0; i < ra_exe_unit.join_quals.size(); i++) {
      const auto& join_condition = ra_exe_unit.join_quals[i];
      os << "\t\t" << std::to_string(i) << " " << ::toString(join_condition.type);
      os << boost::algorithm::join(expr_container_to_string(join_condition.quals), ", ");
    }
  }
  if (!ra_exe_unit.groupby_exprs.empty()) {
    os << "\n\tGroup By: "
       << boost::algorithm::join(expr_container_to_string(ra_exe_unit.groupby_exprs),
                                 ", ");
  }
  os << "\n\tProjected targets: "
     << boost::algorithm::join(expr_container_to_string(ra_exe_unit.target_exprs), ", ");
  os << "\n\tHas Estimator: " << ::toString(ra_exe_unit.estimator == nullptr);
  os << "\n\tSort Info: ";
  const auto& sort_info = ra_exe_unit.sort_info;
  os << "\n\t  Order Entries: "
     << boost::algorithm::join(expr_container_to_string(sort_info.order_entries), ", ");
  os << "\n\t  Algorithm: " << sort_algorithm_to_string(sort_info.algorithm);
  os << "\n\t  Limit: " << std::to_string(sort_info.limit);
  os << "\n\t  Offset: " << std::to_string(sort_info.offset);
  os << "\n\tScan Limit: " << std::to_string(ra_exe_unit.scan_limit);
  os << "\n\tBump Allocator: " << ::toString(ra_exe_unit.use_bump_allocator);
  if (ra_exe_unit.union_all) {
    os << "\n\tUnion: " << std::string(*ra_exe_unit.union_all ? "UNION ALL" : "UNION");
  }
  return os;
}

namespace {

RelAlgExecutionUnit replace_scan_limit(const RelAlgExecutionUnit& ra_exe_unit_in,
                                       const size_t new_scan_limit) {
  return {ra_exe_unit_in.input_descs,
          ra_exe_unit_in.input_col_descs,
          ra_exe_unit_in.simple_quals,
          ra_exe_unit_in.quals,
          ra_exe_unit_in.join_quals,
          ra_exe_unit_in.groupby_exprs,
          ra_exe_unit_in.target_exprs,
          ra_exe_unit_in.estimator,
          ra_exe_unit_in.sort_info,
          new_scan_limit,
          ra_exe_unit_in.query_hint,
          ra_exe_unit_in.query_plan_dag,
          ra_exe_unit_in.hash_table_build_plan_dag,
          ra_exe_unit_in.table_id_to_node_map,
          ra_exe_unit_in.use_bump_allocator,
          ra_exe_unit_in.union_all};
}

}  // namespace

void Executor::addTransientStringLiterals(
    const RelAlgExecutionUnit& ra_exe_unit,
    const std::shared_ptr<RowSetMemoryOwner>& row_set_mem_owner) {
  TransientDictIdVisitor dict_id_visitor;

  auto visit_expr =
      [this, &dict_id_visitor, &row_set_mem_owner](const Analyzer::Expr* expr) {
        if (!expr) {
          return;
        }
        const auto dict_id = dict_id_visitor.visit(expr);
        if (dict_id >= 0) {
          auto sdp = getStringDictionaryProxy(dict_id, row_set_mem_owner, true);
          CHECK(sdp);
          TransientStringLiteralsVisitor visitor(sdp);
          visitor.visit(expr);
        }
      };

  for (const auto& group_expr : ra_exe_unit.groupby_exprs) {
    visit_expr(group_expr.get());
  }

  for (const auto& group_expr : ra_exe_unit.quals) {
    visit_expr(group_expr.get());
  }

  for (const auto& group_expr : ra_exe_unit.simple_quals) {
    visit_expr(group_expr.get());
  }

  for (const auto target_expr : ra_exe_unit.target_exprs) {
    const auto& target_type = target_expr->get_type_info();
    if (target_type.is_string() && target_type.get_compression() != kENCODING_DICT) {
      continue;
    }
    const auto agg_expr = dynamic_cast<const Analyzer::AggExpr*>(target_expr);
    if (agg_expr) {
      if (agg_expr->get_aggtype() == kSINGLE_VALUE ||
          agg_expr->get_aggtype() == kSAMPLE) {
        visit_expr(agg_expr->get_arg());
      }
    } else {
      visit_expr(target_expr);
    }
  }
}

namespace {

int64_t inline_null_val(const SQLTypeInfo& ti, const bool float_argument_input) {
  CHECK(ti.is_number() || ti.is_time() || ti.is_boolean() || ti.is_string());
  if (ti.is_fp()) {
    if (float_argument_input && ti.get_type() == kFLOAT) {
      int64_t float_null_val = 0;
      *reinterpret_cast<float*>(may_alias_ptr(&float_null_val)) =
          static_cast<float>(inline_fp_null_val(ti));
      return float_null_val;
    }
    const auto double_null_val = inline_fp_null_val(ti);
    return *reinterpret_cast<const int64_t*>(may_alias_ptr(&double_null_val));
  }
  return inline_int_null_val(ti);
}

void fill_entries_for_empty_input(std::vector<TargetInfo>& target_infos,
                                  std::vector<int64_t>& entry,
                                  const std::vector<Analyzer::Expr*>& target_exprs,
                                  const QueryMemoryDescriptor& query_mem_desc) {
  for (size_t target_idx = 0; target_idx < target_exprs.size(); ++target_idx) {
    const auto target_expr = target_exprs[target_idx];
    const auto agg_info = get_target_info(target_expr, g_bigint_count);
    CHECK(agg_info.is_agg);
    target_infos.push_back(agg_info);
    const bool float_argument_input = takes_float_argument(agg_info);
    if (agg_info.agg_kind == kCOUNT || agg_info.agg_kind == kAPPROX_COUNT_DISTINCT) {
      entry.push_back(0);
    } else if (agg_info.agg_kind == kAVG) {
      entry.push_back(0);
      entry.push_back(0);
    } else if (agg_info.agg_kind == kSINGLE_VALUE || agg_info.agg_kind == kSAMPLE) {
      if (agg_info.sql_type.is_varlen()) {
        entry.push_back(0);
        entry.push_back(0);
      } else {
        entry.push_back(inline_null_val(agg_info.sql_type, float_argument_input));
      }
    } else {
      entry.push_back(inline_null_val(agg_info.sql_type, float_argument_input));
    }
  }
}
}  // namespace

std::unordered_map<int, const Analyzer::BinOper*> Executor::getInnerTabIdToJoinCond()
    const {
  std::unordered_map<int, const Analyzer::BinOper*> id_to_cond;
  const auto& join_info = plan_state_->join_info_;
  CHECK_EQ(join_info.equi_join_tautologies_.size(), join_info.join_hash_tables_.size());
  for (size_t i = 0; i < join_info.join_hash_tables_.size(); ++i) {
    int inner_table_id = join_info.join_hash_tables_[i]->getInnerTableId();
    id_to_cond.insert(
        std::make_pair(inner_table_id, join_info.equi_join_tautologies_[i].get()));
  }
  return id_to_cond;
}

namespace {

class OutVecOwner {
 public:
  OutVecOwner(const std::vector<int64_t*>& out_vec) : out_vec_(out_vec) {}
  ~OutVecOwner() {
    for (auto out : out_vec_) {
      delete[] out;
    }
  }

 private:
  std::vector<int64_t*> out_vec_;
};
}  // namespace

std::vector<int64_t> Executor::getJoinHashTablePtrs(const int device_id) {
  std::vector<int64_t> table_ptrs;
  const auto& join_hash_tables = plan_state_->join_info_.join_hash_tables_;
  for (auto hash_table : join_hash_tables) {
    if (!hash_table) {
      CHECK(table_ptrs.empty());
      return {};
    }
    table_ptrs.push_back(hash_table->getJoinHashBuffer(0));
  }
  return table_ptrs;
}

void Executor::nukeOldState(const bool allow_lazy_fetch,
                            const std::vector<InputTableInfo>& query_infos,
                            const RelAlgExecutionUnit* ra_exe_unit) {
  kernel_queue_time_ms_ = 0;
  compilation_queue_time_ms_ = 0;
  const bool contains_left_deep_outer_join =
      ra_exe_unit && std::find_if(ra_exe_unit->join_quals.begin(),
                                  ra_exe_unit->join_quals.end(),
                                  [](const JoinCondition& join_condition) {
                                    return join_condition.type == JoinType::LEFT;
                                  }) != ra_exe_unit->join_quals.end();
  cgen_state_.reset(
      new CgenState(query_infos.size(), contains_left_deep_outer_join, this));
  plan_state_.reset(new PlanState(
      allow_lazy_fetch && !contains_left_deep_outer_join, query_infos, this));
}

void Executor::preloadFragOffsets(const std::vector<InputDescriptor>& input_descs,
                                  const std::vector<InputTableInfo>& query_infos) {
  AUTOMATIC_IR_METADATA(cgen_state_.get());
  const auto ld_count = input_descs.size();
  auto frag_off_ptr = get_arg_by_name(cgen_state_->row_func_, "frag_row_off");
  for (size_t i = 0; i < ld_count; ++i) {
    CHECK_LT(i, query_infos.size());
    const auto frag_count = query_infos[i].info.fragments.size();
    if (i > 0) {
      cgen_state_->frag_offsets_.push_back(nullptr);
    } else {
      if (frag_count > 1) {
        cgen_state_->frag_offsets_.push_back(
            cgen_state_->ir_builder_.CreateLoad(frag_off_ptr));
      } else {
        cgen_state_->frag_offsets_.push_back(nullptr);
      }
    }
  }
}

Executor::JoinHashTableOrError Executor::buildHashTableForQualifier(
    const std::shared_ptr<Analyzer::BinOper>& qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const MemoryLevel memory_level,
    const JoinType join_type,
    const HashType preferred_hash_type,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    const HashTableBuildDagMap& hashtable_build_dag_map,
    const RegisteredQueryHint& query_hint,
    const TableIdToNodeMap& table_id_to_node_map) {
  if (g_enable_dynamic_watchdog && interrupted_.load()) {
    CIDER_THROW(CiderRuntimeException,
                fmt::format("Query execution failed with error code {}",
                            std::to_string(ERR_INTERRUPTED)));
  }
  try {
    auto tbl = HashJoin::getInstance(qual_bin_oper,
                                     query_infos,
                                     memory_level,
                                     join_type,
                                     preferred_hash_type,
                                     1,
                                     data_provider,
                                     column_cache,
                                     this,
                                     hashtable_build_dag_map,
                                     query_hint,
                                     table_id_to_node_map);
    return {tbl, ""};
  } catch (const CiderHashJoinException& e) {
    return {nullptr, e.what()};
  }
}

llvm::Value* Executor::castToFP(llvm::Value* value,
                                SQLTypeInfo const& from_ti,
                                SQLTypeInfo const& to_ti) {
  AUTOMATIC_IR_METADATA(cgen_state_.get());
  if (value->getType()->isIntegerTy() && from_ti.is_number() && to_ti.is_fp() &&
      (!from_ti.is_fp() || from_ti.get_size() != to_ti.get_size())) {
    llvm::Type* fp_type{nullptr};
    switch (to_ti.get_size()) {
      case 4:
        fp_type = llvm::Type::getFloatTy(cgen_state_->context_);
        break;
      case 8:
        fp_type = llvm::Type::getDoubleTy(cgen_state_->context_);
        break;
      default:
        LOG(FATAL) << "Unsupported FP size: " << to_ti.get_size();
    }
    value = cgen_state_->ir_builder_.CreateSIToFP(value, fp_type);
    if (from_ti.get_scale()) {
      value = cgen_state_->ir_builder_.CreateFDiv(
          value,
          llvm::ConstantFP::get(value->getType(), exp_to_scale(from_ti.get_scale())));
    }
  }
  return value;
}

llvm::Value* Executor::castToIntPtrTyIn(llvm::Value* val, const size_t bitWidth) {
  AUTOMATIC_IR_METADATA(cgen_state_.get());
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
  if (bitWidth == val_width) {
    return val;
  }
  return cgen_state_->ir_builder_.CreateBitCast(
      val, llvm::PointerType::get(get_int_type(bitWidth, cgen_state_->context_), 0));
}

#define EXECUTE_INCLUDE
#include "function/datetime/DateAdd.cpp"
#include "function/string/StringFunctions.cpp"
#include "function/vector/ArrayOps.cpp"
#undef EXECUTE_INCLUDE

namespace {
// Note(Wamsi): `get_hpt_overflow_underflow_safe_scaled_value` will return `true` for safe
// scaled epoch value and `false` for overflow/underflow values as the first argument of
// return type.
std::tuple<bool, int64_t, int64_t> get_hpt_overflow_underflow_safe_scaled_values(
    const int64_t chunk_min,
    const int64_t chunk_max,
    const SQLTypeInfo& lhs_type,
    const SQLTypeInfo& rhs_type) {
  const int32_t ldim = lhs_type.get_dimension();
  const int32_t rdim = rhs_type.get_dimension();
  CHECK(ldim != rdim);
  const auto scale = DateTimeUtils::get_timestamp_precision_scale(abs(rdim - ldim));
  if (ldim > rdim) {
    // LHS type precision is more than RHS col type. No chance of overflow/underflow.
    return {true, chunk_min / scale, chunk_max / scale};
  }

  using checked_int64_t = boost::multiprecision::number<
      boost::multiprecision::cpp_int_backend<64,
                                             64,
                                             boost::multiprecision::signed_magnitude,
                                             boost::multiprecision::checked,
                                             void>>;
  try {
    auto ret =
        std::make_tuple(true,
                        int64_t(checked_int64_t(chunk_min) * checked_int64_t(scale)),
                        int64_t(checked_int64_t(chunk_max) * checked_int64_t(scale)));
    return ret;
  } catch (const std::overflow_error& e) {
    // noop
  }
  return std::make_tuple(false, chunk_min, chunk_max);
}

}  // namespace

AggregatedColRange Executor::computeColRangesCache(
    const std::unordered_set<InputColDescriptor>& col_descs) {
  AggregatedColRange agg_col_range_cache;
  TableRefSet phys_table_refs;
  for (const auto& col_desc : col_descs) {
    phys_table_refs.insert({col_desc.getDatabaseId(), col_desc.getTableId()});
  }
  std::vector<InputTableInfo> query_infos;
  for (auto& tref : phys_table_refs) {
    query_infos.emplace_back(
        InputTableInfo{tref.db_id, tref.table_id, getTableInfo(tref.table_id)});
  }
  for (const auto& col_desc : col_descs) {
    if (ExpressionRange::typeSupportsRange(col_desc.getType())) {
      const auto col_var =
          boost::make_unique<Analyzer::ColumnVar>(col_desc.getColInfo(), 0);
      const auto col_range = getLeafColumnRange(col_var.get(), query_infos, this, false);
      agg_col_range_cache.setColRange({col_desc.getColId(), col_desc.getTableId()},
                                      col_range);
    }
  }
  return agg_col_range_cache;
}

StringDictionaryGenerations Executor::computeStringDictionaryGenerations(
    const std::unordered_set<InputColDescriptor>& col_descs) {
  StringDictionaryGenerations string_dictionary_generations;
  for (const auto& col_desc : col_descs) {
    const auto& col_ti = col_desc.getType().is_array()
                             ? col_desc.getType().get_elem_type()
                             : col_desc.getType();
    if (col_ti.is_string() && col_ti.get_compression() == kENCODING_DICT) {
      const int dict_id = col_ti.get_comp_param();
      const auto dd = data_provider_->getDictMetadata(db_id_, dict_id);
      CHECK(dd && dd->stringDict);
      string_dictionary_generations.setGeneration(dict_id,
                                                  dd->stringDict->storageEntryCount());
    }
  }
  return string_dictionary_generations;
}

TableGenerations Executor::computeTableGenerations(
    std::unordered_set<int> phys_table_ids) {
  TableGenerations table_generations;
  for (const int table_id : phys_table_ids) {
    const auto table_info = getTableInfo(table_id);
    table_generations.setGeneration(
        table_id,
        TableGeneration{static_cast<int64_t>(table_info.getPhysicalNumTuples()), 0});
  }
  return table_generations;
}

void Executor::setupCaching(DataProvider* data_provider,
                            const std::unordered_set<InputColDescriptor>& col_descs,
                            const std::unordered_set<int>& phys_table_ids) {
  row_set_mem_owner_ = std::make_shared<RowSetMemoryOwner>(
      data_provider, Executor::getArenaBlockSize(), cpu_threads());
  row_set_mem_owner_->setDictionaryGenerations(
      computeStringDictionaryGenerations(col_descs));
  agg_col_range_cache_ = computeColRangesCache(col_descs);
  table_generations_ = computeTableGenerations(phys_table_ids);
}

mapd_shared_mutex& Executor::getDataRecyclerLock() {
  return recycler_mutex_;
}

QueryPlanDagCache& Executor::getQueryPlanDagCache() {
  return query_plan_dag_cache_;
}

JoinColumnsInfo Executor::getJoinColumnsInfo(const Analyzer::Expr* join_expr,
                                             JoinColumnSide target_side,
                                             bool extract_only_col_id) {
  return query_plan_dag_cache_.getJoinColumnsInfoString(
      join_expr, target_side, extract_only_col_id);
}

mapd_shared_mutex& Executor::getSessionLock() {
  return executor_session_mutex_;
}

QuerySessionId& Executor::getCurrentQuerySession(
    mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  return current_query_session_;
}

bool Executor::checkCurrentQuerySession(const QuerySessionId& candidate_query_session,
                                        mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  // if current_query_session is equal to the candidate_query_session,
  // or it is empty session we consider
  return !candidate_query_session.empty() &&
         (current_query_session_ == candidate_query_session);
}

// used only for testing
QuerySessionStatus::QueryStatus Executor::getQuerySessionStatus(
    const QuerySessionId& candidate_query_session,
    mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  if (queries_session_map_.count(candidate_query_session) &&
      !queries_session_map_.at(candidate_query_session).empty()) {
    return queries_session_map_.at(candidate_query_session)
        .begin()
        ->second.getQueryStatus();
  }
  return QuerySessionStatus::QueryStatus::UNDEFINED;
}

void Executor::invalidateRunningQuerySession(
    mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  current_query_session_ = "";
}

CurrentQueryStatus Executor::attachExecutorToQuerySession(
    const QuerySessionId& query_session_id,
    const std::string& query_str,
    const std::string& query_submitted_time) {
  if (!query_session_id.empty()) {
    // if session is valid, do update 1) the exact executor id and 2) query status
    mapd_unique_lock<mapd_shared_mutex> write_lock(executor_session_mutex_);
    updateQuerySessionExecutorAssignment(
        query_session_id, query_submitted_time, executor_id_, write_lock);
    updateQuerySessionStatusWithLock(query_session_id,
                                     query_submitted_time,
                                     QuerySessionStatus::QueryStatus::PENDING_EXECUTOR,
                                     write_lock);
  }
  return {query_session_id, query_str};
}

void Executor::checkPendingQueryStatus(const QuerySessionId& query_session) {
  // check whether we are okay to execute the "pending" query
  // i.e., before running the query check if this query session is "ALREADY" interrupted
  mapd_shared_lock<mapd_shared_mutex> session_read_lock(executor_session_mutex_);
  if (query_session.empty()) {
    return;
  }
  if (queries_interrupt_flag_.find(query_session) == queries_interrupt_flag_.end()) {
    // something goes wrong since we assume this is caller's responsibility
    // (call this function only for enrolled query session)
    if (!queries_session_map_.count(query_session)) {
      VLOG(1) << "Interrupting pending query is not available since the query session is "
                 "not enrolled";
    } else {
      // here the query session is enrolled but the interrupt flag is not registered
      VLOG(1)
          << "Interrupting pending query is not available since its interrupt flag is "
             "not registered";
    }
    return;
  }
  if (queries_interrupt_flag_[query_session]) {
    CIDER_THROW(CiderRuntimeException,
                fmt::format("Query execution failed with error code {}",
                            std::to_string(ERR_INTERRUPTED)));
  }
}

void Executor::clearQuerySessionStatus(const QuerySessionId& query_session,
                                       const std::string& submitted_time_str) {
  mapd_unique_lock<mapd_shared_mutex> session_write_lock(executor_session_mutex_);
  // clear the interrupt-related info for a finished query
  if (query_session.empty()) {
    return;
  }
  removeFromQuerySessionList(query_session, submitted_time_str, session_write_lock);
  if (query_session.compare(current_query_session_) == 0) {
    invalidateRunningQuerySession(session_write_lock);
  }
}

void Executor::updateQuerySessionStatus(
    const QuerySessionId& query_session,
    const std::string& submitted_time_str,
    const QuerySessionStatus::QueryStatus new_query_status) {
  // update the running query session's the current status
  mapd_unique_lock<mapd_shared_mutex> session_write_lock(executor_session_mutex_);
  if (query_session.empty()) {
    return;
  }
  if (new_query_status == QuerySessionStatus::QueryStatus::RUNNING_QUERY_KERNEL) {
    current_query_session_ = query_session;
  }
  updateQuerySessionStatusWithLock(
      query_session, submitted_time_str, new_query_status, session_write_lock);
}

void Executor::enrollQuerySession(
    const QuerySessionId& query_session,
    const std::string& query_str,
    const std::string& submitted_time_str,
    const size_t executor_id,
    const QuerySessionStatus::QueryStatus query_session_status) {
  // enroll the query session into the Executor's session map
  mapd_unique_lock<mapd_shared_mutex> session_write_lock(executor_session_mutex_);
  if (query_session.empty()) {
    return;
  }

  addToQuerySessionList(query_session,
                        query_str,
                        submitted_time_str,
                        executor_id,
                        query_session_status,
                        session_write_lock);

  if (query_session_status == QuerySessionStatus::QueryStatus::RUNNING_QUERY_KERNEL) {
    current_query_session_ = query_session;
  }
}

bool Executor::addToQuerySessionList(const QuerySessionId& query_session,
                                     const std::string& query_str,
                                     const std::string& submitted_time_str,
                                     const size_t executor_id,
                                     const QuerySessionStatus::QueryStatus query_status,
                                     mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  // an internal API that enrolls the query session into the Executor's session map
  if (queries_session_map_.count(query_session)) {
    if (queries_session_map_.at(query_session).count(submitted_time_str)) {
      queries_session_map_.at(query_session).erase(submitted_time_str);
      queries_session_map_.at(query_session)
          .emplace(submitted_time_str,
                   QuerySessionStatus(query_session,
                                      executor_id,
                                      query_str,
                                      submitted_time_str,
                                      query_status));
    } else {
      queries_session_map_.at(query_session)
          .emplace(submitted_time_str,
                   QuerySessionStatus(query_session,
                                      executor_id,
                                      query_str,
                                      submitted_time_str,
                                      query_status));
    }
  } else {
    std::map<std::string, QuerySessionStatus> executor_per_query_map;
    executor_per_query_map.emplace(
        submitted_time_str,
        QuerySessionStatus(
            query_session, executor_id, query_str, submitted_time_str, query_status));
    queries_session_map_.emplace(query_session, executor_per_query_map);
  }
  return queries_interrupt_flag_.emplace(query_session, false).second;
}

bool Executor::updateQuerySessionStatusWithLock(
    const QuerySessionId& query_session,
    const std::string& submitted_time_str,
    const QuerySessionStatus::QueryStatus updated_query_status,
    mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  // an internal API that updates query session status
  if (query_session.empty()) {
    return false;
  }
  if (queries_session_map_.count(query_session)) {
    for (auto& query_status : queries_session_map_.at(query_session)) {
      auto target_submitted_t_str = query_status.second.getQuerySubmittedTime();
      // no time difference --> found the target query status
      if (submitted_time_str.compare(target_submitted_t_str) == 0) {
        auto prev_status = query_status.second.getQueryStatus();
        if (prev_status == updated_query_status) {
          return false;
        }
        query_status.second.setQueryStatus(updated_query_status);
        return true;
      }
    }
  }
  return false;
}

bool Executor::updateQuerySessionExecutorAssignment(
    const QuerySessionId& query_session,
    const std::string& submitted_time_str,
    const size_t executor_id,
    mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  // update the executor id of the query session
  if (query_session.empty()) {
    return false;
  }
  if (queries_session_map_.count(query_session)) {
    auto storage = queries_session_map_.at(query_session);
    for (auto it = storage.begin(); it != storage.end(); it++) {
      auto target_submitted_t_str = it->second.getQuerySubmittedTime();
      // no time difference --> found the target query status
      if (submitted_time_str.compare(target_submitted_t_str) == 0) {
        queries_session_map_.at(query_session)
            .at(submitted_time_str)
            .setExecutorId(executor_id);
        return true;
      }
    }
  }
  return false;
}

bool Executor::removeFromQuerySessionList(
    const QuerySessionId& query_session,
    const std::string& submitted_time_str,
    mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  if (query_session.empty()) {
    return false;
  }
  if (queries_session_map_.count(query_session)) {
    auto& storage = queries_session_map_.at(query_session);
    if (storage.size() > 1) {
      // in this case we only remove query executor info
      for (auto it = storage.begin(); it != storage.end(); it++) {
        auto target_submitted_t_str = it->second.getQuerySubmittedTime();
        // no time difference && have the same executor id--> found the target query
        if (it->second.getExecutorId() == executor_id_ &&
            submitted_time_str.compare(target_submitted_t_str) == 0) {
          storage.erase(it);
          return true;
        }
      }
    } else if (storage.size() == 1) {
      // here this session only has a single query executor
      // so we clear both executor info and its interrupt flag
      queries_session_map_.erase(query_session);
      queries_interrupt_flag_.erase(query_session);
      if (interrupted_.load()) {
        interrupted_.store(false);
      }
      return true;
    }
  }
  return false;
}

void Executor::setQuerySessionAsInterrupted(
    const QuerySessionId& query_session,
    mapd_unique_lock<mapd_shared_mutex>& write_lock) {
  if (query_session.empty()) {
    return;
  }
  if (queries_interrupt_flag_.find(query_session) != queries_interrupt_flag_.end()) {
    queries_interrupt_flag_[query_session] = true;
  }
}

bool Executor::checkIsQuerySessionInterrupted(
    const QuerySessionId& query_session,
    mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  if (query_session.empty()) {
    return false;
  }
  auto flag_it = queries_interrupt_flag_.find(query_session);
  return !query_session.empty() && flag_it != queries_interrupt_flag_.end() &&
         flag_it->second;
}

bool Executor::checkIsQuerySessionEnrolled(
    const QuerySessionId& query_session,
    mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  if (query_session.empty()) {
    return false;
  }
  return !query_session.empty() && queries_session_map_.count(query_session);
}

void Executor::enableRuntimeQueryInterrupt(
    const double runtime_query_check_freq,
    const unsigned pending_query_check_freq) const {
  // The only one scenario that we intentionally call this function is
  // to allow runtime query interrupt in QueryRunner for test cases.
  // Because test machine's default setting does not allow runtime query interrupt,
  // so we have to turn it on within test code if necessary.
  g_enable_runtime_query_interrupt = true;
  g_pending_query_interrupt_freq = pending_query_check_freq;
  g_running_query_interrupt_freq = runtime_query_check_freq;
  if (g_running_query_interrupt_freq) {
    g_running_query_interrupt_freq = 0.5;
  }
}

void Executor::addToCardinalityCache(const std::string& cache_key,
                                     const size_t cache_value) {
  if (g_use_estimator_result_cache) {
    mapd_unique_lock<mapd_shared_mutex> lock(recycler_mutex_);
    cardinality_cache_[cache_key] = cache_value;
    VLOG(1) << "Put estimated cardinality to the cache";
  }
}

Executor::CachedCardinality Executor::getCachedCardinality(const std::string& cache_key) {
  mapd_shared_lock<mapd_shared_mutex> lock(recycler_mutex_);
  if (g_use_estimator_result_cache &&
      cardinality_cache_.find(cache_key) != cardinality_cache_.end()) {
    VLOG(1) << "Reuse cached cardinality";
    return {true, cardinality_cache_[cache_key]};
  }
  return {false, -1};
}

std::vector<QuerySessionStatus> Executor::getQuerySessionInfo(
    const QuerySessionId& query_session,
    mapd_shared_lock<mapd_shared_mutex>& read_lock) {
  if (!queries_session_map_.empty() && queries_session_map_.count(query_session)) {
    auto& query_infos = queries_session_map_.at(query_session);
    std::vector<QuerySessionStatus> ret;
    for (auto& info : query_infos) {
      ret.push_back(QuerySessionStatus(query_session,
                                       info.second.getExecutorId(),
                                       info.second.getQueryStr(),
                                       info.second.getQuerySubmittedTime(),
                                       info.second.getQueryStatus()));
    }
    return ret;
  }
  return {};
}

const std::vector<size_t> Executor::getExecutorIdsRunningQuery(
    const QuerySessionId& interrupt_session) const {
  std::vector<size_t> res;
  mapd_shared_lock<mapd_shared_mutex> session_read_lock(executor_session_mutex_);
  auto it = queries_session_map_.find(interrupt_session);
  if (it != queries_session_map_.end()) {
    for (auto& kv : it->second) {
      if (kv.second.getQueryStatus() ==
          QuerySessionStatus::QueryStatus::RUNNING_QUERY_KERNEL) {
        res.push_back(kv.second.getExecutorId());
      }
    }
  }
  return res;
}

bool Executor::checkNonKernelTimeInterrupted() const {
  // this function should be called within an executor which is assigned
  // to the specific query thread (that indicates we already enroll the session)
  // check whether this is called from non unitary executor
  if (executor_id_ == UNITARY_EXECUTOR_ID) {
    return false;
  };
  mapd_shared_lock<mapd_shared_mutex> session_read_lock(executor_session_mutex_);
  auto flag_it = queries_interrupt_flag_.find(current_query_session_);
  return !current_query_session_.empty() && flag_it != queries_interrupt_flag_.end() &&
         flag_it->second;
}

std::map<int, std::shared_ptr<Executor>> Executor::executors_;

// contain the interrupt flag's status per query session
InterruptFlagMap Executor::queries_interrupt_flag_;
// contain a list of queries per query session
QuerySessionMap Executor::queries_session_map_;
// session lock
mapd_shared_mutex Executor::executor_session_mutex_;

mapd_shared_mutex Executor::execute_mutex_;
mapd_shared_mutex Executor::executors_cache_mutex_;

std::mutex Executor::compilation_mutex_;
std::mutex Executor::kernel_mutex_;

QueryPlanDagCache Executor::query_plan_dag_cache_;
mapd_shared_mutex Executor::recycler_mutex_;
std::unordered_map<std::string, size_t> Executor::cardinality_cache_;
