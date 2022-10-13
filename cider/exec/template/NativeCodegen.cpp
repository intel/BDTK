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

#include <llvm/ExecutionEngine/JITEventListener.h>
#include "exec/template/Execute.h"

#if LLVM_VERSION_MAJOR < 9
static_assert(false, "LLVM Version >= 9 is required.");
#endif

#include <llvm/Analysis/ScopedNoAliasAA.h>
#include <llvm/Analysis/TypeBasedAliasAnalysis.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/IR/Attributes.h>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/IntrinsicInst.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/Casting.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/FormattedStream.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/IPO/InferFunctionAttrs.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Instrumentation.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Scalar/InstSimplifyPass.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <llvm/Transforms/Utils/Cloning.h>

#if LLVM_VERSION_MAJOR >= 11
#include <llvm/Support/Host.h>
#endif

#include "StreamingTopN.h"
#include "exec/optimizer/AnnotateInternalFunctionsPass.h"
#include "exec/template/CodeGenerator.h"
#include "exec/template/LLVMFunctionAttributesUtil.h"
#include "exec/template/OutputBufferInitialization.h"
#include "exec/template/QueryTemplateGenerator.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "type/data/InlineNullValues.h"
#include "util/MathUtils.h"
#include "util/filesystem/cider_path.h"

float g_fraction_code_cache_to_evict = 0.2;

std::unique_ptr<llvm::Module> udf_cpu_module;
std::unique_ptr<llvm::Module> rt_udf_cpu_module;

namespace {

void throw_parseIR_error(const llvm::SMDiagnostic& parse_error, std::string src = "") {
  std::string excname = "LLVM IR ParseError: ";
  llvm::raw_string_ostream ss(excname);
  parse_error.print(src.c_str(), ss, false, false);
  CIDER_THROW(CiderCompileException, ss.str());
}

/* SHOW_DEFINED(<llvm::Module instance>) prints the function names
   that are defined in the given LLVM Module instance.

   SHOW_FUNCTIONS(<llvm::Module instance>) prints the function names
   of all used functions in the given LLVM Module
   instance. Declarations are marked with `[decl]` as a name suffix.

   Useful for debugging.
*/

#define SHOW_DEFINED(MODULE)                                         \
  {                                                                  \
    std::cout << __func__ << "#" << __LINE__ << ": " #MODULE << " "; \
    ::show_defined(MODULE);                                          \
  }

#define SHOW_FUNCTIONS(MODULE)                                       \
  {                                                                  \
    std::cout << __func__ << "#" << __LINE__ << ": " #MODULE << " "; \
    ::show_functions(MODULE);                                        \
  }

template <typename T = void>
void show_defined(llvm::Module& module) {
  std::cout << "defines: ";
  for (auto& f : module.getFunctionList()) {
    if (!f.isDeclaration()) {
      std::cout << f.getName().str() << ", ";
    }
  }
  std::cout << std::endl;
}

template <typename T = void>
void show_defined(llvm::Module* module) {
  if (module == nullptr) {
    std::cout << "is null" << std::endl;
  } else {
    show_defined(*module);
  }
}

template <typename T = void>
void show_defined(std::unique_ptr<llvm::Module>& module) {
  show_defined(module.get());
}

/*
  scan_function_calls(module, defined, undefined, ignored) computes
  defined and undefined sets of function names:

  - defined functions are those that are defined in the given module

  - undefined functions are those that are called by defined functions
    but that are not defined in the given module

  - ignored functions are functions that may be undefined but will not
    be listed in the set of undefined functions.

   Useful for debugging.
*/
template <typename T = void>
void scan_function_calls(llvm::Function& F,
                         std::unordered_set<std::string>& defined,
                         std::unordered_set<std::string>& undefined,
                         const std::unordered_set<std::string>& ignored) {
  for (llvm::inst_iterator I = llvm::inst_begin(F), E = llvm::inst_end(F); I != E; ++I) {
    if (auto* CI = llvm::dyn_cast<llvm::CallInst>(&*I)) {
      auto* F2 = CI->getCalledFunction();
      if (F2 != nullptr) {
        auto F2name = F2->getName().str();
        if (F2->isDeclaration()) {
          if (F2name.rfind("__", 0) !=
                  0  // assume symbols with double underscore are defined
              && F2name.rfind("llvm.", 0) !=
                     0  // TODO: this may give false positive for NVVM intrinsics
              && ignored.find(F2name) == ignored.end()  // not in ignored list
          ) {
            undefined.emplace(F2name);
          }
        } else {
          if (defined.find(F2name) == defined.end()) {
            defined.emplace(F2name);
            scan_function_calls<T>(*F2, defined, undefined, ignored);
          }
        }
      }
    }
  }
}

template <typename T = void>
void scan_function_calls(llvm::Module& module,
                         std::unordered_set<std::string>& defined,
                         std::unordered_set<std::string>& undefined,
                         const std::unordered_set<std::string>& ignored) {
  for (auto& F : module) {
    if (!F.isDeclaration()) {
      scan_function_calls(F, defined, undefined, ignored);
    }
  }
}

template <typename T = void>
std::tuple<std::unordered_set<std::string>, std::unordered_set<std::string>>
scan_function_calls(llvm::Module& module,
                    const std::unordered_set<std::string>& ignored = {}) {
  std::unordered_set<std::string> defined, undefined;
  scan_function_calls(module, defined, undefined, ignored);
  return std::make_tuple(defined, undefined);
}

#if !defined(WITH_JIT_DEBUG)
void eliminate_dead_self_recursive_funcs(
    llvm::Module& M,
    const std::unordered_set<llvm::Function*>& live_funcs) {
  std::vector<llvm::Function*> dead_funcs;
  for (auto& F : M) {
    bool bAlive = false;
    if (live_funcs.count(&F)) {
      continue;
    }
    for (auto U : F.users()) {
      auto* C = llvm::dyn_cast<const llvm::CallInst>(U);
      if (!C || C->getParent()->getParent() != &F) {
        bAlive = true;
        break;
      }
    }
    if (!bAlive) {
      dead_funcs.push_back(&F);
    }
  }
  for (auto pFn : dead_funcs) {
    pFn->eraseFromParent();
  }
}

void optimize_ir(llvm::Function* query_func,
                 llvm::Module* module,
                 llvm::legacy::PassManager& pass_manager,
                 const std::unordered_set<llvm::Function*>& live_funcs,
                 const CompilationOptions& co) {
  // the always inliner legacy pass must always run first
  pass_manager.add(llvm::createAlwaysInlinerLegacyPass());

  pass_manager.add(new AnnotateInternalFunctionsPass());

  pass_manager.add(llvm::createSROAPass());
  // mem ssa drops unused load and store instructions, e.g. passing variables directly
  // where possible
  pass_manager.add(
      llvm::createEarlyCSEPass(/*enable_mem_ssa=*/true));  // Catch trivial redundancies

  pass_manager.add(llvm::createJumpThreadingPass());  // Thread jumps.
  pass_manager.add(llvm::createCFGSimplificationPass());

  // remove load/stores in PHIs if instructions can be accessed directly post thread jumps
  pass_manager.add(llvm::createNewGVNPass());

  pass_manager.add(llvm::createDeadStoreEliminationPass());
  pass_manager.add(llvm::createLICMPass());

  pass_manager.add(llvm::createInstructionCombiningPass());

  // module passes
  pass_manager.add(llvm::createPromoteMemoryToRegisterPass());
  pass_manager.add(llvm::createGlobalOptimizerPass());

  if (co.opt_level == ExecutorOptLevel::LoopStrengthReduction) {
    pass_manager.add(llvm::createLoopStrengthReducePass());
  }

  pass_manager.add(llvm::createCFGSimplificationPass());  // cleanup after everything

  pass_manager.run(*module);

  eliminate_dead_self_recursive_funcs(*module, live_funcs);
}
#endif

}  // namespace

ExecutionEngineWrapper::ExecutionEngineWrapper() {}

ExecutionEngineWrapper::ExecutionEngineWrapper(llvm::ExecutionEngine* execution_engine)
    : execution_engine_(execution_engine) {}

ExecutionEngineWrapper::ExecutionEngineWrapper(llvm::ExecutionEngine* execution_engine,
                                               const CompilationOptions& co)
    : execution_engine_(execution_engine) {
  if (execution_engine_) {
#ifdef ENABLE_PERF_JIT_LISTENER
    jit_listener_ = llvm::JITEventListener::createPerfJITEventListener();
    CHECK(jit_listener_);
    execution_engine_->RegisterJITEventListener(jit_listener_);
#endif

    if (co.register_intel_jit_listener) {
#ifdef ENABLE_INTEL_JIT_LISTENER
      intel_jit_listener_.reset(llvm::JITEventListener::createIntelJITEventListener());
      CHECK(intel_jit_listener_);
      execution_engine_->RegisterJITEventListener(intel_jit_listener_.get());
      LOG(INFO) << "Registered IntelJITEventListener";
#else
      LOG(WARNING) << "This build is not Intel JIT Listener enabled. Ignoring Intel JIT "
                      "listener configuration parameter.";
#endif  // ENABLE_INTEL_JIT_LISTENER
    }
  }
}

ExecutionEngineWrapper& ExecutionEngineWrapper::operator=(
    llvm::ExecutionEngine* execution_engine) {
  execution_engine_.reset(execution_engine);
  intel_jit_listener_ = nullptr;
  jit_listener_ = nullptr;
  return *this;
}

void verify_function_ir(const llvm::Function* func) {
  std::stringstream err_ss;
  llvm::raw_os_ostream err_os(err_ss);
  err_os << "\n-----\n";
  if (llvm::verifyFunction(*func, &err_os)) {
    err_os << "\n-----\n";
    func->print(err_os, nullptr);
    err_os << "\n-----\n";
    LOG(FATAL) << err_ss.str();
  }
}

std::shared_ptr<CompilationContext> Executor::getCodeFromCache(const CodeCacheKey& key,
                                                               const CodeCache& cache) {
  auto it = cache.find(key);
  if (it != cache.cend()) {
    delete cgen_state_->module_;
    cgen_state_->module_ = it->second.second;
    return it->second.first;
  }
  return {};
}

void Executor::addCodeToCache(const CodeCacheKey& key,
                              std::shared_ptr<CompilationContext> compilation_context,
                              llvm::Module* module,
                              CodeCache& cache) {
  cache.put(key,
            std::make_pair<std::shared_ptr<CompilationContext>, decltype(module)>(
                std::move(compilation_context), std::move(module)));
}

namespace {

std::string assemblyForCPU(ExecutionEngineWrapper& execution_engine,
                           llvm::Module* module) {
  llvm::legacy::PassManager pass_manager;
  auto cpu_target_machine = execution_engine->getTargetMachine();
  CHECK(cpu_target_machine);
  llvm::SmallString<256> code_str;
  llvm::raw_svector_ostream os(code_str);
#if LLVM_VERSION_MAJOR >= 10
  cpu_target_machine->addPassesToEmitFile(
      pass_manager, os, nullptr, llvm::CGFT_AssemblyFile);
#else
  cpu_target_machine->addPassesToEmitFile(
      pass_manager, os, nullptr, llvm::TargetMachine::CGFT_AssemblyFile);
#endif
  pass_manager.run(*module);
  return "Assembly for the CPU:\n" + std::string(code_str.str()) + "\nEnd of assembly";
}

}  // namespace

ExecutionEngineWrapper CodeGenerator::generateNativeCPUCode(
    llvm::Function* func,
    const std::unordered_set<llvm::Function*>& live_funcs,
    const CompilationOptions& co) {
  auto module = func->getParent();
  // run optimizations
#ifndef WITH_JIT_DEBUG
  llvm::legacy::PassManager pass_manager;
  optimize_ir(func, module, pass_manager, live_funcs, co);
#endif  // WITH_JIT_DEBUG

  auto init_err = llvm::InitializeNativeTarget();
  CHECK(!init_err);

  llvm::InitializeAllTargetMCs();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  std::string err_str;
  std::unique_ptr<llvm::Module> owner(module);
  llvm::EngineBuilder eb(std::move(owner));
  eb.setMCPU(llvm::sys::getHostCPUName().str());
  eb.setErrorStr(&err_str);
  eb.setEngineKind(llvm::EngineKind::JIT);
  llvm::TargetOptions to;
  to.EnableFastISel = true;
  eb.setTargetOptions(to);
  if (co.opt_level == ExecutorOptLevel::ReductionJIT) {
    eb.setOptLevel(llvm::CodeGenOpt::None);
  }

#ifdef _WIN32
  // TODO: workaround for data layout mismatch crash for now
  auto target_machine = eb.selectTarget();
  CHECK(target_machine);
  module->setDataLayout(target_machine->createDataLayout());
#endif

  ExecutionEngineWrapper execution_engine(eb.create(), co);
  CHECK(execution_engine.get());
  LOG(ASM) << assemblyForCPU(execution_engine, module);

  execution_engine->finalizeObject();
  return execution_engine;
}

std::shared_ptr<CompilationContext> Executor::optimizeAndCodegenCPU(
    llvm::Function* query_func,
    llvm::Function* multifrag_query_func,
    const std::unordered_set<llvm::Function*>& live_funcs,
    const CompilationOptions& co) {
  auto module = multifrag_query_func->getParent();
  CodeCacheKey key{serialize_llvm_object(query_func),
                   serialize_llvm_object(cgen_state_->row_func_)};
  if (cgen_state_->filter_func_) {
    key.push_back(serialize_llvm_object(cgen_state_->filter_func_));
  }
  for (const auto helper : cgen_state_->helper_functions_) {
    key.push_back(serialize_llvm_object(helper));
  }
  auto cached_code = getCodeFromCache(key, cpu_code_cache_);
  if (cached_code) {
    return cached_code;
  }

  auto execution_engine =
      CodeGenerator::generateNativeCPUCode(query_func, live_funcs, co);
  auto cpu_compilation_context =
      std::make_shared<CpuCompilationContext>(std::move(execution_engine));
  cpu_compilation_context->setFunctionPointer(multifrag_query_func);
  addCodeToCache(key, cpu_compilation_context, module, cpu_code_cache_);
  return cpu_compilation_context;
}

void CodeGenerator::link_udf_module(const std::unique_ptr<llvm::Module>& udf_module,
                                    llvm::Module& module,
                                    CgenState* cgen_state,
                                    llvm::Linker::Flags flags) {
  // throw a runtime error if the target module contains functions
  // with the same name as in module of UDF functions.
  for (auto& f : *udf_module.get()) {
    auto func = module.getFunction(f.getName());
    if (!(func == nullptr) && !f.isDeclaration() && flags == llvm::Linker::Flags::None) {
      LOG(ERROR) << "  Attempt to overwrite " << f.getName().str() << " in "
                 << module.getModuleIdentifier() << " from `"
                 << udf_module->getModuleIdentifier() << "`" << std::endl;
      CIDER_THROW(CiderCompileException,
                  "link_udf_module: *** attempt to overwrite a runtime function with a "
                  "UDF function ***");
    } else {
      VLOG(1) << "  Adding " << f.getName().str() << " to "
              << module.getModuleIdentifier() << " from `"
              << udf_module->getModuleIdentifier() << "`" << std::endl;
    }
  }

  std::unique_ptr<llvm::Module> udf_module_copy;

  udf_module_copy = llvm::CloneModule(*udf_module.get(), cgen_state->vmap_);

  udf_module_copy->setDataLayout(module.getDataLayout());
  udf_module_copy->setTargetTriple(module.getTargetTriple());

  // Initialize linker with module for RuntimeFunctions.bc
  llvm::Linker ld(module);
  bool link_error = false;

  link_error = ld.linkInModule(std::move(udf_module_copy), flags);

  if (link_error) {
    CIDER_THROW(CiderCompileException, "link_udf_module: *** error linking module ***");
  }
}

namespace {

std::string cpp_to_llvm_name(const std::string& s) {
  if (s == "int8_t") {
    return "i8";
  }
  if (s == "int16_t") {
    return "i16";
  }
  if (s == "int32_t") {
    return "i32";
  }
  if (s == "int64_t") {
    return "i64";
  }
  CHECK(s == "float" || s == "double");
  return s;
}

std::string gen_array_any_all_sigs() {
  std::string result;
  for (const std::string any_or_all : {"any", "all"}) {
    for (const std::string elem_type :
         {"int8_t", "int16_t", "int32_t", "int64_t", "float", "double"}) {
      for (const std::string needle_type :
           {"int8_t", "int16_t", "int32_t", "int64_t", "float", "double"}) {
        for (const std::string op_name : {"eq", "ne", "lt", "le", "gt", "ge"}) {
          result += ("declare i1 @array_" + any_or_all + "_" + op_name + "_" + elem_type +
                     "_" + needle_type + "(i8*, i64, " + cpp_to_llvm_name(needle_type) +
                     ", " + cpp_to_llvm_name(elem_type) + ");\n");
        }
      }
    }
  }
  return result;
}

std::string gen_translate_null_key_sigs() {
  std::string result;
  for (const std::string key_type : {"int8_t", "int16_t", "int32_t", "int64_t"}) {
    const auto key_llvm_type = cpp_to_llvm_name(key_type);
    result += "declare i64 @translate_null_key_" + key_type + "(" + key_llvm_type + ", " +
              key_llvm_type + ", i64);\n";
  }
  return result;
}

}  // namespace

std::map<std::string, std::string> get_device_parameters(bool cpu_only) {
  std::map<std::string, std::string> result;

  result.insert(std::make_pair("cpu_name", llvm::sys::getHostCPUName()));
  result.insert(std::make_pair("cpu_triple", llvm::sys::getProcessTriple()));
  result.insert(
      std::make_pair("cpu_cores", std::to_string(llvm::sys::getHostNumPhysicalCores())));
  result.insert(std::make_pair("cpu_threads", std::to_string(cpu_threads())));

  // https://en.cppreference.com/w/cpp/language/types
  std::string sizeof_types;
  sizeof_types += "bool:" + std::to_string(sizeof(bool)) + ";";
  sizeof_types += "size_t:" + std::to_string(sizeof(size_t)) + ";";
  sizeof_types += "ssize_t:" + std::to_string(sizeof(ssize_t)) + ";";
  sizeof_types += "char:" + std::to_string(sizeof(char)) + ";";
  sizeof_types += "uchar:" + std::to_string(sizeof(unsigned char)) + ";";
  sizeof_types += "short:" + std::to_string(sizeof(short)) + ";";
  sizeof_types += "ushort:" + std::to_string(sizeof(unsigned short int)) + ";";
  sizeof_types += "int:" + std::to_string(sizeof(int)) + ";";
  sizeof_types += "uint:" + std::to_string(sizeof(unsigned int)) + ";";
  sizeof_types += "long:" + std::to_string(sizeof(long int)) + ";";
  sizeof_types += "ulong:" + std::to_string(sizeof(unsigned long int)) + ";";
  sizeof_types += "longlong:" + std::to_string(sizeof(long long int)) + ";";
  sizeof_types += "ulonglong:" + std::to_string(sizeof(unsigned long long int)) + ";";
  sizeof_types += "float:" + std::to_string(sizeof(float)) + ";";
  sizeof_types += "double:" + std::to_string(sizeof(double)) + ";";
  sizeof_types += "longdouble:" + std::to_string(sizeof(long double)) + ";";
  sizeof_types += "voidptr:" + std::to_string(sizeof(void*)) + ";";

  result.insert(std::make_pair("type_sizeof", sizeof_types));

  std::string null_values;
  null_values += "boolean1:" + std::to_string(serialized_null_value<bool>()) + ";";
  null_values += "boolean8:" + std::to_string(serialized_null_value<int8_t>()) + ";";
  null_values += "int8:" + std::to_string(serialized_null_value<int8_t>()) + ";";
  null_values += "int16:" + std::to_string(serialized_null_value<int16_t>()) + ";";
  null_values += "int32:" + std::to_string(serialized_null_value<int32_t>()) + ";";
  null_values += "int64:" + std::to_string(serialized_null_value<int64_t>()) + ";";
  null_values += "uint8:" + std::to_string(serialized_null_value<uint8_t>()) + ";";
  null_values += "uint16:" + std::to_string(serialized_null_value<uint16_t>()) + ";";
  null_values += "uint32:" + std::to_string(serialized_null_value<uint32_t>()) + ";";
  null_values += "uint64:" + std::to_string(serialized_null_value<uint64_t>()) + ";";
  null_values += "float32:" + std::to_string(serialized_null_value<float>()) + ";";
  null_values += "float64:" + std::to_string(serialized_null_value<double>()) + ";";
  null_values +=
      "Array<boolean8>:" + std::to_string(serialized_null_value<int8_t, true>()) + ";";
  null_values +=
      "Array<int8>:" + std::to_string(serialized_null_value<int8_t, true>()) + ";";
  null_values +=
      "Array<int16>:" + std::to_string(serialized_null_value<int16_t, true>()) + ";";
  null_values +=
      "Array<int32>:" + std::to_string(serialized_null_value<int32_t, true>()) + ";";
  null_values +=
      "Array<int64>:" + std::to_string(serialized_null_value<int64_t, true>()) + ";";
  null_values +=
      "Array<float32>:" + std::to_string(serialized_null_value<float, true>()) + ";";
  null_values +=
      "Array<float64>:" + std::to_string(serialized_null_value<double, true>()) + ";";

  result.insert(std::make_pair("null_values", null_values));

  llvm::StringMap<bool> cpu_features;
  if (llvm::sys::getHostCPUFeatures(cpu_features)) {
    std::string features_str = "";
    for (auto it = cpu_features.begin(); it != cpu_features.end(); ++it) {
      features_str += (it->getValue() ? " +" : " -");
      features_str += it->getKey().str();
    }
    result.insert(std::make_pair("cpu_features", features_str));
  }

  result.insert(std::make_pair("llvm_version",
                               std::to_string(LLVM_VERSION_MAJOR) + "." +
                                   std::to_string(LLVM_VERSION_MINOR) + "." +
                                   std::to_string(LLVM_VERSION_PATCH)));
  return result;
}

namespace {

bool is_udf_module_present(bool cpu_only = false) {
  return cpu_only && (udf_cpu_module != nullptr);
}

}  // namespace

// A small number of runtime functions don't get through CgenState::emitCall. List them
// explicitly here and always clone their implementation from the runtime module.
bool CodeGenerator::alwaysCloneRuntimeFunction(const llvm::Function* func) {
  return func->getName() == "query_stub_hoisted_literals" ||
         func->getName() == "multifrag_query_hoisted_literals" ||
         func->getName() == "query_hoisted_literals_with_row_skip_mask" ||
         func->getName() == "query_stub_hoisted_literals_with_row_skip_mask" ||
         func->getName() == "query_stub" || func->getName() == "multifrag_query" ||
         func->getName() == "fixed_width_int_decode" ||
         func->getName() == "fixed_width_unsigned_decode" ||
         func->getName() == "diff_fixed_width_int_decode" ||
         func->getName() == "fixed_width_double_decode" ||
         func->getName() == "fixed_width_float_decode" ||
         func->getName() == "fixed_width_small_date_decode" ||
         func->getName() == "record_error_code" || func->getName() == "get_error_code" ||
         func->getName() == "pos_start_impl" || func->getName() == "pos_step_impl" ||
         func->getName() == "group_buff_idx_impl" ||
         func->getName() == "init_shared_mem" ||
         func->getName() == "init_shared_mem_nop" || func->getName() == "write_back_nop";
}

std::unique_ptr<llvm::Module> read_llvm_module_from_bc_file(
    const std::string& bc_filename,
    llvm::LLVMContext& context) {
  llvm::SMDiagnostic err;

  auto buffer_or_error = llvm::MemoryBuffer::getFile(bc_filename);
  CHECK(!buffer_or_error.getError()) << "bc_filename=" << bc_filename;
  llvm::MemoryBuffer* buffer = buffer_or_error.get().get();

  auto owner = llvm::parseBitcodeFile(buffer->getMemBufferRef(), context);
  CHECK(!owner.takeError());
  CHECK(owner->get());
  return std::move(owner.get());
}

std::unique_ptr<llvm::Module> read_llvm_module_from_ir_file(
    const std::string& udf_ir_filename,
    llvm::LLVMContext& ctx) {
  llvm::SMDiagnostic parse_error;

  llvm::StringRef file_name_arg(udf_ir_filename);

  auto owner = llvm::parseIRFile(file_name_arg, parse_error, ctx);
  if (!owner) {
    throw_parseIR_error(parse_error, udf_ir_filename);
  }

  return owner;
}

std::unique_ptr<llvm::Module> read_llvm_module_from_ir_string(
    const std::string& udf_ir_string,
    llvm::LLVMContext& ctx) {
  llvm::SMDiagnostic parse_error;

  auto buf = std::make_unique<llvm::MemoryBufferRef>(udf_ir_string,
                                                     "Runtime UDF/UDTF LLVM/NVVM IR");

  auto owner = llvm::parseIR(*buf, parse_error, ctx);
  if (!owner) {
    LOG(IR) << "read_llvm_module_from_ir_string:\n"
            << udf_ir_string << "\nEnd of LLVM/NVVM IR";
    throw_parseIR_error(parse_error);
  }
  return owner;
}

llvm::Module* read_template_module(llvm::LLVMContext& context) {
  llvm::SMDiagnostic err;

  auto buffer_or_error = llvm::MemoryBuffer::getFile(cider::get_root_abs_path() +
                                                     "/function/RuntimeFunctions.bc");
  CHECK(!buffer_or_error.getError()) << "root path=" << cider::get_root_abs_path();
  llvm::MemoryBuffer* buffer = buffer_or_error.get().get();

  auto owner = llvm::parseBitcodeFile(buffer->getMemBufferRef(), context);
  CHECK(!owner.takeError());
  auto module = owner.get().release();
  CHECK(module);

  return module;
}

namespace {

void bind_pos_placeholders(const std::string& pos_fn_name,
                           const bool use_resume_param,
                           llvm::Function* query_func,
                           llvm::Module* module) {
  for (auto it = llvm::inst_begin(query_func), e = llvm::inst_end(query_func); it != e;
       ++it) {
    if (!llvm::isa<llvm::CallInst>(*it)) {
      continue;
    }
    auto& pos_call = llvm::cast<llvm::CallInst>(*it);
    if (std::string(pos_call.getCalledFunction()->getName()) == pos_fn_name) {
      if (use_resume_param) {
        const auto error_code_arg = get_arg_by_name(query_func, "error_code");
        llvm::ReplaceInstWithInst(
            &pos_call,
            llvm::CallInst::Create(module->getFunction(pos_fn_name + "_impl"),
                                   error_code_arg));
      } else {
        llvm::ReplaceInstWithInst(
            &pos_call,
            llvm::CallInst::Create(module->getFunction(pos_fn_name + "_impl")));
      }
      break;
    }
  }
}

void set_row_func_argnames(llvm::Function* row_func,
                           const size_t in_col_count,
                           const size_t agg_col_count,
                           const bool hoist_literals,
                           const bool use_cider_data_format = false) {
  auto arg_it = row_func->arg_begin();

  if (agg_col_count) {
    for (size_t i = 0; i < agg_col_count; ++i) {
      arg_it->setName("out");
      ++arg_it;
    }
    if (use_cider_data_format) {
      for (size_t i = 0; i < agg_col_count; ++i) {
        arg_it->setName("out_null");
        ++arg_it;
      }
    }
  } else {
    arg_it->setName("group_by_buff");
    ++arg_it;
    arg_it->setName("varlen_output_buff");
    ++arg_it;
    arg_it->setName("crt_matched");
    ++arg_it;
    arg_it->setName("total_matched");
    ++arg_it;
    arg_it->setName("old_total_matched");
    ++arg_it;
    arg_it->setName("max_matched");
    ++arg_it;
  }

  arg_it->setName("agg_init_val");
  ++arg_it;

  arg_it->setName("pos");
  ++arg_it;

  arg_it->setName("frag_row_off");
  ++arg_it;

  arg_it->setName("num_rows_per_scan");
  ++arg_it;

  if (hoist_literals) {
    arg_it->setName("literals");
    ++arg_it;
  }

  for (size_t i = 0; i < in_col_count; ++i) {
    arg_it->setName("col_buf" + std::to_string(i));
    ++arg_it;
  }

  arg_it->setName("join_hash_tables");
}

llvm::Function* create_row_function(const size_t in_col_count,
                                    const size_t agg_col_count,
                                    const bool hoist_literals,
                                    llvm::Module* module,
                                    llvm::LLVMContext& context,
                                    const bool use_cider_data_format = false) {
  std::vector<llvm::Type*> row_process_arg_types;

  if (agg_col_count) {
    // output (aggregate) arguments
    for (size_t i = 0; i < agg_col_count; ++i) {
      row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));
      if (use_cider_data_format) {
        row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));
      }
    }
  } else {
    // group by buffer
    row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));
    // varlen output buffer
    row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));
    // current match count
    row_process_arg_types.push_back(llvm::Type::getInt32PtrTy(context));
    // total match count passed from the caller
    row_process_arg_types.push_back(llvm::Type::getInt32PtrTy(context));
    // old total match count returned to the caller
    row_process_arg_types.push_back(llvm::Type::getInt32PtrTy(context));
    // max matched (total number of slots in the output buffer)
    row_process_arg_types.push_back(llvm::Type::getInt32Ty(context));
  }

  // aggregate init values
  row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));

  // position argument
  row_process_arg_types.push_back(llvm::Type::getInt64Ty(context));

  // fragment row offset argument
  row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));

  // number of rows for each scan
  row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));

  // literals buffer argument
  if (hoist_literals) {
    row_process_arg_types.push_back(llvm::Type::getInt8PtrTy(context));
  }

  // column buffer arguments
  for (size_t i = 0; i < in_col_count; ++i) {
    row_process_arg_types.emplace_back(llvm::Type::getInt8PtrTy(context));
  }

  // join hash table argument
  row_process_arg_types.push_back(llvm::Type::getInt64PtrTy(context));

  // generate the function
  auto ft =
      llvm::FunctionType::get(get_int_type(32, context), row_process_arg_types, false);

  auto row_func =
      llvm::Function::Create(ft, llvm::Function::ExternalLinkage, "row_func", module);

  // set the row function argument names; for debugging purposes only
  set_row_func_argnames(
      row_func, in_col_count, agg_col_count, hoist_literals, use_cider_data_format);

  return row_func;
}

// Iterate through multifrag_query_func, replacing calls to query_fname with query_func.
void bind_query(llvm::Function* query_func,
                const std::string& query_fname,
                llvm::Function* multifrag_query_func,
                llvm::Module* module) {
  std::vector<llvm::CallInst*> query_stubs;
  for (auto it = llvm::inst_begin(multifrag_query_func),
            e = llvm::inst_end(multifrag_query_func);
       it != e;
       ++it) {
    if (!llvm::isa<llvm::CallInst>(*it)) {
      continue;
    }
    auto& query_call = llvm::cast<llvm::CallInst>(*it);
    if (std::string(query_call.getCalledFunction()->getName()) == query_fname) {
      query_stubs.push_back(&query_call);
    }
  }
  for (auto& S : query_stubs) {
    std::vector<llvm::Value*> args;
    for (size_t i = 0; i < S->getNumArgOperands(); ++i) {
      args.push_back(S->getArgOperand(i));
    }
    llvm::ReplaceInstWithInst(S, llvm::CallInst::Create(query_func, args, ""));
  }
}

std::vector<std::string> get_agg_fnames(const std::vector<Analyzer::Expr*>& target_exprs,
                                        const bool is_group_by) {
  std::vector<std::string> result;
  for (size_t target_idx = 0, agg_col_idx = 0; target_idx < target_exprs.size();
       ++target_idx, ++agg_col_idx) {
    const auto target_expr = target_exprs[target_idx];
    CHECK(target_expr);
    const auto target_type_info = target_expr->get_type_info();
    const auto agg_expr = dynamic_cast<Analyzer::AggExpr*>(target_expr);
    const bool is_varlen =
        (target_type_info.is_string() &&
         target_type_info.get_compression() == kENCODING_NONE) ||
        target_type_info.is_array();  // TODO: should it use is_varlen_array() ?
    if (!agg_expr || agg_expr->get_aggtype() == kSAMPLE) {
      result.emplace_back(target_type_info.is_fp() ? "agg_id_double" : "agg_id");
      if (is_varlen) {
        result.emplace_back("agg_id");
      }
      continue;
    }
    const auto agg_type = agg_expr->get_aggtype();
    const auto& agg_type_info =
        agg_type != kCOUNT ? agg_expr->get_arg()->get_type_info() : target_type_info;
    switch (agg_type) {
      case kAVG: {
        if (!agg_type_info.is_integer() && !agg_type_info.is_decimal() &&
            !agg_type_info.is_fp()) {
          CIDER_THROW(CiderCompileException,
                      "AVG is only valid on integer and floating point");
        }
        result.emplace_back((agg_type_info.is_integer() || agg_type_info.is_time())
                                ? "agg_sum"
                                : "agg_sum_double");
        result.emplace_back((agg_type_info.is_integer() || agg_type_info.is_time())
                                ? "agg_count"
                                : "agg_count_double");
        break;
      }
      case kMIN: {
        if (agg_type_info.is_string() || agg_type_info.is_array()) {
          CIDER_THROW(CiderCompileException,
                      "MIN on strings or arrays types not supported yet");
        }
        result.emplace_back((agg_type_info.is_integer() || agg_type_info.is_time())
                                ? "agg_min"
                                : "agg_min_double");
        break;
      }
      case kMAX: {
        if (agg_type_info.is_string() || agg_type_info.is_array()) {
          CIDER_THROW(CiderCompileException,
                      "MAX on strings or arrays types not supported yet");
        }
        result.emplace_back((agg_type_info.is_integer() || agg_type_info.is_time())
                                ? "agg_max"
                                : "agg_max_double");
        break;
      }
      case kSUM: {
        if (!agg_type_info.is_integer() && !agg_type_info.is_decimal() &&
            !agg_type_info.is_fp()) {
          CIDER_THROW(CiderCompileException,
                      "SUM is only valid on integer and floating point");
        }
        result.emplace_back((agg_type_info.is_integer() || agg_type_info.is_time())
                                ? "agg_sum"
                                : "agg_sum_double");
        break;
      }
      case kCOUNT:
        result.emplace_back(agg_expr->get_is_distinct() ? "agg_count_distinct"
                                                        : "agg_count");
        break;
      case kSINGLE_VALUE: {
        result.emplace_back(agg_type_info.is_fp() ? "agg_id_double" : "agg_id");
        break;
      }
      case kSAMPLE: {
        // Note that varlen SAMPLE arguments are handled separately above
        result.emplace_back(agg_type_info.is_fp() ? "agg_id_double" : "agg_id");
        break;
      }
      case kAPPROX_COUNT_DISTINCT:
        result.emplace_back("agg_approximate_count_distinct");
        break;
      case kAPPROX_QUANTILE:
        result.emplace_back("agg_approx_quantile");
        break;
      default:
        CHECK(false);
    }
  }
  return result;
}

}  // namespace

bool is_rt_udf_module_present(bool cpu_only) {
  return cpu_only && (rt_udf_cpu_module != nullptr);
}

std::unordered_set<llvm::Function*> CodeGenerator::markDeadRuntimeFuncs(
    llvm::Module& module,
    const std::vector<llvm::Function*>& roots,
    const std::vector<llvm::Function*>& leaves) {
  std::unordered_set<llvm::Function*> live_funcs;
  live_funcs.insert(roots.begin(), roots.end());
  live_funcs.insert(leaves.begin(), leaves.end());

  if (auto F = module.getFunction("init_shared_mem_nop")) {
    live_funcs.insert(F);
  }
  if (auto F = module.getFunction("write_back_nop")) {
    live_funcs.insert(F);
  }

  for (const llvm::Function* F : roots) {
    for (const llvm::BasicBlock& BB : *F) {
      for (const llvm::Instruction& I : BB) {
        if (const llvm::CallInst* CI = llvm::dyn_cast<const llvm::CallInst>(&I)) {
          live_funcs.insert(CI->getCalledFunction());
        }
      }
    }
  }

  for (llvm::Function& F : module) {
    if (!live_funcs.count(&F) && !F.isDeclaration()) {
      F.setLinkage(llvm::GlobalValue::InternalLinkage);
    }
  }

  return live_funcs;
}

namespace {
// searches for a particular variable within a specific basic block (or all if bb_name is
// empty)
template <typename InstType>
llvm::Value* find_variable_in_basic_block(llvm::Function* func,
                                          std::string bb_name,
                                          std::string variable_name) {
  llvm::Value* result = nullptr;
  if (func == nullptr || variable_name.empty()) {
    return result;
  }
  bool is_found = false;
  for (auto bb_it = func->begin(); bb_it != func->end() && !is_found; ++bb_it) {
    if (!bb_name.empty() && bb_it->getName() != bb_name) {
      continue;
    }
    for (auto inst_it = bb_it->begin(); inst_it != bb_it->end(); inst_it++) {
      if (llvm::isa<InstType>(*inst_it)) {
        if (inst_it->getName() == variable_name) {
          result = &*inst_it;
          is_found = true;
          break;
        }
      }
    }
  }
  return result;
}
};  // namespace

void Executor::createErrorCheckControlFlow(
    llvm::Function* query_func,
    bool run_with_dynamic_watchdog,
    bool run_with_allowing_runtime_interrupt,
    const std::vector<InputTableInfo>& input_table_infos) {
  AUTOMATIC_IR_METADATA(cgen_state_.get());

  // check whether the row processing was successful; currently, it can
  // fail by running out of group by buffer slots

  if (run_with_dynamic_watchdog && run_with_allowing_runtime_interrupt) {
    // when both dynamic watchdog and runtime interrupt turns on
    // we use dynamic watchdog
    run_with_allowing_runtime_interrupt = false;
  }

  {
    // disable injecting query interrupt checker if the session info is invalid
    mapd_shared_lock<mapd_shared_mutex> session_read_lock(executor_session_mutex_);
    if (current_query_session_.empty()) {
      run_with_allowing_runtime_interrupt = false;
    }
  }

  llvm::Value* row_count = nullptr;
  bool done_splitting = false;
  for (auto bb_it = query_func->begin(); bb_it != query_func->end() && !done_splitting;
       ++bb_it) {
    llvm::Value* pos = nullptr;
    for (auto inst_it = bb_it->begin(); inst_it != bb_it->end(); ++inst_it) {
      if ((run_with_dynamic_watchdog || run_with_allowing_runtime_interrupt) &&
          llvm::isa<llvm::PHINode>(*inst_it)) {
        if (inst_it->getName() == "pos") {
          pos = &*inst_it;
        }
        continue;
      }
      if (!llvm::isa<llvm::CallInst>(*inst_it)) {
        continue;
      }
      auto& row_func_call = llvm::cast<llvm::CallInst>(*inst_it);
      if (std::string(row_func_call.getCalledFunction()->getName()) == "row_process") {
        auto next_inst_it = inst_it;
        ++next_inst_it;
        auto new_bb = bb_it->splitBasicBlock(next_inst_it);
        auto& br_instr = bb_it->back();
        llvm::IRBuilder<> ir_builder(&br_instr);
        llvm::Value* err_lv = &*inst_it;
        llvm::Value* err_lv_returned_from_row_func = nullptr;
        if (run_with_dynamic_watchdog) {
          CHECK(pos);
          llvm::Value* call_watchdog_lv = nullptr;

          // CPU path: run watchdog for every 64th row
          auto dw_predicate = ir_builder.CreateAnd(pos, uint64_t(0x3f));
          call_watchdog_lv = ir_builder.CreateICmp(
              llvm::ICmpInst::ICMP_EQ, dw_predicate, cgen_state_->llInt(int64_t(0LL)));

          CHECK(call_watchdog_lv);
          auto error_check_bb = bb_it->splitBasicBlock(
              llvm::BasicBlock::iterator(br_instr), ".error_check");
          auto& watchdog_br_instr = bb_it->back();

          auto watchdog_check_bb = llvm::BasicBlock::Create(
              cgen_state_->context_, ".watchdog_check", query_func, error_check_bb);
          llvm::IRBuilder<> watchdog_ir_builder(watchdog_check_bb);
          auto detected_timeout = watchdog_ir_builder.CreateCall(
              cgen_state_->module_->getFunction("dynamic_watchdog"), {});
          auto timeout_err_lv = watchdog_ir_builder.CreateSelect(
              detected_timeout, cgen_state_->llInt(Executor::ERR_OUT_OF_TIME), err_lv);
          watchdog_ir_builder.CreateBr(error_check_bb);

          llvm::ReplaceInstWithInst(
              &watchdog_br_instr,
              llvm::BranchInst::Create(
                  watchdog_check_bb, error_check_bb, call_watchdog_lv));
          ir_builder.SetInsertPoint(&br_instr);
          auto unified_err_lv = ir_builder.CreatePHI(err_lv->getType(), 2);

          unified_err_lv->addIncoming(timeout_err_lv, watchdog_check_bb);
          unified_err_lv->addIncoming(err_lv, &*bb_it);
          err_lv = unified_err_lv;
        } else if (run_with_allowing_runtime_interrupt) {
          CHECK(pos);
          llvm::Value* call_check_interrupt_lv = nullptr;

          // CPU path: run interrupt checker for every 64th row
          auto interrupt_predicate = ir_builder.CreateAnd(pos, uint64_t(0x3f));
          call_check_interrupt_lv =
              ir_builder.CreateICmp(llvm::ICmpInst::ICMP_EQ,
                                    interrupt_predicate,
                                    cgen_state_->llInt(int64_t(0LL)));

          CHECK(call_check_interrupt_lv);
          auto error_check_bb = bb_it->splitBasicBlock(
              llvm::BasicBlock::iterator(br_instr), ".error_check");
          auto& check_interrupt_br_instr = bb_it->back();

          auto interrupt_check_bb = llvm::BasicBlock::Create(
              cgen_state_->context_, ".interrupt_check", query_func, error_check_bb);
          llvm::IRBuilder<> interrupt_checker_ir_builder(interrupt_check_bb);
          auto detected_interrupt = interrupt_checker_ir_builder.CreateCall(
              cgen_state_->module_->getFunction("check_interrupt"), {});
          auto interrupt_err_lv = interrupt_checker_ir_builder.CreateSelect(
              detected_interrupt, cgen_state_->llInt(Executor::ERR_INTERRUPTED), err_lv);
          interrupt_checker_ir_builder.CreateBr(error_check_bb);

          llvm::ReplaceInstWithInst(
              &check_interrupt_br_instr,
              llvm::BranchInst::Create(
                  interrupt_check_bb, error_check_bb, call_check_interrupt_lv));
          ir_builder.SetInsertPoint(&br_instr);
          auto unified_err_lv = ir_builder.CreatePHI(err_lv->getType(), 2);

          unified_err_lv->addIncoming(interrupt_err_lv, interrupt_check_bb);
          unified_err_lv->addIncoming(err_lv, &*bb_it);
          err_lv = unified_err_lv;
        }
        if (!err_lv_returned_from_row_func) {
          err_lv_returned_from_row_func = err_lv;
        }
        // 0: row_func no_error_checked status -1: groupby internal status
        err_lv = ir_builder.CreateAnd(
            ir_builder.CreateICmp(llvm::ICmpInst::ICMP_NE,
                                  err_lv,
                                  cgen_state_->llInt(static_cast<int32_t>(0))),
            ir_builder.CreateICmp(llvm::ICmpInst::ICMP_NE,
                                  err_lv,
                                  cgen_state_->llInt(static_cast<int32_t>(-1))));

        auto error_bb = llvm::BasicBlock::Create(
            cgen_state_->context_, ".error_exit", query_func, new_bb);
        const auto error_code_arg = get_arg_by_name(query_func, "error_code");
        llvm::CallInst::Create(
            cgen_state_->module_->getFunction("record_error_code"),
            std::vector<llvm::Value*>{err_lv_returned_from_row_func, error_code_arg},
            "",
            error_bb);
        llvm::ReturnInst::Create(cgen_state_->context_, error_bb);
        llvm::ReplaceInstWithInst(&br_instr,
                                  llvm::BranchInst::Create(error_bb, new_bb, err_lv));
        done_splitting = true;
        break;
      }
    }
  }
  CHECK(done_splitting);
}

std::vector<llvm::Value*> Executor::inlineHoistedLiterals() {
  AUTOMATIC_IR_METADATA(cgen_state_.get());

  std::vector<llvm::Value*> hoisted_literals;

  // row_func_ is using literals whose defs have been hoisted up to the query_func_,
  // extend row_func_ signature to include extra args to pass these literal values.
  std::vector<llvm::Type*> row_process_arg_types;

  for (llvm::Function::arg_iterator I = cgen_state_->row_func_->arg_begin(),
                                    E = cgen_state_->row_func_->arg_end();
       I != E;
       ++I) {
    row_process_arg_types.push_back(I->getType());
  }

  for (auto& element : cgen_state_->query_func_literal_loads_) {
    for (auto value : element.second) {
      row_process_arg_types.push_back(value->getType());
    }
  }

  auto ft = llvm::FunctionType::get(
      get_int_type(32, cgen_state_->context_), row_process_arg_types, false);
  auto row_func_with_hoisted_literals =
      llvm::Function::Create(ft,
                             llvm::Function::ExternalLinkage,
                             "row_func_hoisted_literals",
                             cgen_state_->row_func_->getParent());

  auto row_func_arg_it = row_func_with_hoisted_literals->arg_begin();
  for (llvm::Function::arg_iterator I = cgen_state_->row_func_->arg_begin(),
                                    E = cgen_state_->row_func_->arg_end();
       I != E;
       ++I) {
    if (I->hasName()) {
      row_func_arg_it->setName(I->getName());
    }
    ++row_func_arg_it;
  }

  decltype(row_func_with_hoisted_literals) filter_func_with_hoisted_literals{nullptr};
  decltype(row_func_arg_it) filter_func_arg_it{nullptr};
  if (cgen_state_->filter_func_) {
    // filter_func_ is using literals whose defs have been hoisted up to the row_func_,
    // extend filter_func_ signature to include extra args to pass these literal values.
    std::vector<llvm::Type*> filter_func_arg_types;

    for (llvm::Function::arg_iterator I = cgen_state_->filter_func_->arg_begin(),
                                      E = cgen_state_->filter_func_->arg_end();
         I != E;
         ++I) {
      filter_func_arg_types.push_back(I->getType());
    }

    for (auto& element : cgen_state_->query_func_literal_loads_) {
      for (auto value : element.second) {
        filter_func_arg_types.push_back(value->getType());
      }
    }

    auto ft2 = llvm::FunctionType::get(
        get_int_type(32, cgen_state_->context_), filter_func_arg_types, false);
    filter_func_with_hoisted_literals =
        llvm::Function::Create(ft2,
                               llvm::Function::ExternalLinkage,
                               "filter_func_hoisted_literals",
                               cgen_state_->filter_func_->getParent());

    filter_func_arg_it = filter_func_with_hoisted_literals->arg_begin();
    for (llvm::Function::arg_iterator I = cgen_state_->filter_func_->arg_begin(),
                                      E = cgen_state_->filter_func_->arg_end();
         I != E;
         ++I) {
      if (I->hasName()) {
        filter_func_arg_it->setName(I->getName());
      }
      ++filter_func_arg_it;
    }
  }

  std::unordered_map<int, std::vector<llvm::Value*>>
      query_func_literal_loads_function_arguments,
      query_func_literal_loads_function_arguments2;

  for (auto& element : cgen_state_->query_func_literal_loads_) {
    std::vector<llvm::Value*> argument_values, argument_values2;

    for (auto value : element.second) {
      hoisted_literals.push_back(value);
      argument_values.push_back(&*row_func_arg_it);
      if (cgen_state_->filter_func_) {
        argument_values2.push_back(&*filter_func_arg_it);
        cgen_state_->filter_func_args_[&*row_func_arg_it] = &*filter_func_arg_it;
      }
      if (value->hasName()) {
        row_func_arg_it->setName("arg_" + value->getName());
        if (cgen_state_->filter_func_) {
          filter_func_arg_it->getContext();
          filter_func_arg_it->setName("arg_" + value->getName());
        }
      }
      ++row_func_arg_it;
      ++filter_func_arg_it;
    }

    query_func_literal_loads_function_arguments[element.first] = argument_values;
    query_func_literal_loads_function_arguments2[element.first] = argument_values2;
  }

  // copy the row_func function body over
  // see
  // https://stackoverflow.com/questions/12864106/move-function-body-avoiding-full-cloning/18751365
  row_func_with_hoisted_literals->getBasicBlockList().splice(
      row_func_with_hoisted_literals->begin(),
      cgen_state_->row_func_->getBasicBlockList());

  // also replace row_func arguments with the arguments from row_func_hoisted_literals
  for (llvm::Function::arg_iterator I = cgen_state_->row_func_->arg_begin(),
                                    E = cgen_state_->row_func_->arg_end(),
                                    I2 = row_func_with_hoisted_literals->arg_begin();
       I != E;
       ++I) {
    I->replaceAllUsesWith(&*I2);
    I2->takeName(&*I);
    cgen_state_->filter_func_args_.replace(&*I, &*I2);
    ++I2;
  }

  cgen_state_->row_func_ = row_func_with_hoisted_literals;

  // and finally replace  literal placeholders
  std::vector<llvm::Instruction*> placeholders;
  std::string prefix("__placeholder__literal_");
  for (auto it = llvm::inst_begin(row_func_with_hoisted_literals),
            e = llvm::inst_end(row_func_with_hoisted_literals);
       it != e;
       ++it) {
    if (it->hasName() && it->getName().startswith(prefix)) {
      auto offset_and_index_entry =
          cgen_state_->row_func_hoisted_literals_.find(llvm::dyn_cast<llvm::Value>(&*it));
      CHECK(offset_and_index_entry != cgen_state_->row_func_hoisted_literals_.end());

      int lit_off = offset_and_index_entry->second.offset_in_literal_buffer;
      int lit_idx = offset_and_index_entry->second.index_of_literal_load;

      it->replaceAllUsesWith(
          query_func_literal_loads_function_arguments[lit_off][lit_idx]);
      placeholders.push_back(&*it);
    }
  }
  for (auto placeholder : placeholders) {
    placeholder->removeFromParent();
  }

  if (cgen_state_->filter_func_) {
    // copy the filter_func function body over
    // see
    // https://stackoverflow.com/questions/12864106/move-function-body-avoiding-full-cloning/18751365
    filter_func_with_hoisted_literals->getBasicBlockList().splice(
        filter_func_with_hoisted_literals->begin(),
        cgen_state_->filter_func_->getBasicBlockList());

    // also replace filter_func arguments with the arguments from
    // filter_func_hoisted_literals
    for (llvm::Function::arg_iterator I = cgen_state_->filter_func_->arg_begin(),
                                      E = cgen_state_->filter_func_->arg_end(),
                                      I2 = filter_func_with_hoisted_literals->arg_begin();
         I != E;
         ++I) {
      I->replaceAllUsesWith(&*I2);
      I2->takeName(&*I);
      ++I2;
    }

    cgen_state_->filter_func_ = filter_func_with_hoisted_literals;

    // and finally replace  literal placeholders
    std::vector<llvm::Instruction*> placeholders;
    std::string prefix("__placeholder__literal_");
    for (auto it = llvm::inst_begin(filter_func_with_hoisted_literals),
              e = llvm::inst_end(filter_func_with_hoisted_literals);
         it != e;
         ++it) {
      if (it->hasName() && it->getName().startswith(prefix)) {
        auto offset_and_index_entry = cgen_state_->row_func_hoisted_literals_.find(
            llvm::dyn_cast<llvm::Value>(&*it));
        CHECK(offset_and_index_entry != cgen_state_->row_func_hoisted_literals_.end());

        int lit_off = offset_and_index_entry->second.offset_in_literal_buffer;
        int lit_idx = offset_and_index_entry->second.index_of_literal_load;

        it->replaceAllUsesWith(
            query_func_literal_loads_function_arguments2[lit_off][lit_idx]);
        placeholders.push_back(&*it);
      }
    }
    for (auto placeholder : placeholders) {
      placeholder->removeFromParent();
    }
  }

  return hoisted_literals;
}

namespace {

size_t get_shared_memory_size(const bool shared_mem_used,
                              const QueryMemoryDescriptor* query_mem_desc_ptr) {
  return shared_mem_used
             ? (query_mem_desc_ptr->getRowSize() * query_mem_desc_ptr->getEntryCount())
             : 0;
}

#ifndef NDEBUG
std::string serialize_llvm_metadata_footnotes(llvm::Function* query_func,
                                              CgenState* cgen_state) {
  std::string llvm_ir;
  std::unordered_set<llvm::MDNode*> md;

  // Loop over all instructions in the query function.
  for (auto bb_it = query_func->begin(); bb_it != query_func->end(); ++bb_it) {
    for (auto instr_it = bb_it->begin(); instr_it != bb_it->end(); ++instr_it) {
      llvm::SmallVector<std::pair<unsigned, llvm::MDNode*>, 100> imd;
      instr_it->getAllMetadata(imd);
      for (auto [kind, node] : imd) {
        md.insert(node);
      }
    }
  }

  // Loop over all instructions in the row function.
  for (auto bb_it = cgen_state->row_func_->begin(); bb_it != cgen_state->row_func_->end();
       ++bb_it) {
    for (auto instr_it = bb_it->begin(); instr_it != bb_it->end(); ++instr_it) {
      llvm::SmallVector<std::pair<unsigned, llvm::MDNode*>, 100> imd;
      instr_it->getAllMetadata(imd);
      for (auto [kind, node] : imd) {
        md.insert(node);
      }
    }
  }

  // Loop over all instructions in the filter function.
  if (cgen_state->filter_func_) {
    for (auto bb_it = cgen_state->filter_func_->begin();
         bb_it != cgen_state->filter_func_->end();
         ++bb_it) {
      for (auto instr_it = bb_it->begin(); instr_it != bb_it->end(); ++instr_it) {
        llvm::SmallVector<std::pair<unsigned, llvm::MDNode*>, 100> imd;
        instr_it->getAllMetadata(imd);
        for (auto [kind, node] : imd) {
          md.insert(node);
        }
      }
    }
  }

  // Sort the metadata by canonical number and convert to text.
  if (!md.empty()) {
    std::map<size_t, std::string> sorted_strings;
    for (auto p : md) {
      std::string str;
      llvm::raw_string_ostream os(str);
      p->print(os, cgen_state->module_, true);
      os.flush();
      auto fields = split(str, {}, 1);
      if (fields.empty() || fields[0].empty()) {
        continue;
      }
      sorted_strings.emplace(std::stoul(fields[0].substr(1)), str);
    }
    llvm_ir += "\n";
    for (auto [id, text] : sorted_strings) {
      llvm_ir += text;
      llvm_ir += "\n";
    }
  }

  return llvm_ir;
}
#endif  // NDEBUG

}  // namespace

std::tuple<CompilationResult, std::unique_ptr<QueryMemoryDescriptor>>
Executor::compileWorkUnit(const std::vector<InputTableInfo>& query_infos,
                          const RelAlgExecutionUnit& ra_exe_unit,
                          const CompilationOptions& co,
                          const ExecutionOptions& eo,
                          const bool allow_lazy_fetch,
                          std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                          const size_t max_groups_buffer_entry_guess,
                          const int8_t crt_min_byte_width,
                          const bool has_cardinality_estimation,
                          DataProvider* data_provider,
                          ColumnCacheMap& column_cache) {
#ifndef NDEBUG
  static std::uint64_t counter = 0;
  ++counter;
  VLOG(1) << "CODEGEN #" << counter << ":";
  LOG(IR) << "CODEGEN #" << counter << ":";
  LOG(PTX) << "CODEGEN #" << counter << ":";
  LOG(ASM) << "CODEGEN #" << counter << ":";
#endif

  // cgenstate_manager uses RAII pattern to manage the live time of
  // CgenState instances.
  Executor::CgenStateManager cgenstate_manager(*this,
                                               allow_lazy_fetch,
                                               query_infos,
                                               &ra_exe_unit);  // locks compilation_mutex

  GroupByAndAggregate group_by_and_aggregate(
      this,
      ra_exe_unit,
      query_infos,
      row_set_mem_owner,
      has_cardinality_estimation ? std::optional<int64_t>(max_groups_buffer_entry_guess)
                                 : std::nullopt);
  auto query_mem_desc =
      group_by_and_aggregate.initQueryMemoryDescriptor(eo.allow_multifrag,
                                                       max_groups_buffer_entry_guess,
                                                       crt_min_byte_width,
                                                       eo.output_columnar_hint,
                                                       co);

  if (query_mem_desc->getQueryDescriptionType() ==
          QueryDescriptionType::GroupByBaselineHash &&
      !has_cardinality_estimation && !eo.just_explain) {
    const auto col_range_info = group_by_and_aggregate.getColRangeInfo();
    CIDER_THROW(CiderCompileException,
                fmt::format("Cardinality Estimation Required : {}",
                            col_range_info.max - col_range_info.min));
  }

  const bool output_columnar = query_mem_desc->didOutputColumnar();

  // Read the module template and target either CPU
  // by binding the stream position functions to the right implementation:
  // contiguous for CPU

  CHECK(cgen_state_->module_ == nullptr);
  cgen_state_->set_module_shallow_copy(get_rt_module(), /*always_clone=*/true);
  AUTOMATIC_IR_METADATA(cgen_state_.get());

  auto agg_fnames =
      get_agg_fnames(ra_exe_unit.target_exprs, !ra_exe_unit.groupby_exprs.empty());

  const auto agg_slot_count = ra_exe_unit.estimator ? size_t(1) : agg_fnames.size();

  const bool is_group_by{query_mem_desc->isGroupBy()};
  const bool is_project{query_mem_desc->getQueryDescriptionType() ==
                        QueryDescriptionType::Projection};
  auto [query_func, row_func_call] =
      is_group_by ? query_group_by_template(cgen_state_->module_,
                                            co.hoist_literals,
                                            *query_mem_desc,
                                            ra_exe_unit.scan_limit,
                                            co.use_cider_groupby_hash && !is_project)
                  : query_template(cgen_state_->module_,
                                   agg_slot_count,
                                   co.hoist_literals,
                                   !!ra_exe_unit.estimator,
                                   co.use_cider_data_format);
  bind_pos_placeholders("pos_start", true, query_func, cgen_state_->module_);
  bind_pos_placeholders("group_buff_idx", false, query_func, cgen_state_->module_);
  bind_pos_placeholders("pos_step", false, query_func, cgen_state_->module_);

  cgen_state_->query_func_ = query_func;
  cgen_state_->row_func_call_ = row_func_call;
  cgen_state_->query_func_entry_ir_builder_.SetInsertPoint(
      &query_func->getEntryBlock().front());

  // Generate the function signature and column head fetches s.t.
  // double indirection isn't needed in the inner loop
  auto& fetch_bb = query_func->front();
  llvm::IRBuilder<> fetch_ir_builder(&fetch_bb);
  fetch_ir_builder.SetInsertPoint(&*fetch_bb.begin());
  auto col_heads = generate_column_heads_load(ra_exe_unit.input_col_descs.size(),
                                              query_func->args().begin(),
                                              fetch_ir_builder,
                                              cgen_state_->context_);
  CHECK_EQ(ra_exe_unit.input_col_descs.size(), col_heads.size());

  cgen_state_->row_func_ = create_row_function(ra_exe_unit.input_col_descs.size(),
                                               is_group_by ? 0 : agg_slot_count,
                                               co.hoist_literals,
                                               cgen_state_->module_,
                                               cgen_state_->context_,
                                               co.use_cider_data_format);
  CHECK(cgen_state_->row_func_);
  cgen_state_->row_func_bb_ =
      llvm::BasicBlock::Create(cgen_state_->context_, "entry", cgen_state_->row_func_);

  cgen_state_->current_func_ = cgen_state_->row_func_;
  cgen_state_->ir_builder_.SetInsertPoint(cgen_state_->row_func_bb_);

  preloadFragOffsets(ra_exe_unit.input_descs, query_infos);
  RelAlgExecutionUnit body_execution_unit = ra_exe_unit;
  const auto join_loops = buildJoinLoops(
      body_execution_unit, co, eo, query_infos, data_provider, column_cache);

  plan_state_->allocateLocalColumnIds(ra_exe_unit.input_col_descs);
  for (auto& simple_qual : ra_exe_unit.simple_quals) {
    plan_state_->addSimpleQual(simple_qual);
  }
  if (!join_loops.empty()) {
    codegenJoinLoops(join_loops,
                     body_execution_unit,
                     group_by_and_aggregate,
                     query_func,
                     cgen_state_->row_func_bb_,
                     *(query_mem_desc.get()),
                     co,
                     eo);
  } else {
    const bool can_return_error =
        compileBody(ra_exe_unit, group_by_and_aggregate, *query_mem_desc, co);
    if (can_return_error || co.needs_error_check || eo.with_dynamic_watchdog ||
        eo.allow_runtime_query_interrupt) {
      createErrorCheckControlFlow(query_func,
                                  eo.with_dynamic_watchdog,
                                  eo.allow_runtime_query_interrupt,
                                  group_by_and_aggregate.query_infos_);
    }
  }
  std::vector<llvm::Value*> hoisted_literals;

  if (co.hoist_literals) {
    VLOG(1) << "number of hoisted literals: "
            << cgen_state_->query_func_literal_loads_.size()
            << " / literal buffer usage: " << cgen_state_->getLiteralBufferUsage(0)
            << " bytes";
  }

  if (co.hoist_literals && !cgen_state_->query_func_literal_loads_.empty()) {
    // we have some hoisted literals...
    hoisted_literals = inlineHoistedLiterals();
  }

  // replace the row func placeholder call with the call to the actual row func
  std::vector<llvm::Value*> row_func_args;
  for (size_t i = 0; i < cgen_state_->row_func_call_->getNumArgOperands(); ++i) {
    row_func_args.push_back(cgen_state_->row_func_call_->getArgOperand(i));
  }
  row_func_args.insert(row_func_args.end(), col_heads.begin(), col_heads.end());
  row_func_args.push_back(get_arg_by_name(query_func, "join_hash_tables"));
  // push hoisted literals arguments, if any
  row_func_args.insert(
      row_func_args.end(), hoisted_literals.begin(), hoisted_literals.end());
  llvm::ReplaceInstWithInst(
      cgen_state_->row_func_call_,
      llvm::CallInst::Create(cgen_state_->row_func_, row_func_args, ""));

  // replace the filter func placeholder call with the call to the actual filter func
  if (cgen_state_->filter_func_) {
    std::vector<llvm::Value*> filter_func_args;
    for (auto arg_it = cgen_state_->filter_func_args_.begin();
         arg_it != cgen_state_->filter_func_args_.end();
         ++arg_it) {
      filter_func_args.push_back(arg_it->first);
    }
    llvm::ReplaceInstWithInst(
        cgen_state_->filter_func_call_,
        llvm::CallInst::Create(cgen_state_->filter_func_, filter_func_args, ""));
  }

  // Aggregate
  plan_state_->init_agg_vals_ =
      init_agg_val_vec(ra_exe_unit.target_exprs, ra_exe_unit.quals, *query_mem_desc);

  auto multifrag_query_func =
      co.use_cider_groupby_hash && !is_project
          ? cgen_state_->module_->getFunction("query_hoisted_literals_with_row_skip_mask")
          : cgen_state_->module_->getFunction(
                "multifrag_query" +
                std::string(co.hoist_literals ? "_hoisted_literals" : ""));
  CHECK(multifrag_query_func);

  bind_query(
      query_func,
      co.use_cider_groupby_hash && !is_project
          ? "query_stub_hoisted_literals_with_row_skip_mask"
          : "query_stub" + std::string(co.hoist_literals ? "_hoisted_literals" : ""),
      multifrag_query_func,
      cgen_state_->module_);

  std::vector<llvm::Function*> root_funcs{query_func, cgen_state_->row_func_};
  if (cgen_state_->filter_func_) {
    root_funcs.push_back(cgen_state_->filter_func_);
  }
  auto live_funcs = CodeGenerator::markDeadRuntimeFuncs(
      *cgen_state_->module_, root_funcs, {multifrag_query_func});

  // Always inline the row function and the filter function.
  // We don't want register spills in the inner loops.
  // LLVM seems to correctly free up alloca instructions
  // in these functions even when they are inlined.
  mark_function_always_inline(cgen_state_->row_func_);
  if (cgen_state_->filter_func_) {
    mark_function_always_inline(cgen_state_->filter_func_);
  }

#ifndef NDEBUG
  // Add helpful metadata to the LLVM IR for debugging.
  AUTOMATIC_IR_METADATA_DONE();
#endif

  // Serialize the important LLVM IR functions to text for SQL EXPLAIN.
  std::string llvm_ir;
  //  if (eo.just_explain) {
  if (true) {  // we always want IR
    if (co.explain_type == ExecutorExplainType::Optimized) {
#ifdef WITH_JIT_DEBUG
      CIDER_THROW(
          CiderCompileException,
          "Explain optimized not available when JIT runtime debug symbols are enabled");
#else
      // Note that we don't run the NVVM reflect pass here. Use LOG(IR) to get the
      // optimized IR after NVVM reflect
      llvm::legacy::PassManager pass_manager;
      optimize_ir(query_func, cgen_state_->module_, pass_manager, live_funcs, co);
#endif  // WITH_JIT_DEBUG
    }
    llvm_ir =
        serialize_llvm_object(multifrag_query_func) + serialize_llvm_object(query_func) +
        serialize_llvm_object(cgen_state_->row_func_) +
        (cgen_state_->filter_func_ ? serialize_llvm_object(cgen_state_->filter_func_)
                                   : "");

#ifndef NDEBUG
    llvm_ir += serialize_llvm_metadata_footnotes(query_func, cgen_state_.get());
#endif
  }

  LOG(IR) << "\n\n" << query_mem_desc->toString() << "\n";
  LOG(DEBUG1) << "IR for the CPU: \n";
#ifndef NDEBUG
  LOG(DEBUG1) << serialize_llvm_object(query_func)
              << serialize_llvm_object(cgen_state_->row_func_)
              << (cgen_state_->filter_func_
                      ? serialize_llvm_object(cgen_state_->filter_func_)
                      : "")
              << "\nEnd of IR";
#else
  LOG(IR) << serialize_llvm_object(cgen_state_->module_) << "\nEnd of IR";
#endif

  // Run some basic validation checks on the LLVM IR before code is generated below.
  verify_function_ir(cgen_state_->row_func_);
  if (cgen_state_->filter_func_) {
    verify_function_ir(cgen_state_->filter_func_);
  }

  // Generate final native code from the LLVM IR.
  return std::make_tuple(
      CompilationResult{
          optimizeAndCodegenCPU(query_func, multifrag_query_func, live_funcs, co),
          cgen_state_->getLiterals(),
          output_columnar,
          llvm_ir,
          getJoinHashTablePtrs(0)},  // cpu device id is always 0
      std::move(query_mem_desc));
}

void Executor::insertErrorCodeChecker(llvm::Function* query_func,
                                      bool hoist_literals,
                                      bool allow_runtime_query_interrupt) {
  auto query_stub_func_name =
      "query_stub" + std::string(hoist_literals ? "_hoisted_literals" : "");
  for (auto bb_it = query_func->begin(); bb_it != query_func->end(); ++bb_it) {
    for (auto inst_it = bb_it->begin(); inst_it != bb_it->end(); ++inst_it) {
      if (!llvm::isa<llvm::CallInst>(*inst_it)) {
        continue;
      }
      auto& row_func_call = llvm::cast<llvm::CallInst>(*inst_it);
      if (std::string(row_func_call.getCalledFunction()->getName()) ==
          query_stub_func_name) {
        auto next_inst_it = inst_it;
        ++next_inst_it;
        auto new_bb = bb_it->splitBasicBlock(next_inst_it);
        auto& br_instr = bb_it->back();
        llvm::IRBuilder<> ir_builder(&br_instr);
        llvm::Value* err_lv = &*inst_it;
        auto error_check_bb =
            bb_it->splitBasicBlock(llvm::BasicBlock::iterator(br_instr), ".error_check");
        llvm::Value* error_code_arg = nullptr;
        auto arg_cnt = 0;
        for (auto arg_it = query_func->arg_begin(); arg_it != query_func->arg_end();
             arg_it++, ++arg_cnt) {
          // since multi_frag_* func has anonymous arguments so we use arg_offset
          // explicitly to capture "error_code" argument in the func's argument list
          if (hoist_literals) {
            if (arg_cnt == 9) {
              error_code_arg = &*arg_it;
              break;
            }
          } else {
            if (arg_cnt == 8) {
              error_code_arg = &*arg_it;
              break;
            }
          }
        }
        CHECK(error_code_arg);
        llvm::Value* err_code = nullptr;
        if (allow_runtime_query_interrupt) {
          // decide the final error code with a consideration of interrupt status
          auto& check_interrupt_br_instr = bb_it->back();
          auto interrupt_check_bb = llvm::BasicBlock::Create(
              cgen_state_->context_, ".interrupt_check", query_func, error_check_bb);
          llvm::IRBuilder<> interrupt_checker_ir_builder(interrupt_check_bb);
          auto detected_interrupt = interrupt_checker_ir_builder.CreateCall(
              cgen_state_->module_->getFunction("check_interrupt"), {});
          auto detected_error = interrupt_checker_ir_builder.CreateCall(
              cgen_state_->module_->getFunction("get_error_code"),
              std::vector<llvm::Value*>{error_code_arg});
          err_code = interrupt_checker_ir_builder.CreateSelect(
              detected_interrupt,
              cgen_state_->llInt(Executor::ERR_INTERRUPTED),
              detected_error);
          interrupt_checker_ir_builder.CreateBr(error_check_bb);
          llvm::ReplaceInstWithInst(&check_interrupt_br_instr,
                                    llvm::BranchInst::Create(interrupt_check_bb));
          ir_builder.SetInsertPoint(&br_instr);
        } else {
          // uses error code returned from row_func and skip to check interrupt status
          ir_builder.SetInsertPoint(&br_instr);
          err_code =
              ir_builder.CreateCall(cgen_state_->module_->getFunction("get_error_code"),
                                    std::vector<llvm::Value*>{error_code_arg});
        }
        err_lv = ir_builder.CreateICmp(
            llvm::ICmpInst::ICMP_NE, err_code, cgen_state_->llInt(0));
        auto error_bb = llvm::BasicBlock::Create(
            cgen_state_->context_, ".error_exit", query_func, new_bb);
        llvm::CallInst::Create(cgen_state_->module_->getFunction("record_error_code"),
                               std::vector<llvm::Value*>{err_code, error_code_arg},
                               "",
                               error_bb);
        llvm::ReturnInst::Create(cgen_state_->context_, error_bb);
        llvm::ReplaceInstWithInst(&br_instr,
                                  llvm::BranchInst::Create(error_bb, new_bb, err_lv));
        break;
      }
    }
  }
}

bool Executor::compileBody(const RelAlgExecutionUnit& ra_exe_unit,
                           GroupByAndAggregate& group_by_and_aggregate,
                           const QueryMemoryDescriptor& query_mem_desc,
                           const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_.get());

  // Switch the code generation into a separate filter function if enabled.
  // Note that accesses to function arguments are still codegenned from the
  // row function's arguments, then later automatically forwarded and
  // remapped into filter function arguments by redeclareFilterFunction().
  cgen_state_->row_func_bb_ = cgen_state_->ir_builder_.GetInsertBlock();
  llvm::Value* loop_done{nullptr};
  std::unique_ptr<Executor::FetchCacheAnchor> fetch_cache_anchor;
  if (cgen_state_->filter_func_) {
    if (cgen_state_->row_func_bb_->getName() == "loop_body") {
      auto row_func_entry_bb = &cgen_state_->row_func_->getEntryBlock();
      cgen_state_->ir_builder_.SetInsertPoint(row_func_entry_bb,
                                              row_func_entry_bb->begin());
      loop_done = cgen_state_->ir_builder_.CreateAlloca(
          get_int_type(1, cgen_state_->context_), nullptr, "loop_done");
      cgen_state_->ir_builder_.SetInsertPoint(cgen_state_->row_func_bb_);
      cgen_state_->ir_builder_.CreateStore(cgen_state_->llBool(true), loop_done);
    }
    cgen_state_->ir_builder_.SetInsertPoint(cgen_state_->filter_func_bb_);
    cgen_state_->current_func_ = cgen_state_->filter_func_;
    fetch_cache_anchor = std::make_unique<Executor::FetchCacheAnchor>(cgen_state_.get());
  }
  // generate the code for the filter
  std::vector<Analyzer::Expr*> primary_quals;
  std::vector<Analyzer::Expr*> deferred_quals;
  bool short_circuited = CodeGenerator::prioritizeQuals(
      ra_exe_unit, primary_quals, deferred_quals, plan_state_->hoisted_filters_);
  if (short_circuited) {
    VLOG(1) << "Prioritized " << std::to_string(primary_quals.size()) << " quals, "
            << "short-circuited and deferred " << std::to_string(deferred_quals.size())
            << " quals";
  }
  llvm::Value* filter_lv = cgen_state_->llBool(true);
  CodeGenerator code_generator(this);
  auto filter_builder = [this, &code_generator, &co](
                            llvm::Value* filter_lv,
                            Analyzer::Expr* expr) -> llvm::Value* {
    auto expr_value = code_generator.codegen(expr, co, true);
    auto fixedsize_expr_value = dynamic_cast<FixedSizeColValues*>(expr_value.get());
    CHECK(fixedsize_expr_value);
    if (llvm::Value* is_null = fixedsize_expr_value->getNull()) {
      filter_lv = cgen_state_->ir_builder_.CreateAnd(
          filter_lv, cgen_state_->ir_builder_.CreateNot(is_null));
    }
    // need to convert to bool expr first.
    auto cond = code_generator.toBool(fixedsize_expr_value->getValue());
    return cgen_state_->ir_builder_.CreateAnd(filter_lv, cond);
  };
  for (auto expr : primary_quals) {
    // Generate the filter for primary quals
    if (co.use_cider_data_format) {
      filter_lv = filter_builder(filter_lv, expr);
    } else {
      auto cond = code_generator.toBool(code_generator.codegen(expr, true, co).front());
      filter_lv = cgen_state_->ir_builder_.CreateAnd(filter_lv, cond);
    }
  }
  CHECK(filter_lv->getType()->isIntegerTy(1));
  llvm::BasicBlock* sc_false{nullptr};
  if (!deferred_quals.empty()) {
    auto sc_true = llvm::BasicBlock::Create(
        cgen_state_->context_, "sc_true", cgen_state_->current_func_);
    sc_false = llvm::BasicBlock::Create(
        cgen_state_->context_, "sc_false", cgen_state_->current_func_);
    cgen_state_->ir_builder_.CreateCondBr(filter_lv, sc_true, sc_false);
    cgen_state_->ir_builder_.SetInsertPoint(sc_false);
    if (ra_exe_unit.join_quals.empty()) {
      cgen_state_->ir_builder_.CreateRet(cgen_state_->llInt(int32_t(0)));
    }
    cgen_state_->ir_builder_.SetInsertPoint(sc_true);
    filter_lv = cgen_state_->llBool(true);
  }
  for (auto expr : deferred_quals) {
    if (co.use_cider_data_format) {
      filter_lv = filter_builder(filter_lv, expr);
    } else {
      filter_lv = cgen_state_->ir_builder_.CreateAnd(
          filter_lv,
          code_generator.toBool(code_generator.codegen(expr, true, co).front()));
    }
  }

  CHECK(filter_lv->getType()->isIntegerTy(1));
  auto ret = group_by_and_aggregate.codegen(filter_lv, sc_false, query_mem_desc, co);

  // Switch the code generation back to the row function if a filter
  // function was enabled.
  if (cgen_state_->filter_func_) {
    if (cgen_state_->row_func_bb_->getName() == "loop_body") {
      cgen_state_->ir_builder_.CreateStore(cgen_state_->llBool(false), loop_done);
      cgen_state_->ir_builder_.CreateRet(cgen_state_->llInt<int32_t>(0));
    }

    cgen_state_->ir_builder_.SetInsertPoint(cgen_state_->row_func_bb_);
    cgen_state_->current_func_ = cgen_state_->row_func_;
    cgen_state_->filter_func_call_ =
        cgen_state_->ir_builder_.CreateCall(cgen_state_->filter_func_, {});

    // Create real filter function declaration after placeholder call
    // is emitted.
    redeclareFilterFunction();

    if (cgen_state_->row_func_bb_->getName() == "loop_body") {
      auto loop_done_true = llvm::BasicBlock::Create(
          cgen_state_->context_, "loop_done_true", cgen_state_->row_func_);
      auto loop_done_false = llvm::BasicBlock::Create(
          cgen_state_->context_, "loop_done_false", cgen_state_->row_func_);
      auto loop_done_flag = cgen_state_->ir_builder_.CreateLoad(loop_done);
      cgen_state_->ir_builder_.CreateCondBr(
          loop_done_flag, loop_done_true, loop_done_false);
      cgen_state_->ir_builder_.SetInsertPoint(loop_done_true);
      cgen_state_->ir_builder_.CreateRet(cgen_state_->filter_func_call_);
      cgen_state_->ir_builder_.SetInsertPoint(loop_done_false);
    } else {
      cgen_state_->ir_builder_.CreateRet(cgen_state_->filter_func_call_);
    }
  }
  return ret;
}

std::vector<llvm::Value*> generate_column_heads_load(const int num_columns,
                                                     llvm::Value* byte_stream_arg,
                                                     llvm::IRBuilder<>& ir_builder,
                                                     llvm::LLVMContext& ctx) {
  CHECK(byte_stream_arg);
  const auto max_col_local_id = num_columns - 1;

  std::vector<llvm::Value*> col_heads;
  for (int col_id = 0; col_id <= max_col_local_id; ++col_id) {
    col_heads.emplace_back(ir_builder.CreateLoad(ir_builder.CreateGEP(
        byte_stream_arg, llvm::ConstantInt::get(llvm::Type::getInt32Ty(ctx), col_id))));
  }
  return col_heads;
}
