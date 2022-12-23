
#include "exec/nextgen/jitlib/llvmjit/LLVMJITTargets.h"

#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITTargets.h"

namespace cider::jitlib {
static const char* avx256_inst_sets[] = {"avx", "avx2"};
static const char* avx512_inst_sets[] = {"avx512ifma",
                                         "avx512bitalg",
                                         "avx512er",
                                         "avx512vnni",
                                         "avx512vpopcntdq",
                                         "avx512f",
                                         "avx512bw",
                                         "avx512vbmi2",
                                         "avx512vl",
                                         "avx512cd",
                                         "avx512vbmi",
                                         "avx512bf16",
                                         "avx512dq",
                                         "avx512pf"};

static const std::string process_triple = llvm::sys::getProcessTriple();
static const std::string process_name = llvm::sys::getHostCPUName();
static const llvm::Target* host_target = []() {
  // Initialize LLVM runtime env
  llvm::InitializeNativeTarget();
  llvm::InitializeAllTargetMCs();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  std::string error;
  auto* target = llvm::TargetRegistry::lookupTarget(process_triple, error);
  if (nullptr == target) {
    LOG(FATAL) << "Unable to initialize host target, process triple: " << process_triple
               << ", error: " << error << ".";
  }
  return target;
}();

static llvm::StringMap<bool> host_supported_features = []() {
  llvm::StringMap<bool> features;
  if (!llvm::sys::getHostCPUFeatures(features)) {
    LOG(FATAL) << "Unable to get host supported features.";
  }
  // TBD (bigPYJ1151): whether need to filter unused instruction sets.
  return features;
}();

static llvm::SubtargetFeatures buildTargetFeatures(const CompilationOptions& co) {
  llvm::StringMap<bool> features_copy(host_supported_features);
  auto switch_inst_set = [&features_copy](auto&& sets, bool flag) {
    for (auto feature : sets) {
      if (auto iter = features_copy.find(feature); features_copy.end() != iter) {
        iter->second &= flag;
      }
    }
  };

  switch_inst_set(avx256_inst_sets, co.enable_avx2);
  switch_inst_set(avx512_inst_sets, co.enable_avx512);

  llvm::SubtargetFeatures features;
  for (auto&& entry : features_copy) {
    features.AddFeature(entry.getKey(), entry.getValue());
  }
  return features;
}

static llvm::TargetOptions buildTargetOptions() {
  llvm::TargetOptions to;
  to.EnableFastISel = true;
  to.MCOptions.AsmVerbose = false;

  return to;
}

llvm::TargetMachine* buildTargetMachine(const jitlib::CompilationOptions& co) {
  return host_target->createTargetMachine(process_triple,
                                          process_name,
                                          buildTargetFeatures(co).getString(),
                                          buildTargetOptions(),
                                          llvm::None,
                                          llvm::None,
                                          co.aggressive_jit_compile
                                              ? llvm::CodeGenOpt::Aggressive
                                              : llvm::CodeGenOpt::Default,
                                          true);
}
}  // namespace cider::jitlib