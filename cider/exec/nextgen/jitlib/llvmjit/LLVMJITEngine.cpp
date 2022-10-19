
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"

#include <llvm/IR/Module.h>
#include <llvm/Support/CodeGen.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetOptions.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"

namespace jitlib {
LLVMJITEngine::~LLVMJITEngine() {
  LLVMDisposeExecutionEngine(llvm::wrap(engine));
}

LLVMJITEngineBuilder::LLVMJITEngineBuilder(LLVMJITModule& module) : module_(module) {
  llvm::InitializeNativeTarget();
  llvm::InitializeAllTargetMCs();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
}

static llvm::TargetOptions buildTargetOptions() {
  llvm::TargetOptions to;
  to.EnableFastISel = true;

  return to;
}

std::unique_ptr<LLVMJITEngine> LLVMJITEngineBuilder::build() {
  std::string error;
  llvm::EngineBuilder eb(std::move(module_.module_));

  eb.setMCPU(llvm::sys::getHostCPUName().str())
      .setEngineKind(llvm::EngineKind::JIT)
      .setTargetOptions(buildTargetOptions())
      .setOptLevel(llvm::CodeGenOpt::None)
      .setErrorStr(&error);

  auto engine = std::make_unique<LLVMJITEngine>();
  engine->engine = eb.create();
  engine->engine->finalizeObject();

  return engine;
}
};  // namespace jitlib
