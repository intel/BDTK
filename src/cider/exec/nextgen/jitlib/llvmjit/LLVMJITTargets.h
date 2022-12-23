#ifndef JITLIB_LLVMJIT_LLVMJITTARGETS_H
#define JITLIB_LLVMJIT_LLVMJITTARGETS_H

namespace llvm {
class TargetMachine;
}

namespace cider::jitlib {
struct CompilationOptions;

llvm::TargetMachine* buildTargetMachine(const CompilationOptions& co);
}  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITTARGETS_H
