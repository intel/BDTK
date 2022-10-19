#ifndef JITLIB_LLVM_JIT_CONTROL_FLOW_H
#define JITLIB_LLVM_JIT_CONTROL_FLOW_H

#include "exec/nextgen/jitlib/base/ControlFlow.h"
#include "exec/nextgen/jitlib/base/Values.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"

namespace jitlib {
template <TypeTag Type>
class Ret<Type, LLVMJITFunction> {
 public:
  explicit Ret(LLVMJITFunction& function) { function.getIRBuilder()->CreateRetVoid(); }

  explicit Ret(LLVMJITFunction& function, Value<Type, LLVMJITFunction>& ret_value) {
    function.getIRBuilder()->CreateRet(ret_value.load());
  }
};
};  // namespace jitlib
#endif