#ifndef JIT_MODULE_H
#define JIT_MODULE_H

#include "exec/nextgen/jitlib/base/JITFunction.h"

namespace jitlib {
template <typename JITModuleImpl, typename JITFunctionImpl>
class JITModule {
 public:
  JITFunctionImpl createJITFunction(const JITFunctionDescriptor& descriptor) {
    return getImpl()->createJITFunctionImpl(descriptor);
  }

  void finish() { getImpl()->finishImpl(); }

  void* getFunctionPtr(JITFunctionImpl& function) {
    return getImpl()->getFunctionPtrImpl(function);
  }

 private:
  JITModuleImpl* getImpl() { return static_cast<JITModuleImpl*>(this); }
};
};  // namespace jitlib

#endif