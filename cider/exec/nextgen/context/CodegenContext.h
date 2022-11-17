#ifndef NEXTGEN_CONTEXT_CODEGENCONTEXT_H
#define NEXTGEN_CONTEXT_CODEGENCONTEXT_H

#include <cstddef>

namespace cider::jitlib {
class JITFunction;
}

namespace cider::exec::nextgen::context {
struct CodegenCtxDescriptor {
  
};

class CodegenContext {
 public:
 private:
  cider::jitlib::JITFunction* jit_func_;
   
};
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CODEGENCONTEXT_H
