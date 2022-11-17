#ifndef NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
#define NEXTGEN_CONTEXT_RUNTIMECONTEXT_H

#include <memory>
#include <vector>

namespace cider::exec::nextgen::context {

class RuntimeContext {
 public:
  void* getContextItem(size_t id) { return runtime_ctx_pointers_[id]; }

 private:
  std::vector<void*> runtime_ctx_pointers_;
};

using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
