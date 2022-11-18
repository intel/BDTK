#ifndef NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
#define NEXTGEN_CONTEXT_RUNTIMECONTEXT_H

#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/CodegenContext.h"

namespace cider::exec::nextgen::context {
class RuntimeContext {
 public:
  RuntimeContext(int64_t ctx_num) : runtime_ctx_pointers_(ctx_num, nullptr) {}

  size_t getContextItemNum() const { return runtime_ctx_pointers_.size(); }

  void* getContextItem(size_t id) { return runtime_ctx_pointers_[id]; }

  void addBatch(const CodegenContext::BatchDescriptorPtr& descriptor);

  void instantiate(const CiderAllocatorPtr& allocator);

 private:
  std::vector<void*> runtime_ctx_pointers_;
  std::vector<std::pair<CodegenContext::BatchDescriptorPtr, BatchPtr>> batch_holder_;
};

using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
