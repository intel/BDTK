#ifndef NEXTGEN_CONTEXT_CODEGENCONTEXT_H
#define NEXTGEN_CONTEXT_CODEGENCONTEXT_H

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "include/cider/CiderAllocator.h"
#include "type/data/sqltypes.h"

namespace cider::jitlib {
class JITFunction;
}

namespace cider::exec::nextgen::context {
class RuntimeContext;
using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;

class CodegenContext {
 public:
  CodegenContext() : jit_func_(nullptr) {}

  void setJITFunction(jitlib::JITFunction* jit_func) {
    CHECK(nullptr == jit_func_);
    jit_func_ = jit_func;
  }

 public:
  jitlib::JITValuePointer registerBatch(const SQLTypeInfo& type,
                                        const std::string& name = "");

  // TBD: HashTable (GroupBy, Join), other objects registration.

  RuntimeCtxPtr generateRuntimeCTX(const CiderAllocatorPtr& allocator) const;

  struct BatchDescriptor {
    int64_t ctx_id;
    std::string name;
    SQLTypeInfo type;

    BatchDescriptor(int64_t id, const std::string& n, const SQLTypeInfo& t)
        : ctx_id(id), name(n), type(t) {}
  };

  using BatchDescriptorPtr = std::shared_ptr<BatchDescriptor>;

 private:
  int64_t acquireContextID() { return id_counter++; }
  int64_t getNextContextID() const { return id_counter; }

 private:
  std::vector<std::pair<BatchDescriptorPtr, jitlib::JITValuePointer>> batch_descriptors_;

 private:
  jitlib::JITFunction* jit_func_;
  int64_t id_counter{0};
};
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CODEGENCONTEXT_H