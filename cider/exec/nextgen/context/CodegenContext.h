#ifndef NEXTGEN_CONTEXT_CODEGENCONTEXT_H
#define NEXTGEN_CONTEXT_CODEGENCONTEXT_H

#include <cstdint>
#include <string>

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "type/data/sqltypes.h"

namespace cider::jitlib {
class JITFunction;
}

namespace cider::exec::nextgen::context {
class CodegenContext {
 public:
  CodegenContext(jitlib::JITFunction* jit_func) : jit_func_(jit_func) {}

 public:
  jitlib::JITValuePointer registerBatch(const SQLTypeInfo& type,
                                        const std::string& name = "");

  // TBD: HashTable (GroupBy, Join), other objects registration.

  RuntimeCtxPtr generateRuntimeCTX() const;

 private:
  int64_t acquireContextID() { return id_counter++; }

 private:
  struct BatchDescriptor {
    int64_t ctx_id;
    std::string name;
    SQLTypeInfo type;
    jitlib::JITValuePointer jit_value;

    BatchDescriptor(int64_t id,
                    const std::string& n,
                    const SQLTypeInfo& t,
                    const jitlib::JITValuePointer& j)
        : ctx_id(id), name(n), type(t), jit_value(j) {}
  };

  std::vector<BatchDescriptor> batch_descriptors_;

 private:
  jitlib::JITFunction* jit_func_;
  int64_t id_counter{0};
};
}  // namespace cider::exec::nextgen::context

#endif // NEXTGEN_CONTEXT_CODEGENCONTEXT_H
