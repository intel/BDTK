#include "exec/nextgen/context/CodegenContext.h"

#include "exec/nextgen/jitlib/JITLib.h"

namespace cider::exec::nextgen::context {
using namespace cider::jitlib;

JITValuePointer CodegenContext::registerBatch(const SQLTypeInfo& type,
                                              const std::string& name) {
  int64_t id = acquireContextID();
  auto ret = jit_func_->createLocalJITValue([this, id]() {
    auto index = this->jit_func_->createConstant(JITTypeTag::INT64, id);
    auto pointer = this->jit_func_->emitRuntimeFunctionCall(
        "get_query_context_ptr",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .ret_sub_type = JITTypeTag::INT8,
            .params_vector = {this->jit_func_->getArgument(0).get(), index.get()}});
    return pointer;
  });
  ret->setName(name);

  batch_descriptors_.emplace_back(id, name, type, ret);

  return ret;
}
}  // namespace cider::exec::nextgen::context