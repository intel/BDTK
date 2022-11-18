#include "exec/nextgen/context/CodegenContext.h"

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/JITLib.h"

namespace cider::exec::nextgen::context {
using namespace cider::jitlib;

JITValuePointer CodegenContext::registerBatch(const SQLTypeInfo& type,
                                              const std::string& name) {
  int64_t id = acquireContextID();
  JITValuePointer ret = jit_func_->createLocalJITValue([this, id]() {
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

  batch_descriptors_.emplace_back(std::make_shared<BatchDescriptor>(id, name, type), ret);

  return ret;
}

RuntimeCtxPtr CodegenContext::generateRuntimeCTX(
    const CiderAllocatorPtr& allocator) const {
  auto runtime_ctx = std::make_unique<RuntimeContext>(getNextContextID());

  for (auto& batch_desc : batch_descriptors_) {
    runtime_ctx->addBatch(batch_desc.first);
  }

  runtime_ctx->instantiate(allocator);
  return runtime_ctx;
}
}  // namespace cider::exec::nextgen::context