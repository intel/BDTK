#ifndef JIT_Function_H
#define JIT_Function_H

#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace jitlib {
enum JITFunctionParamAttr : uint64_t {};

struct JITFunctionParam {
  const char* name{""};
  TypeTag type;
  uint64_t attribute;
};

struct JITFunctionDescriptor {
  static constexpr size_t DefaultParamsNum = 8;

  const char* function_name;
  JITFunctionParam ret_type;
  boost::container::small_vector<JITFunctionParam, DefaultParamsNum> params_type;
};

template <typename JITFunctionImpl>
class JITFunction {
 public:
  const JITFunctionDescriptor* getFunctionDescriptor() const { return &descriptor_; }

  void finish() { getImpl()->finishImpl(); }

 protected:
  JITFunction(const JITFunctionDescriptor& descriptor) : descriptor_(descriptor) {}

  JITFunctionImpl* getImpl() { return static_cast<JITFunctionImpl*>(this); }

  JITFunctionDescriptor descriptor_;
};
};  // namespace jitlib

#endif