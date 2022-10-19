#ifndef JIT_CONTROL_FLOW_H
#define JIT_CONTROL_FLOW_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/jitlib/base/Values.h"

namespace jitlib {
template <TypeTag, typename FunctionImpl>
class Ret;

template <TypeTag Type = VOID, typename FunctionImpl>
inline void createRet(FunctionImpl& function) {
  Ret<Type, FunctionImpl> ret(function);
}

template <TypeTag Type, typename FunctionImpl>
inline void createRet(FunctionImpl& function, Value<Type, FunctionImpl>& value) {
  Ret<Type, FunctionImpl> ret(function, value);
}
};  // namespace jitlib

#endif