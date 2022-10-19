#ifndef JITLIB_VALUE_H
#define JITLIB_VALUE_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace jitlib {

template <TypeTag, typename FunctionImpl>
class Value;

template <typename ValueImpl, TypeTag Type, typename FunctionImpl>
class BasicValue {
 public:
  using NativeType = typename TypeTraits<Type>::NativeType;

  static ValueImpl createVariable(FunctionImpl& function,
                                  const char* name,
                                  NativeType init) {
    return ValueImpl::createVariableImpl(function, name, init);
  }
};

template <TypeTag Type,
          typename FunctionImpl,
          typename NativeType = typename TypeTraits<Type>::NativeType>
inline Value<Type, FunctionImpl> createVariable(FunctionImpl& function,
                                                const char* name,
                                                NativeType init) {
  return Value<Type, FunctionImpl>::createVariable(function, name, init);
}

};  // namespace jitlib

#endif