#ifndef JITLIB_BASE_JITVALUEOPERATIONS_H
#define JITLIB_BASE_JITVALUEOPERATIONS_H

#include <type_traits>
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

namespace jitlib {
template <typename T>
struct is_jitvalue_convertable {
  using NT = typename std::remove_reference<T>::type;
  static constexpr bool v = std::is_arithmetic_v<T> || std::is_same_v<T, bool>;
};
template <typename T>
inline constexpr bool is_jitvalue_convertable_v = is_jitvalue_convertable<T>::v;

namespace op_utils {
template <typename T>
inline std::any castConstant(JITTypeTag target_type, T value) {
  std::any ret;
  switch (target_type) {
    case BOOL:
      return ret = static_cast<JITTypeTraits<BOOL>::NativeType>(value);
    case INT8:
      return ret = static_cast<JITTypeTraits<INT8>::NativeType>(value);
    case INT16:
      return ret = static_cast<JITTypeTraits<INT16>::NativeType>(value);
    case INT32:
      return ret = static_cast<JITTypeTraits<INT32>::NativeType>(value);
    case INT64:
      return ret = static_cast<JITTypeTraits<INT64>::NativeType>(value);
    case FLOAT:
      return ret = static_cast<JITTypeTraits<FLOAT>::NativeType>(value);
    case DOUBLE:
      return ret = static_cast<JITTypeTraits<DOUBLE>::NativeType>(value);
    default:
      return ret;
  }
}
};  // namespace op_utils

JITValuePointer operator+(JITValue& lh, JITValue& rh) {
  return lh.add(rh);
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator+(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, rh));
  return lh + *rh_pointer;
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator+(T lh, JITValue& rh) {
  return rh + lh;
}

JITValuePointer operator-(JITValue& lh, JITValue& rh) {
  return lh.sub(rh);
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator-(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, rh));
  return lh - *rh_pointer;
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator-(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, lh));
  return *lh_pointer - rh;
}

JITValuePointer operator*(JITValue& lh, JITValue& rh) {
  return lh.mul(rh);
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator*(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, rh));
  return lh * *rh_pointer;
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator*(T lh, JITValue& rh) {
  return rh * lh;
}

JITValuePointer operator/(JITValue& lh, JITValue& rh) {
  return lh.div(rh);
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator/(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, rh));
  return lh / *rh_pointer;
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator/(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, lh));
  return *lh_pointer / rh;
}

JITValuePointer operator%(JITValue& lh, JITValue& rh) {
  return lh.mod(rh);
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator%(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, rh));
  return lh % *rh_pointer;
}

template <typename T, typename = std::enable_if_t<is_jitvalue_convertable_v<T>>>
JITValuePointer operator%(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer =
      parent_func.createConstant(type, op_utils::castConstant(type, lh));
  return *lh_pointer % rh;
}

JITValuePointer operator!(JITValue& value) {
  return value.notOp();
}

};  // namespace jitlib

#endif  // JITLIB_BASE_JITVALUEOPERATIONS_H
