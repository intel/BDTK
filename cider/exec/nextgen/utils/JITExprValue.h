#ifndef NEXTGEN_UTILS_JITEXPRVALUE_H
#define NEXTGEN_UTILS_JITEXPRVALUE_H

#include "exec/nextgen/jitlib/base/JITValue.h"

namespace cider::exec::nextgen::utils {
enum class JITExprValueType { ROW, BATCH };

class JITExprValue {
  template <typename T>
  using IsJITValuePointer = typename std::enable_if_t<
      std::is_same_v<typename std::remove_reference<T>::type, jitlib::JITValuePointer>,
      bool>;

 public:
  JITExprValue(size_t size = 0, JITExprValueType type = JITExprValueType::ROW)
      : ptrs_(0), value_type_(type) {
    ptrs_.reserve(size);
  }

  // for {JITValuePointer, ...}
  template <typename... T>
  JITExprValue(JITExprValueType type, T&&... ptrs) {
    value_type_ = type;
    append(std::forward<T>(ptrs)...);
  }

  template <typename... T>
  JITExprValue& append(T&&... values) {
    ptrs_.reserve(sizeof...(values));
    (ptrs_.emplace_back(jitlib::JITValuePointer(values)), ...);
    return *this;
  }

  void resize(size_t attributes_num) { ptrs_.resize(attributes_num); }

  size_t size() { return ptrs_.size(); }

  void clear() { resize(0); }

  operator bool() { return size(); }

  jitlib::JITValuePointer& operator[](size_t index) { return ptrs_[index]; }

 private:
  std::vector<cider::jitlib::JITValuePointer> ptrs_{};
  JITExprValueType value_type_{JITExprValueType::ROW};
};

class JITExprValueAdaptor {
 public:
  JITExprValueAdaptor(JITExprValue& values) : values_(values) {}

  jitlib::JITValuePointer& getNull() { return values_[0]; }

  void setNull(jitlib::JITValuePointer& rh) { values_[0].replace(rh); }

 protected:
  JITExprValue& values_;
};

class FixSizeJITExprValue : public JITExprValueAdaptor {
 public:
  FixSizeJITExprValue(JITExprValue& values) : JITExprValueAdaptor(values) {
    values_.resize(2);
  }

  jitlib::JITValuePointer& getValue() { return values_[1]; }

  void setValue(jitlib::JITValuePointer& rh) { values_[1].replace(rh); }
};
}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_JITEXPRVALUE_H
