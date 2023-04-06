
#include "exec/nextgen/context/CiderSet.h"

#include "include/cider/CiderException.h"

namespace cider::exec::nextgen::context {
#define DEF_CIDER_SET_INSERT(type)                                                 \
  void CiderSet::insert(type key_val) {                                            \
    std::string name(typeid(*this).name());                                        \
    CIDER_THROW(CiderRuntimeException, name + " doesn't support insert " + #type); \
  }
DEF_CIDER_SET_INSERT(int8_t)
DEF_CIDER_SET_INSERT(int16_t)
DEF_CIDER_SET_INSERT(int32_t)
DEF_CIDER_SET_INSERT(int64_t)
DEF_CIDER_SET_INSERT(float)
DEF_CIDER_SET_INSERT(double)
DEF_CIDER_SET_INSERT(std::string&)

#define DEF_CIDER_SET_CONTAINS(type)                                               \
  bool CiderSet::contains(type key_val) {                                          \
    std::string name(typeid(*this).name());                                        \
    CIDER_THROW(CiderRuntimeException, name + " doesn't support search " + #type); \
  }
DEF_CIDER_SET_CONTAINS(int8_t)
DEF_CIDER_SET_CONTAINS(int16_t)
DEF_CIDER_SET_CONTAINS(int32_t)
DEF_CIDER_SET_CONTAINS(int64_t)
DEF_CIDER_SET_CONTAINS(float)
DEF_CIDER_SET_CONTAINS(double)
DEF_CIDER_SET_CONTAINS(std::string&)

#undef DEF_CIDER_SET_INSERT
#undef DEF_CIDER_SET_CONTAINS

void CiderInt64Set::insert(int8_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int16_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int32_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int64_t key_val) {
  set_.insert((int64_t)key_val);
}

bool CiderInt64Set::contains(int8_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int16_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int32_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int64_t key_val) {
  return set_.contains(key_val);
}

void CiderDoubleSet::insert(float key_val) {
  set_.insert((double)key_val);
}

void CiderDoubleSet::insert(double key_val) {
  set_.insert(key_val);
}

bool CiderDoubleSet::contains(float key_val) {
  return set_.contains((double)key_val);
}

bool CiderDoubleSet::contains(double key_val) {
  return set_.contains(key_val);
}

void CiderStringSet::insert(std::string& key_val) {
  set_.insert(key_val);
}

bool CiderStringSet::contains(std::string& key_val) {
  return set_.contains(key_val);
}
};  // namespace cider::exec::nextgen::context