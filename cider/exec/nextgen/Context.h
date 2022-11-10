#ifndef EXEC_NEXTGEN_CONTEXT_H
#define EXEC_NEXTGEN_CONTEXT_H

#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/operators/expr.h"

namespace cider::exec::nextgen {
using namespace cider::jitlib;

class Context {
 public:
  Context(JITFunction* func_) : query_func_(func_) {}
  JITFunction* query_func_;
  std::vector<cider::jitlib::JITExprValue*> expr_outs_;
};
}  // namespace cider::exec::nextgen

#endif // EXEC_NEXTGEN_CONTEXT_H
