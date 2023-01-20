#ifndef NEXTGEN_UTILS_EXPRUTILS_H
#define NEXTGEN_UTILS_EXPRUTILS_H

#include <cstddef>
#include <unordered_set>

#include "exec/nextgen/utils/FunctorUtils.h"
#include "type/plan/Analyzer.h"
#include "type/plan/Expr.h"

namespace cider::exec::nextgen::operators {
using ExprPtr = std::shared_ptr<Analyzer::Expr>;
using ExprPtrVector = std::vector<ExprPtr>;
}  // namespace cider::exec::nextgen::operators

namespace cider::exec::nextgen::utils {

// Collect all leaf ColumnVar Exprs in a expr tree.
inline operators::ExprPtrVector collectColumnVars(const operators::ExprPtrVector& exprs) {
  operators::ExprPtrVector outputs;
  outputs.reserve(8);
  std::unordered_set<Analyzer::Expr*> expr_record;

  RecursiveFunctor traverser{
      [&outputs, &expr_record](auto&& traverser, const operators::ExprPtr& expr) {
        CHECK(expr);
        if (dynamic_cast<Analyzer::ColumnVar*>(expr.get())) {
          if (expr_record.find(expr.get()) == expr_record.end()) {
            expr_record.insert(expr.get());
            outputs.emplace_back(expr);
          }
          return;
        }

        auto&& children = expr->get_children_reference();
        for (auto&& child : children) {
          traverser(*child);
        }
        return;
      }};
  std::for_each(exprs.begin(), exprs.end(), traverser);

  return outputs;
}

// Check whether a is a subset of b. Both inputs should be sorted.
inline bool isSubsetExprVector(const operators::ExprPtrVector& a,
                               const operators::ExprPtrVector& b) {
  if (a.size() <= b.size()) {
    for (size_t i = 0; i < a.size(); ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }
  return false;
}
}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_EXPRUTILS_H
