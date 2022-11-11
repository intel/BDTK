#ifndef NEXTGEN_OPERATORS_SOURCENODE_H
#define NEXTGEN_OPERATORS_SOURCENODE_H

#include <vector>

#include "exec/nextgen/operators/OpNode.h"

class InputColDescriptor;
namespace cider::exec::nextgen::operators {

class SourceNode : public OpNode {
 public:
  SourceNode(const ExprPtrVector& col_exprs);

  ExprPtrVector getExprs() override { return input_cols_; }

 private:
  ExprPtrVector input_cols_;
};
}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_OPERATORS_SOURCENODE_H
