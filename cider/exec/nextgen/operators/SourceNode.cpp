#include "exec/nextgen/operators/SourceNode.h"

#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
SourceNode::SourceNode(const ExprPtrVector& input_cols)
    : OpNode("SourceNode"), input_cols_(input_cols) {}
}  // namespace cider::exec::nextgen::operators