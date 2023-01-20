#ifndef NEXTGEN_OPERATORS_VECTORIZEDPROJECTNODE_H
#define NEXTGEN_OPERATORS_VECTORIZEDPROJECTNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class VectorizedProjectNode : public OpNode {
 public:
  explicit VectorizedProjectNode(ExprPtrVector&& output_exprs)
      : OpNode("VectorizedProjectNode",
               std::move(output_exprs),
               JITExprValueType::BATCH) {}

  explicit VectorizedProjectNode(const ExprPtrVector& output_exprs)
      : OpNode("VectorizedProjectNode", output_exprs, JITExprValueType::BATCH) {}

  TranslatorPtr toTranslator(const TranslatorPtr& successor = nullptr) override;
};

class VectorizedProjectTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};
}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_OPERATORS_VECTORIZEDPROJECTNODE_H
