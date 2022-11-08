#ifndef NEXTGEN_OPERATORS_FILTERNODE_H
#define NEXTGEN_OPERATORS_FILTERNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::cider::exec::nextgen {
class FilterNode : public OpNode {
 public:
  FilterNode(const OpNodePtr& input, const std::vector<Analyzer::ExprPtr>& filter)
      : OpNode(input), filter_(filter) {}

  const char* name() const override { return "FilterNode"; }

  std::shared_ptr<Translator> toTranslator() const { return nullptr; }

 private:
  std::vector<std::shared_ptr<Analyzer::Expr>> filter_;
};

class FilterTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(const JITTuple& input) override;
};

}  // namespace cider::cider::exec::nextgen

#endif  // NEXTGEN_OPERATORS_FILTERNODE_H
