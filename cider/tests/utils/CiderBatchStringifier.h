#ifndef CIDER_CIDERBATCHSTRINGIFIER_H
#define CIDER_CIDERBATCHSTRINGIFIER_H

#include "cider/CiderBatch.h"

class CiderBatchStringifier {
 public:
  virtual std::string stringifyValueAt(CiderBatch* batch, int row_index) = 0;
};

class StructBatchStringifier : public CiderBatchStringifier {
 private:
  // stores stringifiers to stringify children batches
  std::vector<std::unique_ptr<CiderBatchStringifier>> child_stringifiers_;

 public:
  StructBatchStringifier(CiderBatch* batch);
  virtual std::string stringifyValueAt(CiderBatch* batch, int row_index) override;
};

template <typename T>
class ScalarBatchStringifier : public CiderBatchStringifier {
 public:
  virtual std::string stringifyValueAt(CiderBatch* batch, int row_index) override;
};

#endif
