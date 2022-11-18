#ifndef NEXTGEN_CONTEXT_BATCH_H
#define NEXTGEN_CONTEXT_BATCH_H


#include "exec/module/batch/ArrowABI.h"
#include "include/cider/batch/CiderBatchUtils.h"

namespace cider::exec::nextgen::context {
class Batch {
 public:
  Batch(const SQLTypeInfo& type, const CiderAllocatorPtr& allocator) {
    schema_.release = nullptr;
    array_.release = nullptr;
    reset(type, allocator);
  }

  Batch(ArrowSchema& schema, ArrowArray& array) : schema_(schema), array_(array) {}

  ~Batch() { release(); }

  void reset(const SQLTypeInfo& type, const CiderAllocatorPtr& allocator);

  void move(ArrowSchema& schema, ArrowArray& array) {
    schema = schema_;
    array = array_;

    schema_.release = nullptr;
    array_.release = nullptr;
  }

  void release() {
    if (schema_.release) {
      schema_.release(&schema_);
    }
    schema_.release = nullptr;

    if (array_.release) {
      array_.release(&array_);
    }
    array_.release = nullptr;
  }

  bool isMoved() const { return schema_.release; }

 private:
  ArrowSchema schema_;
  ArrowArray array_;
};

using BatchPtr = std::unique_ptr<Batch>;

namespace utils {
// void resizeBatch(Batch* batch, size_t size);
}
}  // namespace cider::exec::nextgen::context
#endif // NEXTGEN_CONTEXT_BATCH_H
