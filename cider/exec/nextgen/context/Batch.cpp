#include "exec/nextgen/context/Batch.h"

#include <functional>

#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"

namespace cider::exec::nextgen::context {

void Batch::reset(const SQLTypeInfo& type, const CiderAllocatorPtr& allocator) {
  release();

  auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
  schema_ = *schema;
  CiderBatchUtils::freeArrowSchema(schema);

  std::function<void(ArrowSchema*, ArrowArray*)> builder =
      [&allocator, &builder](ArrowSchema* schema, ArrowArray* array) {
        array->length = 0;
        array->null_count = 0;
        array->offset = 0;

        array->n_buffers = CiderBatchUtils::getBufferNum(schema);
        array->n_children = schema->n_children;
        CiderArrowArrayBufferHolder* root_holder = new CiderArrowArrayBufferHolder(
            array->n_buffers, schema->n_children, allocator, schema->dictionary);
        array->buffers = root_holder->getBufferPtrs();
        array->children = root_holder->getChildrenPtrs();
        array->dictionary = root_holder->getDictPtr();
        array->private_data = root_holder;
        array->release = CiderBatchUtils::ciderArrowArrayReleaser;

        for (size_t i = 0; i < schema->n_children; ++i) {
          builder(schema->children[i], array->children[i]);
        }
      };

  builder(&schema_, &array_);
}
}  // namespace cider::exec::nextgen::context