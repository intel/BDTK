#include "exec/nextgen/context/Buffer.h"

#include "include/cider/CiderAllocator.h"

namespace cider::exec::nextgen::context {
Buffer::Buffer(const int32_t capacity,
               const CiderAllocatorPtr& allocator,
               const std::function<void(Buffer*)>& initializer)
    : capacity_(capacity)
    , allocator_(allocator)
    , buffer_(allocator_->allocate(capacity_)) {
  initializer(this);
}

Buffer::~Buffer() {
  allocator_->deallocate(buffer_, capacity_);
}

void Buffer::allocateBuffer(int32_t size) {
  if (buffer_) {
    buffer_ = allocator_->reallocate(buffer_, capacity_, size);
  } else {
    buffer_ = allocator_->allocate(size);
  }
  capacity_ = size;
}
};  // namespace cider::exec::nextgen::context