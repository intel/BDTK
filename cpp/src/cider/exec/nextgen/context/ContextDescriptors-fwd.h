#ifndef NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H
#define NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H

#include <functional>
#include <memory>

namespace cider::exec::processor {
class JoinHashTable;
};

namespace cider::exec::nextgen::context {

struct AggExprsInfo;
class CiderSet;
class Buffer;
class Batch;

using BatchPtr = std::unique_ptr<Batch>;
using BufferPtr = std::unique_ptr<Buffer>;
using CiderSetPtr = std::unique_ptr<CiderSet>;
using BufferInitializer = std::function<void(Buffer*)>;
using BatchSharedPtr = std::shared_ptr<Batch>;
using TrimCharMapsPtr = std::shared_ptr<std::vector<std::vector<int8_t>>>;

struct BatchDescriptor;
struct BufferDescriptor;
struct HashTableDescriptor;
struct CrossJoinBuildTableDescriptor;
struct CiderSetDescriptor;

using BatchDescriptorPtr = std::shared_ptr<BatchDescriptor>;
using BufferDescriptorPtr = std::shared_ptr<BufferDescriptor>;
using HashTableDescriptorPtr = std::shared_ptr<HashTableDescriptor>;
using BuildTableDescriptorPtr = std::shared_ptr<CrossJoinBuildTableDescriptor>;
using CiderSetDescriptorPtr = std::shared_ptr<CiderSetDescriptor>;

};  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H
