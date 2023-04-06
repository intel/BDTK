#ifndef NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H
#define NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H

#include <functional>
#include <memory>

#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/Buffer.h"
#include "exec/nextgen/context/CiderSet.h"

namespace cider::exec::processor {
class JoinHashTable;
};

namespace cider::exec::nextgen::context {

struct AggExprsInfo;

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
