#ifndef NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
#define NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H

#include "exec/nextgen/context/RuntimeContext.h"
#include "type/data/funcannotations.h"

extern "C" ALWAYS_INLINE int8_t* get_query_context_ptr(int8_t* context, size_t id) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return reinterpret_cast<int8_t*>(context_ptr->getContextItem(id));
}

#endif // NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
