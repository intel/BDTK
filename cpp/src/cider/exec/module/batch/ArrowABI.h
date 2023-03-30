/*
 * Copyright(c) 2022-2023 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#include <cstdint>
#include <sstream>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format{nullptr};
  const char* name{nullptr};
  const char* metadata{nullptr};
  int64_t flags{0};
  int64_t n_children{0};
  struct ArrowSchema** children{nullptr};
  struct ArrowSchema* dictionary{nullptr};

  // Release callback
  void (*release)(struct ArrowSchema*) = nullptr;
  // Opaque producer-specific data
  void* private_data{nullptr};
};

struct ArrowArray {
  // Array data description
  int64_t length{0};
  int64_t null_count{0};
  int64_t offset{0};
  int64_t n_buffers{0};
  int64_t n_children{0};
  const void** buffers{nullptr};
  struct ArrowArray** children{nullptr};
  struct ArrowArray* dictionary{nullptr};

  // Release callback
  void (*release)(struct ArrowArray*) = nullptr;
  // Opaque producer-specific data
  void* private_data{nullptr};

  std::string toString(std::string indent = "") const {
    std::stringstream ss;
    ss << indent << "{" << '\n';
    indent.push_back(' ');
    indent.push_back(' ');
    ss << indent << "length = " << length << '\n';
    ss << indent << "null_count = " << null_count << '\n';
    ss << indent << "offset = " << offset << '\n';
    ss << indent << "n_buffers = " << n_buffers << '\n';
    for (int64_t i = 0; i < n_buffers; ++i) {
      double* p = (double*)buffers[i];
      if (p == nullptr) {
        ss << indent << "buffers[" << i << "] is nullptr\n";
      } else {
        ss << indent << "buffers[" << i << "] = " << *p << '\n';
      }
    }
    ss << indent << "n_children = " << n_children << '\n';
    for (int64_t i = 0; i < n_children; ++i) {
      struct ArrowArray* paa = children[i];
      ss << paa->toString(indent);
    }
    if (dictionary != nullptr) {
      ss << indent << "dictionary field is not null\n";
      ss << dictionary->toString(indent);
    }
    indent.pop_back();
    indent.pop_back();
    ss << indent << "}\n";
  }
};

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.  The returned pointer is only valid until
  // the next operation on this stream (including release).
  // If unavailable, NULL is returned.
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifdef __cplusplus
}
#endif

#endif
