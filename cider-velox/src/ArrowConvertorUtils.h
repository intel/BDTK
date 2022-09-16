/*
 * Copyright (c) 2022 Intel Corporation.
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

#pragma once

#include <cfloat>
#include <cstdint>
#include <string>
#include "CiderNullValues.h"
#include "cider/CiderInterface.h"
#include "velox/vector/arrow/Abi.h"

namespace {
enum CIDER_DIMEN { SECOND = 0, MILLISECOND = 3, MICROSECOND = 6, NANOSECOND = 9 };
}

namespace facebook::velox::plugin {

/// Convert ArrowArray and ArrowSchema to cider data buffer.
/// As ArrowArray's internal buffer is immutable, there will be memory copy when
/// calling such function. Current supported data types are basic types
/// including integral and floating points.
int8_t* convertToCider(const ArrowSchema& arrowSchema,
                       const ArrowArray& arrowArray,
                       int num_rows,
                       std::shared_ptr<CiderAllocator> allocator);

/// Convert cider data buffer to ArrowArray and ArrowSchema.
/// As ArrowArray has null buffer for null data while cider only has one
/// buffer and usea MIN_VAL to represent null, extra memory allocation needed
/// for creating null buffer. Current supported data types are basic types
/// including integral and floating points.
void convertToArrow(ArrowArray& arrowArray,
                    ArrowSchema& arrowSchema,
                    const int8_t* data_buffer,
                    ::substrait::Type col_type,
                    int num_rows,
                    std::shared_ptr<CiderAllocator> allocator = nullptr);

}  // namespace facebook::velox::plugin
