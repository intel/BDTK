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

#ifndef CIDER_CIDERBATCHUTILS_H
#define CIDER_CIDERBATCHUTILS_H

#include "cider/CiderAllocator.h"
#include "type/data/sqltypes.h"

class CiderBatch;

struct ArrowSchema;
struct ArrowArray;

namespace CiderBatchUtils {

void ciderArrowSchemaReleaser(ArrowSchema* schema);

void ciderArrowArrayReleaser(ArrowArray* schema);

void freeArrowArray(ArrowArray* ptr);

void freeArrowSchema(ArrowSchema* ptr);

ArrowArray* allocateArrowArray();

ArrowSchema* allocateArrowSchema();

int64_t getBufferNum(const ArrowSchema* schema);

SQLTypes convertArrowTypeToCiderType(const char* format);

std::unique_ptr<CiderBatch> createCiderBatch(ArrowSchema* schema,
                                             std::shared_ptr<CiderAllocator> allocator,
                                             ArrowArray* array = nullptr);

const char* convertCiderTypeToArrowType(SQLTypes type);

ArrowSchema* convertCiderTypeInfoToArrowSchema(const SQLTypeInfo& sql_info);

};  // namespace CiderBatchUtils

#endif
