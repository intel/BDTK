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
#include "substrait/type.pb.h"
#include "type/data/sqltypes.h"

class CiderBatch;
class CiderTableSchema;

struct ArrowSchema;
struct ArrowArray;

namespace CiderBatchUtils {

void ciderArrowSchemaReleaser(ArrowSchema* schema);

void ciderArrowArrayReleaser(ArrowArray* array);

void freeArrowArray(ArrowArray* ptr);

void freeArrowSchema(ArrowSchema* ptr);

ArrowArray* allocateArrowArray();

ArrowSchema* allocateArrowSchema();

ArrowSchema* allocateArrowSchema(const ArrowSchema& schema);

int64_t getBufferNum(const ArrowSchema* schema);

SQLTypes convertArrowTypeToCiderType(const char* format);

std::unique_ptr<CiderBatch> createCiderBatch(std::shared_ptr<CiderAllocator> allocator,
                                             ArrowSchema* schema,
                                             ArrowArray* array = nullptr);

const char* convertCiderTypeToArrowType(SQLTypes type);

ArrowSchema* convertCiderTypeInfoToArrowSchema(const SQLTypeInfo& sql_info);

const char* convertSubstraitTypeToArrowType(const substrait::Type& type);

ArrowSchema* convertCiderTableSchemaToArrowSchema(const CiderTableSchema& table);

};  // namespace CiderBatchUtils

#endif
