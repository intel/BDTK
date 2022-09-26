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

#define CIDERBATCH_WITH_ARROW

#include "cider/batch/CiderBatchUtils.h"
#include "ArrowABI.h"
#include "CiderArrowBufferHolder.h"

#include "include/cider/CiderException.h"
#include "include/cider/batch/CiderBatch.h"
#include "include/cider/batch/CiderBatchUtils.h"
#include "include/cider/batch/ScalarBatch.h"
#include "include/cider/batch/StructBatch.h"

namespace CiderBatchUtils {
void freeArrowArray(ArrowArray* ptr) {
  delete ptr;
}

void freeArrowSchema(ArrowSchema* ptr) {
  delete ptr;
}

ArrowArray* allocateArrowArray() {
  ArrowArray* ptr = new ArrowArray;
  *ptr = ArrowArray{.length = 0,
                    .null_count = 0,
                    .offset = 0,
                    .n_buffers = 0,
                    .n_children = 0,
                    .buffers = nullptr,
                    .children = nullptr,
                    .dictionary = nullptr,
                    .release = nullptr,
                    .private_data = nullptr};
  return ptr;
}

ArrowSchema* allocateArrowSchema() {
  ArrowSchema* ptr = new ArrowSchema;
  *ptr = ArrowSchema{.format = nullptr,
                     .name = nullptr,
                     .metadata = nullptr,
                     .flags = 0,
                     .n_children = 0,
                     .children = nullptr,
                     .dictionary = nullptr,
                     .release = nullptr,
                     .private_data = nullptr};
  return ptr;
}

ArrowSchema* allocateArrowSchema(const ArrowSchema& schema) {
  ArrowSchema* ptr = new ArrowSchema(schema);
  return ptr;
}

void ciderArrowSchemaReleaser(ArrowSchema* schema) {
  if (!schema || !schema->release) {
    return;
  }

  for (size_t i = 0; i < schema->n_children; ++i) {
    ArrowSchema* child = schema->children[i];
    if (child && child->release) {
      child->release(child);
      CHECK_EQ(child->release, nullptr);
    }
  }

  ArrowSchema* dict = schema->dictionary;
  if (dict && dict->release) {
    dict->release(dict);
    CHECK_EQ(dict->release, nullptr);
  }

  CHECK_NE(schema->private_data, nullptr);
  auto holder = reinterpret_cast<CiderArrowSchemaBufferHolder*>(schema->private_data);
  delete holder;

  schema->release = nullptr;
  schema->private_data = nullptr;
}

void ciderArrowArrayReleaser(ArrowArray* array) {
  if (!array || !array->release) {
    return;
  }

  for (size_t i = 0; i < array->n_children; ++i) {
    ArrowArray* child = array->children[i];
    if (child && child->release) {
      child->release(child);
      CHECK_EQ(child->release, nullptr);
    }
  }

  ArrowArray* dict = array->dictionary;
  if (dict && dict->release) {
    dict->release(dict);
    CHECK_EQ(dict->release, nullptr);
  }

  CHECK_NE(array->private_data, nullptr);
  auto holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);
  delete holder;

  array->release = nullptr;
  array->private_data = nullptr;
}

int64_t getBufferNum(const ArrowSchema* schema) {
  CHECK(schema);
  const char* type = schema->format;
  switch (type[0]) {
    // Scalar Types
    case 'b':
    case 'c':
    case 's':
    case 'i':
    case 'l':
    case 'f':
    case 'g':
      return 2;
    case '+':
      // Complex Types
      switch (type[1]) {
        // Struct Type
        case 's':
          return 1;
      }
    default:
      CIDER_THROW(CiderException,
                  std::string("Unsupported data type to CiderBatch: ") + type);
  }
}

SQLTypes convertArrowTypeToCiderType(const char* format) {
  CHECK(format);
  switch (format[0]) {
    // Scalar Types
    case 'b':
      return kBOOLEAN;
    case 'c':
      return kTINYINT;
    case 's':
      return kSMALLINT;
    case 'i':
      return kINT;
    case 'l':
      return kBIGINT;
    case 'f':
      return kFLOAT;
    case 'g':
      return kDOUBLE;
    case '+':
      // Complex Types
      switch (format[1]) {
        // Struct Type
        case 's':
          return kSTRUCT;
      }
    default:
      CIDER_THROW(CiderCompileException,
                  std::string("Unsupported data type to CiderBatch: ") + format);
  }
}

const char* convertCiderTypeToArrowType(SQLTypes type) {
  switch (type) {
    case kBOOLEAN:
      return "b";
    case kTINYINT:
      return "c";
    case kSMALLINT:
      return "s";
    case kINT:
      return "i";
    case kBIGINT:
      return "l";
    case kFLOAT:
      return "f";
    case kDOUBLE:
      return "g";
    case kSTRUCT:
      return "+s";
    default:
      CIDER_THROW(CiderCompileException,
                  std::string("Unsupported to convert type ") + toString(type) +
                      "to Arrow type.");
  }
}

ArrowSchema* convertCiderTypeInfoToArrowSchema(const SQLTypeInfo& sql_info) {
  ArrowSchema* root_schema = allocateArrowSchema();

  std::function<void(ArrowSchema*, const SQLTypeInfo&)> build_function =
      [&build_function](ArrowSchema* schema, const SQLTypeInfo& info) {
        CHECK(schema);
        schema->format = convertCiderTypeToArrowType(info.get_type());
        schema->n_children = info.getChildrenNum();

        CiderArrowSchemaBufferHolder* holder =
            new CiderArrowSchemaBufferHolder(info.getChildrenNum(),
                                             false);  // TODO: Dictionary support is TBD;
        schema->children = holder->getChildrenPtrs();
        schema->dictionary = holder->getDictPtr();
        schema->release = ciderArrowSchemaReleaser;
        schema->private_data = holder;

        for (size_t i = 0; i < schema->n_children; ++i) {
          build_function(schema->children[i], info.getChildAt(i));
        }
      };

  build_function(root_schema, sql_info);

  return root_schema;
}

const char* convertSubstraitTypeToArrowType(const substrait::Type& type) {
  using namespace substrait;
  switch (type.kind_case()) {
    case Type::kBool:
      return "b";
    case Type::kI8:
      return "c";
    case Type::kI16:
      return "s";
    case Type::kI32:
      return "i";
    case Type::kI64:
      return "l";
    case Type::kFp32:
      return "f";
    case Type::kFp64:
      return "g";
    case Type::kStruct:
      return "+s";
    default:
      CIDER_THROW(CiderRuntimeException,
                  std::string("Unsupported to convert type ") + type.GetTypeName() +
                      "to Arrow type.");
  }
}

ArrowSchema* convertCiderTableSchemaToArrowSchema(const CiderTableSchema& table) {
  auto&& children = table.getColumnTypes();

  ArrowSchema* root_schema = allocateArrowSchema();
  root_schema->format = "+s";
  root_schema->n_children = children.size();
  CiderArrowSchemaBufferHolder* holder =
      new CiderArrowSchemaBufferHolder(children.size(), false);
  root_schema->children = holder->getChildrenPtrs();
  root_schema->dictionary = holder->getDictPtr();
  root_schema->release = ciderArrowSchemaReleaser;
  root_schema->private_data = holder;

  for (size_t i = 0; i < children.size(); ++i) {
    ArrowSchema* schema = root_schema->children[i];
    schema->format = convertSubstraitTypeToArrowType(children[i]);
    schema->n_children = 0;

    CiderArrowSchemaBufferHolder* holder = new CiderArrowSchemaBufferHolder(0, false);
    schema->children = holder->getChildrenPtrs();
    schema->dictionary = holder->getDictPtr();
    schema->release = ciderArrowSchemaReleaser;
    schema->private_data = holder;
  }

  return root_schema;
}

std::unique_ptr<CiderBatch> createCiderBatch(std::shared_ptr<CiderAllocator> allocator,
                                             ArrowSchema* schema,
                                             ArrowArray* array) {
  CHECK(schema);
  CHECK(schema->release);

  const char* format = schema->format;
  switch (format[0]) {
    // Scalar Types
    case 'b':
      return ScalarBatch<bool>::Create(schema, allocator, array);
    case 'c':
      return ScalarBatch<int8_t>::Create(schema, allocator, array);
    case 's':
      return ScalarBatch<int16_t>::Create(schema, allocator, array);
    case 'i':
      return ScalarBatch<int32_t>::Create(schema, allocator, array);
    case 'l':
      return ScalarBatch<int64_t>::Create(schema, allocator, array);
    case 'f':
      return ScalarBatch<float>::Create(schema, allocator, array);
    case 'g':
      return ScalarBatch<double>::Create(schema, allocator, array);
    case '+':
      // Complex Types
      switch (format[1]) {
        // Struct Type
        case 's':
          return StructBatch::Create(schema, allocator, array);
      }
    default:
      CIDER_THROW(CiderCompileException,
                  std::string("Unsupported data type to create CiderBatch: ") + format);
  }
}
}  // namespace CiderBatchUtils
