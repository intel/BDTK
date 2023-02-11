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

#include "DuckDbArrowAdaptor.h"
#include <string>
#include "cider/CiderException.h"
// function interfaces from duckdb are different when enabling/disabling velox
// for interface stability, we maintain a copy of relevant codes in duckdb
// most of the following codes are copied from duckdb sources

void DuckDbArrowSchemaAdaptor::duckdbResultSchemaToArrowSchema(
    ArrowSchema* out_schema,
    std::vector<::duckdb::LogicalType>& types,
    std::vector<std::string>& names) {
  // CHECK(out_schema);
  // CHECK_EQ(types.size(), names.size());
  idx_t column_count = types.size();
  // Allocate as unique_ptr first to cleanup properly on error
  auto root_holder = std::make_unique<DuckDBArrowSchemaHolder>();

  // Allocate the children
  root_holder->children.resize(column_count);
  root_holder->children_ptrs.resize(column_count, nullptr);
  for (size_t i = 0; i < column_count; ++i) {
    root_holder->children_ptrs[i] = &root_holder->children[i];
  }
  out_schema->children = root_holder->children_ptrs.data();
  out_schema->n_children = column_count;

  // Store the schema
  out_schema->format = "+s";  // struct apparently
  out_schema->flags = 0;
  out_schema->metadata = nullptr;
  out_schema->name = "duckdb_query_result";
  out_schema->dictionary = nullptr;

  // Configure all child schemas
  for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
    auto& child = root_holder->children[col_idx];
    InitializeChild(child, names[col_idx]);
    SetArrowFormat(*root_holder, child, types[col_idx]);
  }

  // Release ownership to caller
  out_schema->private_data = root_holder.release();
  out_schema->release = ReleaseDuckDBArrowSchema;
}

void DuckDbArrowSchemaAdaptor::ReleaseDuckDBArrowSchema(ArrowSchema* schema) {
  if (!schema || !schema->release) {
    return;
  }
  schema->release = nullptr;
  auto holder = static_cast<DuckDBArrowSchemaHolder*>(schema->private_data);
  delete holder;
}

void DuckDbArrowSchemaAdaptor::SetArrowFormat(DuckDBArrowSchemaHolder& root_holder,
                                              ArrowSchema& child,
                                              const ::duckdb::LogicalType& type) {
  switch (type.id()) {
    case ::duckdb::LogicalTypeId::BOOLEAN:
      child.format = "b";
      break;
    case ::duckdb::LogicalTypeId::TINYINT:
      child.format = "c";
      break;
    case ::duckdb::LogicalTypeId::SMALLINT:
      child.format = "s";
      break;
    case ::duckdb::LogicalTypeId::INTEGER:
      child.format = "i";
      break;
    case ::duckdb::LogicalTypeId::BIGINT:
      child.format = "l";
      break;
    case ::duckdb::LogicalTypeId::UTINYINT:
      child.format = "C";
      break;
    case ::duckdb::LogicalTypeId::USMALLINT:
      child.format = "S";
      break;
    case ::duckdb::LogicalTypeId::UINTEGER:
      child.format = "I";
      break;
    case ::duckdb::LogicalTypeId::UBIGINT:
      child.format = "L";
      break;
    case ::duckdb::LogicalTypeId::FLOAT:
      child.format = "f";
      break;
    case ::duckdb::LogicalTypeId::HUGEINT:
      child.format = "d:38,0";
      break;
    case ::duckdb::LogicalTypeId::DOUBLE:
      child.format = "g";
      break;
    case ::duckdb::LogicalTypeId::UUID:
    case ::duckdb::LogicalTypeId::JSON:
    case ::duckdb::LogicalTypeId::VARCHAR:
      child.format = "u";
      break;
    case ::duckdb::LogicalTypeId::DATE:
      child.format = "tdD";
      break;
    case ::duckdb::LogicalTypeId::TIME:
    case ::duckdb::LogicalTypeId::TIME_TZ:
      child.format = "ttu";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP:
    case ::duckdb::LogicalTypeId::TIMESTAMP_TZ:
      child.format = "tsu:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_SEC:
      child.format = "tss:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_NS:
      child.format = "tsn:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_MS:
      child.format = "tsm:";
      break;
    case ::duckdb::LogicalTypeId::INTERVAL:
      child.format = "tDm";
      break;
    case ::duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width, scale;
      type.GetDecimalProperties(width, scale);
      // std::to_string(uint8_t) may result in converting ints to ASCII chars
      // instead of string of ints (e.g. 97 -> 'a' instead of "97")
      std::string format = "d:" + std::to_string(int(width)) + "," + std::to_string(int(scale));
      std::unique_ptr<char[]> format_ptr =
          std::unique_ptr<char[]>(new char[format.size() + 1]);
      for (size_t i = 0; i < format.size(); i++) {
        format_ptr[i] = format[i];
      }
      format_ptr[format.size()] = '\0';
      root_holder.owned_type_names.push_back(move(format_ptr));
      child.format = root_holder.owned_type_names.back().get();
      break;
    }
    case ::duckdb::LogicalTypeId::SQLNULL: {
      child.format = "n";
      break;
    }
    case ::duckdb::LogicalTypeId::BLOB: {
      child.format = "z";
      break;
    }
    case ::duckdb::LogicalTypeId::LIST: {
      child.format = "+l";
      child.n_children = 1;
      root_holder.nested_children.emplace_back();
      root_holder.nested_children.back().resize(1);
      root_holder.nested_children_ptr.emplace_back();
      root_holder.nested_children_ptr.back().push_back(
          &root_holder.nested_children.back()[0]);
      InitializeChild(root_holder.nested_children.back()[0]);
      child.children = &root_holder.nested_children_ptr.back()[0];
      child.children[0]->name = "l";
      SetArrowFormat(
          root_holder, **child.children, ::duckdb::ListType::GetChildType(type));
      break;
    }
    case ::duckdb::LogicalTypeId::STRUCT: {
      child.format = "+s";
      auto& child_types = ::duckdb::StructType::GetChildTypes(type);
      child.n_children = child_types.size();
      root_holder.nested_children.emplace_back();
      root_holder.nested_children.back().resize(child_types.size());
      root_holder.nested_children_ptr.emplace_back();
      root_holder.nested_children_ptr.back().resize(child_types.size());
      for (idx_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
        root_holder.nested_children_ptr.back()[type_idx] =
            &root_holder.nested_children.back()[type_idx];
      }
      child.children = &root_holder.nested_children_ptr.back()[0];
      for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
        InitializeChild(*child.children[type_idx]);

        auto& struct_col_name = child_types[type_idx].first;
        std::unique_ptr<char[]> name_ptr =
            std::unique_ptr<char[]>(new char[struct_col_name.size() + 1]);
        for (size_t i = 0; i < struct_col_name.size(); i++) {
          name_ptr[i] = struct_col_name[i];
        }
        name_ptr[struct_col_name.size()] = '\0';
        root_holder.owned_type_names.push_back(move(name_ptr));

        child.children[type_idx]->name = root_holder.owned_type_names.back().get();
        SetArrowFormat(
            root_holder, *child.children[type_idx], child_types[type_idx].second);
      }
      break;
    }
    case ::duckdb::LogicalTypeId::MAP: {
      SetArrowMapFormat(root_holder, child, type);
      break;
    }
    case ::duckdb::LogicalTypeId::ENUM:
      /// NOTE: (YBRua) GetPhysicalType() implementation of DuckDB is again different
      /// it accepts different types of parameter when enabling/disabling velox
      /// since we are not daling with ENUM types for the moment
      /// we skip the following code to make the code compilable in both scenarios
      // switch (::duckdb::EnumType::GetPhysicalType(::duckdb::EnumType::GetSize(type)))
      // {
      //   case ::duckdb::PhysicalType::UINT8:
      //     child.format = "C";
      //     break;
      //   case ::duckdb::PhysicalType::UINT16:
      //     child.format = "S";
      //     break;
      //   case ::duckdb::PhysicalType::UINT32:
      //     child.format = "I";
      //     break;
      //   default:
      //     CIDER_THROW(CiderCompileException,
      //                 "Unsupported Enum Internal Type for DuckDB");
      // }
      // root_holder.nested_children.emplace_back();
      // root_holder.nested_children.back().resize(1);
      // root_holder.nested_children_ptr.emplace_back();
      // root_holder.nested_children_ptr.back().push_back(
      //     &root_holder.nested_children.back()[0]);
      // InitializeChild(root_holder.nested_children.back()[0]);
      // child.dictionary = root_holder.nested_children_ptr.back()[0];
      // child.dictionary->format = "u";
      // break;
    default:
      CIDER_THROW(CiderCompileException,
                  "Unsupported Arrow type " + type.ToString() + " for DuckDB");
  }
}

void DuckDbArrowSchemaAdaptor::SetArrowMapFormat(DuckDBArrowSchemaHolder& root_holder,
                                                 ArrowSchema& child,
                                                 const ::duckdb::LogicalType& type) {
  child.format = "+m";
  //! Map has one child which is a struct
  child.n_children = 1;
  root_holder.nested_children.emplace_back();
  root_holder.nested_children.back().resize(1);
  root_holder.nested_children_ptr.emplace_back();
  root_holder.nested_children_ptr.back().push_back(
      &root_holder.nested_children.back()[0]);
  InitializeChild(root_holder.nested_children.back()[0]);
  child.children = &root_holder.nested_children_ptr.back()[0];
  child.children[0]->name = "entries";
  ::duckdb::child_list_t<::duckdb::LogicalType> struct_child_types;
  struct_child_types.push_back(std::make_pair(
      "key",
      ::duckdb::ListType::GetChildType(::duckdb::StructType::GetChildType(type, 0))));
  struct_child_types.push_back(std::make_pair(
      "value",
      ::duckdb::ListType::GetChildType(::duckdb::StructType::GetChildType(type, 1))));
  auto struct_type = ::duckdb::LogicalType::STRUCT(move(struct_child_types));
  SetArrowFormat(root_holder, *child.children[0], struct_type);
}
