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

#ifndef CIDER_DUCKDBARROWADAPTOR_H
#define CIDER_DUCKDBARROWADAPTOR_H

#include <list>
#include <vector>
#include "cider/CiderBatch.h"
#include "duckdb.hpp"

// function interfaces from duckdb are different when enabling/disabling velox
// for interface stability, we maintain a copy of relevant codes in duckdb
// most of the following codes are copied from duckdb sources

struct DuckDBArrowSchemaHolder {
  // unused in children
  std::vector<ArrowSchema> children;
  // unused in children
  std::vector<ArrowSchema*> children_ptrs;
  //! used for nested structures
  std::list<std::vector<ArrowSchema>> nested_children;
  std::list<std::vector<ArrowSchema*>> nested_children_ptr;
  //! This holds strings created to represent decimal types
  std::vector<std::unique_ptr<char[]>> owned_type_names;
};

class DuckDbArrowSchemaAdaptor {
 private:
  static void ReleaseDuckDBArrowSchema(ArrowSchema* schema);

  static void InitializeChild(ArrowSchema& child, const std::string& name = "") {
    //! Child is cleaned up by parent
    child.private_data = nullptr;
    child.release = ReleaseDuckDBArrowSchema;

    //! Store the child schema
    child.flags = ARROW_FLAG_NULLABLE;
    child.name = name.c_str();
    child.n_children = 0;
    child.children = nullptr;
    child.metadata = nullptr;
    child.dictionary = nullptr;
  }

  static void SetArrowMapFormat(DuckDBArrowSchemaHolder& root_holder,
                                ArrowSchema& child,
                                const ::duckdb::LogicalType& type);

  static void SetArrowFormat(DuckDBArrowSchemaHolder& root_holder,
                             ArrowSchema& child,
                             const ::duckdb::LogicalType& type);

 public:
  static void duckdbResultSchemaToArrowSchema(ArrowSchema* out_schema,
                                              std::vector<::duckdb::LogicalType>& types,
                                              std::vector<std::string>& names);
};

#endif  // CIDER_DUCKDBARROWADAPTOR_H
