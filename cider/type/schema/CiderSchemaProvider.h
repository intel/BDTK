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

#ifndef CIDER_CIDERSCHEMAPROVIDER_H
#define CIDER_CIDERSCHEMAPROVIDER_H

#include <vector>

#include "cider/CiderTableSchema.h"
#include "type/schema/SchemaProvider.h"

class CiderSchemaProvider : public SchemaProvider {
 public:
  explicit CiderSchemaProvider(std::vector<CiderTableSchema> tableSchemas)
      : tableSchemas_(tableSchemas) {}
  ~CiderSchemaProvider() {}

  int getId() const override { return -1; }
  std::string_view getName() const override { return "CiderSchemaProvider"; }

  std::vector<int> listDatabases() const override;

  TableInfoList listTables(int db_id) const override;

  ColumnInfoList listColumns(int db_id, int table_id) const override;

  TableInfoPtr getTableInfo(int db_id, int table_id) const override;
  TableInfoPtr getTableInfo(int db_id, const std::string& table_name) const override;

  ColumnInfoPtr getColumnInfo(int db_id, int table_id, int col_id) const override;
  ColumnInfoPtr getColumnInfo(int db_id,
                              int table_id,
                              const std::string& col_name) const override;

 private:
  std::vector<CiderTableSchema> tableSchemas_;
};

#endif  // CIDER_CIDERSCHEMAPROVIDER_H
