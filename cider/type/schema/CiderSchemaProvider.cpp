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

#include "type/schema/CiderSchemaProvider.h"
#include "exec/plan/parser/ConverterHelper.h"
#include "util/Logger.h"

const int k_db_id = 100;

std::vector<int> CiderSchemaProvider::listDatabases() const {
  std::vector<int> res;
  res.push_back(k_db_id);
  return res;
}

TableInfoList CiderSchemaProvider::listTables(int db_id) const {
  TableInfoList res;
  int table_id = 100;
  for (size_t i = 0; i < tableSchemas_.size(); ++i) {
    TableInfoPtr tableInfo =
        std::make_shared<TableInfo>(db_id,
                                    table_id + i,
                                    std::to_string(table_id + i),
                                    false,
                                    Data_Namespace::MemoryLevel::CPU_LEVEL,
                                    1);  // fragments
    res.push_back(tableInfo);
  }
  return res;
}

ColumnInfoList CiderSchemaProvider::listColumns(int db_id, int table_id) const {
  ColumnInfoList columnInfoList;
  CHECK_EQ(k_db_id, db_id);
  int tb_index = table_id - 100;
  CiderTableSchema tableSchema = tableSchemas_[tb_index];
  for (int i = 0; i < tableSchema.getColumnCount(); ++i) {
    ColumnInfoPtr columnInfo = std::make_shared<ColumnInfo>(
        db_id,
        table_id,
        i,
        tableSchema.getColumnNameById(i),
        generator::getSQLTypeInfo(tableSchema.getColumnTypeById(i)),
        false);
    columnInfoList.push_back(columnInfo);
  }

  return columnInfoList;
}

TableInfoPtr CiderSchemaProvider::getTableInfo(int db_id, int table_id) const {
  UNREACHABLE();
}
TableInfoPtr CiderSchemaProvider::getTableInfo(int db_id,
                                               const std::string& table_name) const {
  UNREACHABLE();
}

ColumnInfoPtr CiderSchemaProvider::getColumnInfo(int db_id,
                                                 int table_id,
                                                 int col_id) const {
  auto tableSchema = tableSchemas_[table_id - 100];
  ColumnInfoPtr columnInfo = std::make_shared<ColumnInfo>(
      db_id,
      table_id,
      col_id,
      tableSchema.getColumnNameById(col_id),
      generator::getSQLTypeInfo(tableSchema.getColumnTypeById(col_id)),
      false);
  return columnInfo;
}

ColumnInfoPtr CiderSchemaProvider::getColumnInfo(int db_id,
                                                 int table_id,
                                                 const std::string& col_name) const {
  UNREACHABLE();
}
