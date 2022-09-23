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

#include "Utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <algorithm>
#include <array>
#include <cstdlib>
#include "cider/CiderException.h"

CommandResult Command::exec(const std::string& command) {
  int exitcode = 255;
  std::array<char, 1048576> buffer{};
  std::string result;
#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#define WEXITSTATUS
#endif
  FILE* pipe = popen(command.c_str(), "r");
  if (!pipe) {
    CIDER_THROW(CiderCompileException, "popen() failed!");
  }
  try {
    std::size_t bytes_read;
    while (bytes_read =
               fread(buffer.data(), sizeof(buffer.at(0)), sizeof(buffer), pipe)) {
      result += std::string(buffer.data(), bytes_read);
    }
  } catch (...) {
    pclose(pipe);
    throw;
  }
  auto status = pclose(pipe);
  exitcode = WEXITSTATUS(status);
  return CommandResult{result, exitcode};
}

std::vector<std::string> TpcHTableCreator::createAllTables() {
  return {createTpcHTableOrders(),
          createTpcHTableLineitem(),
          createTpcHTablePart(),
          createTpcHTableSupplier(),
          createTpcHTablePartsupp(),
          createTpcHTableCustomer(),
          createTpcHTableNation(),
          createTpcHTableRegion()};
}

std::string TpcHTableCreator::createTpcHTables(const std::string& table_name) {
  std::string name = table_name;
  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
  if (name == "orders") {
    return createTpcHTableOrders();
  }
  if (name == "lineitem") {
    return createTpcHTableLineitem();
  }
  if (name == "part") {
    return createTpcHTablePart();
  }
  if (name == "supplier") {
    return createTpcHTableSupplier();
  }
  if (name == "partsupp") {
    return createTpcHTablePartsupp();
  }
  if (name == "customer") {
    return createTpcHTableCustomer();
  }
  if (name == "nation") {
    return createTpcHTableNation();
  }
  if (name == "region") {
    return createTpcHTableRegion();
  }
  CIDER_THROW(CiderCompileException, "table " + table_name + " is not in TPC-H.");
}

std::string TpcHTableCreator::createTpcHTableOrders() {
  return "CREATE TABLE ORDERS (O_ORDERKEY BIGINT NOT NULL, O_CUSTKEY "
         "BIGINT NOT NULL, O_ORDERSTATUS VARCHAR(10), O_TOTALPRICE DECIMAL, "
         "O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR(10), O_CLERK VARCHAR(10), "
         "O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR(10)); ";
}

std::string TpcHTableCreator::createTpcHTableLineitem() {
  return "CREATE TABLE LINEITEM (L_ORDERKEY BIGINT NOT NULL, L_PARTKEY "
         "BIGINT NOT NULL, L_SUPPKEY BIGINT NOT NULL, L_LINENUMBER INTEGER, L_QUANTITY "
         "DECIMAL, L_EXTENDEDPRICE DECIMAL, L_DISCOUNT DECIMAL, L_TAX DECIMAL, "
         "L_RETURNFLAG CHAR(1), L_LINESTATUS CHAR(1), L_SHIPDATE DATE, L_COMMITDATE "
         "DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25), L_SHIPMODE CHAR(10), "
         "L_COMMENT VARCHAR(44)); ";
}

std::string TpcHTableCreator::createTpcHTablePart() {
  return "CREATE TABLE PART (P_PARTKEY BIGINT NOT NULL, P_NAME "
         "VARCHAR(55), P_MFGR CHAR(25), P_BRAND CHAR(10), P_TYPE VARCHAR(25), P_SIZE "
         "INTEGER, P_CONTAINER CHAR(10), P_RETAILPRICE DECIMAL, P_COMMENT "
         "VARCHAR(23)); ";
}

std::string TpcHTableCreator::createTpcHTableSupplier() {
  return "CREATE TABLE SUPPLIER (S_SUPPKEY BIGINT NOT NULL, S_NAME "
         "CHAR(25), S_ADDRESS VARCHAR(40), S_NATIONKEY BIGINT NOT NULL, S_PHONE "
         "CHAR(15), S_ACCTBAL DECIMAL, S_COMMENT VARCHAR(101)); ";
}

std::string TpcHTableCreator::createTpcHTablePartsupp() {
  return "CREATE TABLE PARTSUPP (PS_PARTKEY BIGINT NOT NULL, PS_SUPPKEY "
         "BIGINT NOT NULL, PS_AVAILQTY INTEGER, PS_SUPPLYCOST DECIMAL, PS_COMMENT "
         "VARCHAR(199)); ";
}

std::string TpcHTableCreator::createTpcHTableCustomer() {
  return "CREATE TABLE CUSTOMER ( C_CUSTKEY BIGINT NOT NULL, C_NAME "
         "VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY BIGINT NOT NULL, C_PHONE "
         "CHAR(15), C_ACCTBAL DECIMAL, C_MKTSEGMENT CHAR(10), C_COMMENT VARCHAR(117)); ";
}

std::string TpcHTableCreator::createTpcHTableNation() {
  return "CREATE TABLE NATION ( N_NATIONKEY BIGINT NOT NULL, N_NAME "
         "CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152));";
}

std::string TpcHTableCreator::createTpcHTableRegion() {
  return "CREATE TABLE REGION ( R_REGIONKEY BIGINT NOT NULL, R_NAME "
         "CHAR(25), R_COMMENT VARCHAR(152));";
}

std::string RunIsthmus::processTpchSql(std::string sql) {
  std::string create_table_cmd = createTpcHTables();
  return processSql(sql, create_table_cmd);
}

std::string RunIsthmus::processSql(std::string sql, std::string create_ddl) {
  std::string isthmus_exec = getIsthmusExecutable();
  std::string cmd =
      isthmus_exec + " \"" + sql + "\" " + " --create " + " \"" + create_ddl + "\" ";
  auto cmd_result = Command::exec(cmd);
  if (cmd_result.exitstatus) {
    CIDER_THROW(CiderCompileException, "invalid sql, please double check!");
  }
  return cmd_result.output;
}

std::string RunIsthmus::getIsthmusExecutable() {
  const char* isthmus_exec = std::getenv("ISTHMUS_EXEC");
  if (!isthmus_exec) {
    CIDER_THROW(CiderCompileException, "cannot find isthmus!");
  }
  return isthmus_exec;
}

std::string RunIsthmus::createTpcHTables() {
  std::vector<std::string> commands = TpcHTableCreator::createAllTables();
  std::string s;
  for (auto command : commands) {
    s += command;
  }
  return s;
}
