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

#ifndef CIDER_UTILS_H
#define CIDER_UTILS_H

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "substrait/type.pb.h"

#include "cider/CiderBatch.h"
#include "cider/CiderTableSchema.h"

struct CommandResult {
  std::string output;
  int exitstatus;

  friend std::ostream& operator<<(std::ostream& os, const CommandResult& result) {
    os << "command exitstatus: " << result.exitstatus << " output: " << result.output;
    return os;
  }
  bool operator==(const CommandResult& rhs) const {
    return output == rhs.output && exitstatus == rhs.exitstatus;
  }
  bool operator!=(const CommandResult& rhs) const { return !(rhs == *this); }
};

class Command {
 public:
  /**
   * Execute system command and get STDOUT result.
   * Like system() but gives back exit status and stdout.
   * @param command system command to execute
   * @return CommandResult containing STDOUT (not stderr) output & exitstatus
   * of command. Empty if command failed (or has no output). If you want stderr,
   * use shell redirection (2&>1).
   */
  static CommandResult exec(const std::string& command);
};

class TpcHTableCreator {
 public:
  static std::vector<std::string> createAllTables();

  static std::string createTpcHTables(const std::string& table_name);

 private:
  static std::string createTpcHTableOrders();

  static std::string createTpcHTableLineitem();

  static std::string createTpcHTablePart();

  static std::string createTpcHTableSupplier();

  static std::string createTpcHTablePartsupp();

  static std::string createTpcHTableCustomer();

  static std::string createTpcHTableNation();

  static std::string createTpcHTableRegion();
};

class RunIsthmus {
 public:
  static std::string processTpchSql(std::string sql);

  static std::string processSql(std::string sql, std::string createDdl);

 private:
  static std::string getIsthmusExecutable();
  static std::string createTpcHTables();
};

class Random {
 public:
  /**
   * One in X chance of generating a null value in the output vector
   * (e.g: 10 means 1/10 for 10%, 2 means 1/2 for 50%, 1 for 100%, 0 for 0%).
   */
  static bool oneIn(int32_t n, std::mt19937& rng) {
    if (n < 2) {
      return n;
    }
    return randInt32(0, n - 1, rng) == 0;
  }

  static int32_t randInt32(int32_t min, int32_t max, std::mt19937& rng) {
    if (min == max) {
      return min;
    }
    return std::uniform_int_distribution<int32_t>(min, max)(rng);
  }

  static int64_t randInt64(int64_t min, int64_t max, std::mt19937& rng) {
    if (min == max) {
      return min;
    }
    return std::uniform_int_distribution<int64_t>(min, max)(rng);
  }

  static _Float32 randFloat(float_t min, float_t max, std::mt19937& rng) {
    if (min == max) {
      return min;
    }
    return std::uniform_real_distribution<_Float32>(min, max)(rng);
  }
};

class SchemaUtils {
 public:
  // Get data type based on flattened table schema
  static substrait::Type getFlattenColumnTypeById(
      const std::shared_ptr<CiderTableSchema> schema,
      const int column) {
    CHECK(schema->getFlattenColumnTypes().size() > 0 &&
          column < schema->getFlattenColumnTypes().size());
    return schema->getFlattenColumnTypes()[column];
  }
};

class ArrowBuilderUtils {
 public:
  static std::shared_ptr<CiderBatch> createCiderBatchFromArrowBuilder(
      std::tuple<ArrowSchema*&, ArrowArray*&> array_with_schema) {
    ArrowSchema* schema = nullptr;
    ArrowArray* array = nullptr;
    std::tie(schema, array) = array_with_schema;

    return std::make_shared<CiderBatch>(
        schema, array, std::make_shared<CiderDefaultAllocator>());
  }

  static std::tuple<std::string, std::vector<int32_t>> createDataAndOffsetFromStrVector(
      const std::vector<std::string>& input) {
    std::vector<int32_t> offsets{0};
    std::string str_buffer;
    for (auto& s : input) {
      str_buffer = str_buffer + s;
      offsets.push_back(offsets.back() + s.size());
    }

    return {str_buffer, offsets};
  }
};

#endif  // CIDER_UTILS_H
