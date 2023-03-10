/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#ifndef CIDER_PARQUETREADER_H
#define CIDER_PARQUETREADER_H

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/util/cpu_info.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <string>
#include <vector>
#include "exec/plan/parser/TypeUtils.h"
#include "util/ArrowArrayBuilder.h"
#include "util/Logger.h"

namespace CiderParquetReader {
class Reader {
 public:
  Reader(/* args */);
  ~Reader();
  void init(std::string fileName,
            std::string requiredSchema,
            int firstRowGroup,
            int rowGroupToRead);

  void close();
  int readBatch(int32_t batchSize, ArrowSchema* outputSchema, ArrowArray* outputArray);
  bool hasNext();

 private:
  void convertSchema(std::string requiredSchema);
  void doReadBatch(int rowsToRead,
                   std::vector<int64_t*>& buffersPtr,
                   std::vector<uint8_t*>& nullsPtr);
  void initRowGroupReaders();
  bool checkEndOfRowGroup();
  void allocateBuffers(int rowsToRead,
                       std::vector<int64_t*>& buffersPtr,
                       std::vector<uint8_t*>& nullsPtr);
  std::tuple<ArrowSchema*&, ArrowArray*&> convert2Arrow(int rowsToRead,
                                                        std::vector<int64_t*>& buffersPtr,
                                                        std::vector<uint8_t*>& nullsPtr);

  std::shared_ptr<arrow::io::RandomAccessFile> file_;
  std::unique_ptr<parquet::ParquetFileReader> parquetReader_;
  std::vector<std::shared_ptr<parquet::ColumnReader>> columnReaders_;
  std::shared_ptr<parquet::FileMetaData> fileMetaData_;

  int firstRowGroupIndex_ = 0;
  int totalRowGroupsRead_ = 0;
  int totalRowGroups_ = 0;
  int totalColumns_ = 0;
  int currentRowGroup_ = 0;
  int initRequiredColumnCount_ = 0;
  int currentBatchSize_ = 0;

  int64_t totalRowsRead_ = 0;
  int64_t totalRows_ = 0;
  int64_t totalRowsLoadedSoFar_ = 0;

  std::vector<std::shared_ptr<parquet::RowGroupReader>> rowGroupReaders_;
  std::shared_ptr<parquet::RowGroupReader> rowGroupReader_;

  std::vector<int> requiredColumnIndex_;
  std::vector<std::string> requiredColumnNames_;
  std::vector<int> usedInitBufferIndex_;
  std::vector<int64_t> nullCountVector_;

  std::vector<parquet::Type::type> typeVector_ = std::vector<parquet::Type::type>();
};
}  // namespace CiderParquetReader

#endif  // CIDER_PARQUETREADER_H
