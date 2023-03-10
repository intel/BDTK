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

#include "util/CiderParquetReader.h"
#include <nlohmann/json.hpp>

namespace CiderParquetReader {
Reader::Reader() {}
Reader::~Reader() {}

void Reader::init(std::string fileName,
                  std::string requiredSchema,
                  int firstRowGroup,
                  int rowGroupToRead) {
  file_ = arrow::io::ReadableFile::Open(fileName).ValueOrDie();
  parquet::ReaderProperties properties;
  parquetReader_ = parquet::ParquetFileReader::Open(file_, properties, NULLPTR);

  fileMetaData_ = parquetReader_->metadata();

  this->firstRowGroupIndex_ = firstRowGroup;
  this->totalRowGroups_ = rowGroupToRead;

  totalColumns_ = fileMetaData_->num_columns();

  LOG(INFO) << "schema is " << fileMetaData_->schema()->ToString();
  LOG(INFO) << "required schema is " << requiredSchema;
  convertSchema(requiredSchema);

  currentRowGroup_ = firstRowGroupIndex_;

  columnReaders_.resize(requiredColumnIndex_.size());
  initRequiredColumnCount_ = requiredColumnIndex_.size();
}

void Reader::convertSchema(std::string requiredColumnName) {
  auto j = nlohmann::json::parse(requiredColumnName);
  int filedsNum = j["fields"].size();
  for (int i = 0; i < filedsNum; i++) {
    std::string columnName = j["fields"][i]["name"];
    int columnIndex = fileMetaData_->schema()->ColumnIndex(columnName);
    if (columnIndex >= 0) {
      usedInitBufferIndex_.push_back(i);
      requiredColumnIndex_.push_back(columnIndex);
      requiredColumnNames_.push_back(columnName);
    }
  }
}

void Reader::allocateBuffers(int rowsToRead,
                             std::vector<int64_t*>& buffersPtr,
                             std::vector<uint8_t*>& nullsPtr) {
  for (int i = 0; i < buffersPtr.size(); i++) {
    buffersPtr[i] = new int64_t[rowsToRead];
    memset(buffersPtr[i], 0x00, 8 * rowsToRead);
    nullsPtr[i] = new uint8_t[(rowsToRead + 7) >> 3];
    memset(nullsPtr[i], 0x00, rowsToRead);
  }
}

int Reader::readBatch(int32_t batchSize,
                      ArrowSchema* outputSchema,
                      ArrowArray* outputArray) {
  initRowGroupReaders();

  // this reader have read all rows
  if (totalRowsRead_ >= totalRows_) {
    return -1;
  }
  checkEndOfRowGroup();

  nullCountVector_.resize(columnReaders_.size());
  int rowsToRead = std::min((int64_t)batchSize, totalRowsLoadedSoFar_ - totalRowsRead_);

  std::vector<int64_t*> buffersPtr(initRequiredColumnCount_);
  std::vector<uint8_t*> nullsPtr(initRequiredColumnCount_);

  allocateBuffers(rowsToRead, buffersPtr, nullsPtr);

  currentBatchSize_ = batchSize;

  doReadBatch(rowsToRead, buffersPtr, nullsPtr);
  totalRowsRead_ += rowsToRead;
  LOG(INFO) << "total rows read yet: " << totalRowsRead_;
  LOG(INFO) << "ret rows " << batchSize;
  auto schema_and_arrow = convert2Arrow(rowsToRead, buffersPtr, nullsPtr);
  memcpy(outputSchema, std::get<0>(schema_and_arrow), sizeof(ArrowSchema*));
  memcpy(outputArray, std::get<1>(schema_and_arrow), sizeof(ArrowArray*));
  return 0;
}

void Reader::doReadBatch(int rowsToRead,
                         std::vector<int64_t*>& buffersPtr,
                         std::vector<uint8_t*>& nullsPtr) {
  std::vector<int16_t> defLevel(rowsToRead);
  std::vector<int16_t> repLevel(rowsToRead);
  LOG(INFO) << "will read " << rowsToRead << " rows";
  for (int i = 0; i < columnReaders_.size(); i++) {
    int64_t levelsRead = 0, valuesRead = 0, nullCount = 0;
    int rows = 0;
    int tmpRows = 0;
    // ReadBatchSpaced API will return rows left in a data page
    while (rows < rowsToRead) {
      // TODO: refactor. it's ugly, but didn't find some better way.
      switch (typeVector_[i]) {
        case parquet::Type::INT32: {
          int64_t* tmp = (int64_t*)(new int32_t[10]);
          parquet::Int32Reader* int32Reader =
              static_cast<parquet::Int32Reader*>(columnReaders_[i].get());
          tmpRows = int32Reader->ReadBatchSpaced(rowsToRead - rows,
                                                 defLevel.data(),
                                                 repLevel.data(),
                                                 (int32_t*)buffersPtr[i] + rows,
                                                 nullsPtr[i],
                                                 0,
                                                 &levelsRead,
                                                 &valuesRead,
                                                 &nullCount);
          break;
        }
        case parquet::Type::INT64: {
          parquet::Int64Reader* int64Reader =
              static_cast<parquet::Int64Reader*>(columnReaders_[i].get());
          tmpRows = int64Reader->ReadBatchSpaced(rowsToRead - rows,
                                                 defLevel.data(),
                                                 repLevel.data(),
                                                 (int64_t*)buffersPtr[i] + rows,
                                                 nullsPtr[i],
                                                 0,
                                                 &levelsRead,
                                                 &valuesRead,
                                                 &nullCount);
          break;
        }
        case parquet::Type::INT96: {
          parquet::Int96Reader* int96Reader =
              static_cast<parquet::Int96Reader*>(columnReaders_[i].get());
          tmpRows = int96Reader->ReadBatchSpaced(rowsToRead - rows,
                                                 defLevel.data(),
                                                 repLevel.data(),
                                                 (parquet::Int96*)buffersPtr[i] + rows,
                                                 nullsPtr[i],
                                                 0,
                                                 &levelsRead,
                                                 &valuesRead,
                                                 &nullCount);
          break;
        }
        case parquet::Type::FLOAT: {
          parquet::FloatReader* floatReader =
              static_cast<parquet::FloatReader*>(columnReaders_[i].get());
          tmpRows = floatReader->ReadBatchSpaced(rowsToRead - rows,
                                                 defLevel.data(),
                                                 repLevel.data(),
                                                 (float*)buffersPtr[i] + rows,
                                                 nullsPtr[i],
                                                 0,
                                                 &levelsRead,
                                                 &valuesRead,
                                                 &nullCount);
          break;
        }
        case parquet::Type::DOUBLE: {
          parquet::DoubleReader* doubleReader =
              static_cast<parquet::DoubleReader*>(columnReaders_[i].get());
          tmpRows = doubleReader->ReadBatchSpaced(rowsToRead - rows,
                                                  defLevel.data(),
                                                  repLevel.data(),
                                                  (double*)buffersPtr[i] + rows,
                                                  nullsPtr[i],
                                                  0,
                                                  &levelsRead,
                                                  &valuesRead,
                                                  &nullCount);
          break;
        }
        default:
          LOG(WARNING) << "Unsupported Type!";
          break;
      }
      rows += tmpRows;
      nullCountVector_[i] = nullCount;
    }
    assert(rowsToRead == rows);
    LOG(INFO) << "columnReader read rows: " << rows;
  }
}

void Reader::close() {
  file_->Close();
}
bool Reader::hasNext() {
  return columnReaders_[0]->HasNext();
}

void Reader::initRowGroupReaders() {
  if (rowGroupReaders_.size() > 0) {
    return;
  }

  rowGroupReaders_.resize(totalRowGroups_);
  for (int i = 0; i < totalRowGroups_; i++) {
    rowGroupReaders_[i] = parquetReader_->RowGroup(firstRowGroupIndex_ + i);
    totalRows_ += rowGroupReaders_[i]->metadata()->num_rows();
    LOG(INFO) << "this rg have rows: " << rowGroupReaders_[i]->metadata()->num_rows();
  }
}

bool Reader::checkEndOfRowGroup() {
  if (totalRowsRead_ != totalRowsLoadedSoFar_) {
    return false;
  }

  // rowGroupReaders index starts from 0
  LOG(INFO) << "totalRowsLoadedSoFar: " << totalRowsLoadedSoFar_;
  rowGroupReader_ = rowGroupReaders_[currentRowGroup_ - firstRowGroupIndex_];
  currentRowGroup_++;
  totalRowGroupsRead_++;

  for (int i = 0; i < requiredColumnIndex_.size(); i++) {
    columnReaders_[i] = rowGroupReader_->Column(requiredColumnIndex_[i]);
  }

  if (typeVector_.size() == 0) {
    for (int i = 0; i < requiredColumnIndex_.size(); i++) {
      typeVector_.push_back(
          fileMetaData_->schema()->Column(requiredColumnIndex_[i])->physical_type());
    }
  }

  totalRowsLoadedSoFar_ += rowGroupReader_->metadata()->num_rows();
  return true;
}

std::tuple<ArrowSchema*&, ArrowArray*&> Reader::convert2Arrow(
    int rowsToRead,
    std::vector<int64_t*>& buffersPtr,
    std::vector<uint8_t*>& nullsPtr) {
  auto builder = ArrowArrayBuilder().setRowNum(rowsToRead);
  for (int i = 0; i < buffersPtr.size(); i++) {
    builder.addColumn(requiredColumnNames_[i],
                      CREATE_SUBSTRAIT_TYPE(I64),
                      (uint8_t*)buffersPtr[i],
                      nullsPtr[i],
                      nullCountVector_[i]);
  }
  return builder.build();
}

}  // namespace CiderParquetReader