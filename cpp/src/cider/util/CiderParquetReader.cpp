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
#include <parquet/types.h>

namespace CiderParquetReader {
Reader::Reader() {}
Reader::~Reader() {}

void Reader::init(std::string fileName,
                  std::vector<std::string> requiredColumnNames,
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
  convertSchema(requiredColumnNames);

  currentRowGroup_ = firstRowGroupIndex_;
  columnReaders_.resize(requiredColumnIndex_.size());
  requiredColumnNum_ = requiredColumnIndex_.size();
  nullCountVector_.resize(requiredColumnNum_);

  initRowGroupReaders();
}

void Reader::convertSchema(std::vector<std::string> requiredColumnName) {
  requiredColumnNames_ = std::vector<std::string>(requiredColumnName);
  for (int i = 0; i < requiredColumnNames_.size(); i++) {
    int columnIndex = fileMetaData_->schema()->ColumnIndex(requiredColumnNames_[i]);
    auto columnSchema = fileMetaData_->schema()->Column(columnIndex);
    if (columnIndex >= 0) {
      requiredColumnIndex_.push_back(columnIndex);
      parquetTypeVector_.push_back(columnSchema->physical_type());
      convertedTypeVector_.push_back(columnSchema->converted_type());
    }
  }
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
  rowGroupReader_ = rowGroupReaders_[currentRowGroup_ - firstRowGroupIndex_];
  currentRowGroup_++;
  totalRowGroupsRead_++;

  for (int i = 0; i < requiredColumnIndex_.size(); i++) {
    columnReaders_[i] = rowGroupReader_->Column(requiredColumnIndex_[i]);
  }

  totalRowsLoadedSoFar_ += rowGroupReader_->metadata()->num_rows();
  LOG(INFO) << "totalRowsLoadedSoFar: " << totalRowsLoadedSoFar_;
  return true;
}

void Reader::allocateBuffers(int rowsToRead,
                             std::vector<int64_t*>& buffersPtr,
                             std::vector<uint8_t*>& nullsPtr) {
  for (int i = 0; i < buffersPtr.size(); i++) {
    switch (parquetTypeVector_[i]) {
      case parquet::Type::BOOLEAN:
        buffersPtr[i] = (int64_t*)allocator_->allocate(rowsToRead);
      case parquet::Type::INT32:
      case parquet::Type::FLOAT:
        buffersPtr[i] = (int64_t*)allocator_->allocate(rowsToRead * 4);
        break;
      case parquet::Type::INT64:
      case parquet::Type::DOUBLE:
        buffersPtr[i] = (int64_t*)allocator_->allocate(rowsToRead * 8);
        break;
      case parquet::Type::INT96:
        buffersPtr[i] =
            (int64_t*)allocator_->allocate(rowsToRead * sizeof(parquet::Int96));
        break;
      case parquet::Type::BYTE_ARRAY:
        buffersPtr[i] =
            (int64_t*)allocator_->allocate(rowsToRead * sizeof(parquet::ByteArray));
        break;
      default:
        CIDER_THROW(CiderCompileException, "unsupport type");
    }
    nullsPtr[i] = (uint8_t*)allocator_->allocate((rowsToRead + 7) >> 3);
  }
}

// return rows actually read
int Reader::readBatch(int32_t batchSize,
                      ArrowSchema*& outputSchema,
                      ArrowArray*& outputArray) {
  // this reader have read all rows
  if (totalRowsRead_ >= totalRows_) {
    return -1;
  }

  // at most read to the end of current row group
  checkEndOfRowGroup();

  int rowsToRead = std::min((int64_t)batchSize, totalRowsLoadedSoFar_ - totalRowsRead_);

  std::vector<int64_t*> buffersPtr(requiredColumnNum_);
  std::vector<uint8_t*> nullsPtr(requiredColumnNum_);

  allocateBuffers(rowsToRead, buffersPtr, nullsPtr);

  currentBatchSize_ = batchSize;

  int rowsActualRead = doReadBatch(rowsToRead, buffersPtr, nullsPtr);

  totalRowsRead_ += rowsActualRead;
  LOG(INFO) << "total rows read yet: " << totalRowsRead_;
  LOG(INFO) << "ret rows " << rowsActualRead;

  auto schema_and_array = convert2Arrow(rowsToRead, buffersPtr, nullsPtr);
  outputSchema = std::get<0>(schema_and_array);
  outputArray = std::get<1>(schema_and_array);

  return rowsActualRead;
}

int Reader::doReadBatch(int rowsToRead,
                        std::vector<int64_t*>& buffersPtr,
                        std::vector<uint8_t*>& nullsPtr) {
  std::vector<int16_t> defLevel(rowsToRead);
  std::vector<int16_t> repLevel(rowsToRead);
  LOG(INFO) << "will read " << rowsToRead << " rows";
  for (int i = 0; i < columnReaders_.size(); i++) {
    int64_t levelsRead = 0, valuesRead = 0;
    int rows = 0;
    int tmpRows = 0;
    while (rows < rowsToRead) {
      switch (parquetTypeVector_[i]) {
        case parquet::Type::BOOLEAN: {
          parquet::BoolReader* boolReader =
              static_cast<parquet::BoolReader*>(columnReaders_[i].get());
          tmpRows = boolReader->ReadBatchSpaced(rowsToRead - rows,
                                                defLevel.data(),
                                                repLevel.data(),
                                                (bool*)buffersPtr[i] + rows,
                                                nullsPtr[i],
                                                0,
                                                &levelsRead,
                                                &valuesRead,
                                                &nullCountVector_[i]);
          break;
        }
        case parquet::Type::INT32: {
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
                                                 &nullCountVector_[i]);
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
                                                 &nullCountVector_[i]);
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
                                                 &nullCountVector_[i]);
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
                                                 &nullCountVector_[i]);
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
                                                  &nullCountVector_[i]);
          break;
        }
        case parquet::Type::BYTE_ARRAY: {
          parquet::ByteArrayReader* byteArrayReader =
              static_cast<parquet::ByteArrayReader*>(columnReaders_[i].get());
          tmpRows =
              byteArrayReader->ReadBatchSpaced(rowsToRead - rows,
                                               defLevel.data(),
                                               repLevel.data(),
                                               (parquet::ByteArray*)buffersPtr[i] + rows,
                                               nullsPtr[i],
                                               0,
                                               &levelsRead,
                                               &valuesRead,
                                               &nullCountVector_[i]);
          // we do need to read twice, buffer may be overwrite, so let's do an extra
          // memory copy.
          // parquet::ByteArray* p = (parquet::ByteArray*)buffersPtr[i] + rows;
          // if (tmpRows + rows < rowsToRead) {
          //   ARROW_LOG(DEBUG) << "read rows: " << tmpRows << " need to do memory copy!";
          //   // calculate total size
          //   uint32_t totalLen = 0;
          //   for (int k = 0; k < tmpRows; k++) {
          //     totalLen += p[k].len;
          //   }
          //   char* buffer = new char[totalLen];
          //   uint32_t write = 0;
          //   for (int k = 0; k < tmpRows; k++) {
          //     std::memcpy(buffer + write, p[k].ptr, p[k].len);
          //     p[k].ptr = (uint8_t*)(buffer + write);
          //     write += p[k].len;
          //   }
          // }
          break;
        }
        default:
          CIDER_THROW(CiderCompileException, "unsupport type");
      }
      rows += tmpRows;
    }
    assert(rowsToRead == rows);
    LOG(INFO) << "columnReader read rows: " << rows;
  }
  return rowsToRead;
}

void Reader::close() {
  file_->Close();
}

bool Reader::hasNext() {
  return totalRowsRead_ < totalRows_;
}

::substrait::Type parquetType2Substrait(parquet::Type::type type) {
  switch (type) {
    case parquet::Type::BOOLEAN:
      return CREATE_SUBSTRAIT_TYPE(Bool);
    case parquet::Type::FLOAT:
      return CREATE_SUBSTRAIT_TYPE(Fp32);
    case parquet::Type::DOUBLE:
      return CREATE_SUBSTRAIT_TYPE(Fp64);
    default:
      CIDER_THROW(CiderCompileException, "unsupport type");
  }
}

::substrait::Type convertedType2Substrait(parquet::ConvertedType::type type) {
  switch (type) {
    case parquet::ConvertedType::INT_8:
      return CREATE_SUBSTRAIT_TYPE(I8);
    case parquet::ConvertedType::INT_16:
      return CREATE_SUBSTRAIT_TYPE(I16);
    case parquet::ConvertedType::INT_32:
      return CREATE_SUBSTRAIT_TYPE(I32);
    case parquet::ConvertedType::INT_64:
      return CREATE_SUBSTRAIT_TYPE(I64);
    case parquet::ConvertedType::UTF8:
      return CREATE_SUBSTRAIT_TYPE(Varchar);
    default:
      CIDER_THROW(CiderCompileException, "unsupport type");
  }
}

void Reader::compressDataBuffer(int64_t* dataBuffer, int rowsToRead, int colIdx) {
  if (convertedTypeVector_[colIdx] == parquet::ConvertedType::INT_8) {
    std::vector<int8_t> values{};
    for (int i = 0; i < rowsToRead; i++) {
      values.push_back(*((int32_t*)(dataBuffer) + i));
    }
    memcpy(dataBuffer, values.data(), sizeof(int8_t) * values.size());
  }
  if (convertedTypeVector_[colIdx] == parquet::ConvertedType::INT_16) {
    std::vector<int16_t> values{};
    for (int i = 0; i < rowsToRead; i++) {
      values.push_back(*((int32_t*)(dataBuffer) + i));
    }
    memcpy(dataBuffer, values.data(), sizeof(int16_t) * values.size());
  }
  if (parquetTypeVector_[colIdx] == parquet::Type::BOOLEAN) {
    int bitmapSize = (rowsToRead + 7) >> 3;
    uint8_t* bitmap = (uint8_t*)allocator_->allocate(bitmapSize);
    memset(bitmap, 0x00, bitmapSize);
    for (int i = 0; i < rowsToRead; i++) {
      if (*((bool*)(dataBuffer) + i)) {
        CiderBitUtils::setBitAt(bitmap, i);
      }
    }
    memcpy(dataBuffer, bitmap, bitmapSize);
  }
}

int32_t* Reader::compressVarSizeDataBuffer(int64_t* dataBuffer,
                                           uint8_t* nullsPtr,
                                           int rowsToRead) {
  parquet::ByteArray* p = (parquet::ByteArray*)dataBuffer;
  int compressedLength = 0;

  // generate offset buffer in the first loop
  std::vector<int32_t> offsetsBuffer{0};
  for (int i = 0; i < rowsToRead; i++) {
    if (CiderBitUtils::isBitSetAt(nullsPtr, i)) {
      compressedLength += p[i].len;
      offsetsBuffer.push_back(p[i].len + offsetsBuffer[i]);
    } else {
      offsetsBuffer.push_back(offsetsBuffer[i]);
    }
  }
  int offsetsBufferLen = (rowsToRead + 1) * sizeof(int32_t);
  int32_t* offsetsPtr = (int32_t*)allocator_->allocate(offsetsBufferLen);
  memcpy(offsetsPtr, offsetsBuffer.data(), offsetsBufferLen);

  // compressed data buffer in the second loop
  char* compressedDataBuffer = (char*)allocator_->allocate(compressedLength);
  int writeIdx = 0;
  for (int i = 0; i < rowsToRead; i++) {
    if (CiderBitUtils::isBitSetAt(nullsPtr, i)) {
      memcpy(compressedDataBuffer + writeIdx, p[i].ptr, p[i].len);
      writeIdx += p[i].len;
    }
  }
  memcpy(dataBuffer, compressedDataBuffer, compressedLength);
  return offsetsPtr;
}

std::tuple<ArrowSchema*&, ArrowArray*&> Reader::convert2Arrow(
    int rowsToRead,
    std::vector<int64_t*>& buffersPtr,
    std::vector<uint8_t*>& nullsPtr) {
  int32_t* offsetsBuffer = nullptr;
  auto builder = ArrowArrayBuilder().setRowNum(rowsToRead);
  for (int i = 0; i < buffersPtr.size(); i++) {
    // utf8 need to remove null values and generate offset buffer
    if (parquetTypeVector_[i] == parquet::Type::BYTE_ARRAY) {
      offsetsBuffer = compressVarSizeDataBuffer(buffersPtr[i], nullsPtr[i], rowsToRead);
    } else {
      // tinyint and smallint are stored as int32 in parquet
      // bool type need convert it to bitmap
      compressDataBuffer(buffersPtr[i], rowsToRead, i);
    }
    if (convertedTypeVector_[i] == parquet::ConvertedType::type::NONE) {
      builder.addColumn(requiredColumnNames_[i],
                        parquetType2Substrait(parquetTypeVector_[i]),
                        nullsPtr[i],
                        (uint8_t*)buffersPtr[i],
                        nullCountVector_[i]);
    } else {
      builder.addColumn(requiredColumnNames_[i],
                        convertedType2Substrait(convertedTypeVector_[i]),
                        nullsPtr[i],
                        (uint8_t*)buffersPtr[i],
                        nullCountVector_[i],
                        offsetsBuffer);
    }
  }
  return builder.build();
}

}  // namespace CiderParquetReader