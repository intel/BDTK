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

#include <gtest/gtest.h>
#include "type/schema/ColumnInfo.h"
#include "type/schema/TableInfo.h"

#include "exec/template/AggregatedColRange.h"
#include "exec/template/InputMetadata.h"
#include "exec/template/common/descriptors/InputDescriptors.h"

#include "cider/CiderRuntimeModule.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"

const int db_id = 100;

class MockTable {
 public:
  MockTable(const std::vector<std::string>& col_names,
            const std::vector<SQLTypeInfo>& col_types,
            const std::vector<int8_t*>& col_datas,
            size_t element_num)
      : col_names_(col_names)
      , col_types_(col_types)
      , col_datas_(col_datas)
      , element_num_(element_num)
      , fake_table_desc_(db_id, 111, "TestMockTable", false, MemoryLevel::CPU_LEVEL, 0) {
    const bool is_row_id = false;

    for (int i = 1; i <= col_names.size(); ++i) {
      fake_col_descs_.insert(
          std::make_pair(col_names[i - 1],
                         std::make_shared<ColumnInfo>(db_id,
                                                      fake_table_desc_.table_id,
                                                      i,
                                                      col_names_[i - 1],
                                                      col_types_[i - 1],
                                                      is_row_id)));
    }
  }

  ~MockTable() {}

  int8_t* getColData(const std::string& col_name) const {
    auto iter = fake_col_descs_.find(col_name);
    CHECK(iter != fake_col_descs_.end());
    return col_datas_[iter->second->column_id - 1];
  }

  size_t getRowNum() const { return element_num_; }

  ColumnInfoPtr getMetadataForColumn(const std::string& col_name) {
    auto iter = fake_col_descs_.find(col_name);
    CHECK(iter != fake_col_descs_.end());
    return iter->second;
  }

  const TableInfo* getMetadataForTable() const { return &fake_table_desc_; }

  std::list<std::shared_ptr<const InputColDescriptor>> getInputColDescs(
      const std::vector<ColumnInfoPtr>& col_desc_list) const {
    std::list<std::shared_ptr<const InputColDescriptor>> out;
    for (auto col_desc : col_desc_list) {
      out.push_back(std::make_shared<InputColDescriptor>(col_desc, 0));
    }

    return out;
  }

  template <typename T>
  void copyDataToScalarBatch(CiderBatch* child, const int8_t* data) const {
    CHECK(child->resizeBatch(element_num_, true));
    auto ptr = child->asMutable<T>()->getMutableRawData();
    std::memcpy(ptr, data, sizeof(std::remove_pointer_t<decltype(ptr)>) * element_num_);
  }

  std::unique_ptr<CiderBatch> generateStructBatch(
      const std::vector<std::string>& col_names) const {
    std::vector<SQLTypeInfo> children_types;
    children_types.reserve(col_names.size());
    for (const auto& name : col_names) {
      auto iter = fake_col_descs_.find(name);
      CHECK(fake_col_descs_.end() != iter);
      children_types.push_back(col_types_[iter->second->column_id - 1]);
    }
    auto type = SQLTypeInfo(kSTRUCT, false, children_types);
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(type);
    auto batch = StructBatch::Create(schema, std::make_shared<CiderDefaultAllocator>());
    CHECK(batch->resizeBatch(element_num_, true));

    for (size_t i = 0; i < col_names.size(); ++i) {
      auto child = batch->getChildAt(i);
      auto iter = fake_col_descs_.find(col_names[i]);
      CHECK(fake_col_descs_.end() != iter);
      auto data = col_datas_[iter->second->column_id - 1];
      switch (child->getCiderType()) {
        case kBOOLEAN:
          copyDataToScalarBatch<ScalarBatch<bool>>(child.get(), data);
          break;
        case kTINYINT:
          copyDataToScalarBatch<ScalarBatch<int8_t>>(child.get(), data);
          break;
        case kSMALLINT:
          copyDataToScalarBatch<ScalarBatch<int16_t>>(child.get(), data);
          break;
        case kINT:
          copyDataToScalarBatch<ScalarBatch<int32_t>>(child.get(), data);
          break;
        case kBIGINT:
          copyDataToScalarBatch<ScalarBatch<int64_t>>(child.get(), data);
          break;
        case kFLOAT:
          copyDataToScalarBatch<ScalarBatch<float>>(child.get(), data);
          break;
        case kDOUBLE:
          copyDataToScalarBatch<ScalarBatch<double>>(child.get(), data);
          break;
        default:
          CIDER_THROW(CiderCompileException, "Unsupported type to generate StructBatch.");
      }
    }
    return batch;
  }

  std::vector<const int8_t*> getInputData(
      const std::vector<std::string>& col_names) const {
    std::vector<const int8_t*> out;
    out.reserve(col_names.size());

    for (auto& name : col_names) {
      auto iter = fake_col_descs_.find(name);
      CHECK(fake_col_descs_.end() != iter);
      out.push_back(col_datas_[iter->second->column_id - 1]);
    }

    return out;
  }

  InputTableInfo getInputTableInfo() const {
    Fragmenter_Namespace::FragmentInfo frag_info;
    frag_info.fragmentId = 0;
    frag_info.shadowNumTuples = element_num_;
    frag_info.physicalTableId = fake_table_desc_.table_id;
    frag_info.setPhysicalNumTuples(element_num_);

    Fragmenter_Namespace::TableInfo table_info;
    table_info.fragments = {frag_info};
    table_info.setPhysicalNumTuples(element_num_);

    return InputTableInfo{fake_table_desc_.db_id, fake_table_desc_.table_id, table_info};
  }

 private:
  PhysicalInput getPhysicalInputs(const std::string& col_name) const {
    auto iter = fake_col_descs_.find(col_name);
    CHECK(fake_col_descs_.end() != iter);
    return {iter->second->column_id, fake_table_desc_.table_id};
  }

  std::unordered_set<int> getPhysicalTableId() const {
    return {fake_table_desc_.table_id};
  }

  std::vector<std::string> col_names_;
  std::vector<SQLTypeInfo> col_types_;
  std::vector<int8_t*> col_datas_;
  TableInfo fake_table_desc_;
  std::unordered_map<std::string, ColumnInfoPtr> fake_col_descs_;
  size_t element_num_;
};

using TargetType = std::variant<int32_t, int64_t, float, double>;

struct AggResult {
  int64_t count_one;
  int64_t count_column;
  union {
    int64_t sum_int64;
    float sum_float;
    double sum_double;
  };
  union {
    int64_t max_int64;
    float max_float;
    double max_double;
  };
  union {
    int64_t min_int64;
    float min_float;
    double min_double;
  };
  int64_t null;
};

static void verifyResult(SQLTypes type, CiderBatch* out_batch, AggResult& expect_result) {
  switch (type) {
    case kFLOAT: {
      EXPECT_EQ(*(int64_t*)out_batch->column(0), expect_result.count_one);
      EXPECT_EQ(*(int64_t*)out_batch->column(1), expect_result.count_column);
      EXPECT_FLOAT_EQ(*(float*)out_batch->column(2), expect_result.sum_float);
      EXPECT_FLOAT_EQ(*(float*)out_batch->column(3), expect_result.max_float);
      EXPECT_FLOAT_EQ(*(float*)out_batch->column(4), expect_result.min_float);
      EXPECT_FLOAT_EQ(*(float*)out_batch->column(5), expect_result.sum_float);
      EXPECT_EQ(*(int64_t*)out_batch->column(6), expect_result.count_column);
      // EXPECT_EQ(*(int64_t*)out_batch->column(7), expect_result.null);
      // EXPECT_EQ(*(int64_t*)out_batch->column(8), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(9), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(10), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(11), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(12), expect_result.null);
      break;
    }
    case kDOUBLE: {
      EXPECT_EQ(*(int64_t*)out_batch->column(0), expect_result.count_one);
      EXPECT_EQ(*(int64_t*)out_batch->column(1), expect_result.count_column);
      EXPECT_DOUBLE_EQ(*(double*)out_batch->column(2), expect_result.sum_double);
      EXPECT_DOUBLE_EQ(*(double*)out_batch->column(3), expect_result.max_double);
      EXPECT_DOUBLE_EQ(*(double*)out_batch->column(4), expect_result.min_double);
      EXPECT_DOUBLE_EQ(*(double*)out_batch->column(5), expect_result.sum_double);
      EXPECT_EQ(*(int64_t*)out_batch->column(6), expect_result.count_column);
      // EXPECT_EQ(*(int64_t*)out_batch->column(7), expect_result.null);
      // EXPECT_EQ(*(int64_t*)out_batch->column(8), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(9), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(10), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(11), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(12), expect_result.null);
      break;
    }
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT: {
      EXPECT_EQ(*(int64_t*)out_batch->column(0), expect_result.count_one);
      EXPECT_EQ(*(int64_t*)out_batch->column(1), expect_result.count_column);
      EXPECT_EQ(*(int64_t*)out_batch->column(2), expect_result.sum_int64);
      EXPECT_EQ(*(int64_t*)out_batch->column(3), expect_result.max_int64);
      EXPECT_EQ(*(int64_t*)out_batch->column(4), expect_result.min_int64);
      EXPECT_EQ(*(int64_t*)out_batch->column(5), expect_result.sum_int64);
      EXPECT_EQ(*(int64_t*)out_batch->column(6), expect_result.count_column);
      // EXPECT_EQ(*(int64_t*)out_batch->column(7), expect_result.null);
      // EXPECT_EQ(*(int64_t*)out_batch->column(8), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(9), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(10), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(11), expect_result.null);
      EXPECT_EQ(*(int64_t*)out_batch->column(12), expect_result.null);
      break;
    }
    case kBOOLEAN: {
      break;
    }
    default:
      LOG(ERROR) << "Unsupported type: ";
  }
}

void runTest(const std::string& test_name,
             const MockTable* table_ptr,
             std::shared_ptr<RelAlgExecutionUnit> ra_exe_unit_ptr,
             const std::vector<std::string>& input_cols_name,
             AggResult expect_result,
             bool all_null = false,
             bool is_columnar_layout = false,
             size_t buffer_entry_num = 16384,
             size_t spilled_entry_num = 0,
             const std::vector<CiderBitUtils::CiderBitVector<>>& nulls = {}) {
  LOG(DEBUG1) << "----------------------Test case: " + test_name +
                     " --------------------------------------";

  auto cider_compile_module =
      CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());
  auto exe_option = CiderExecutionOption::defaults();
  auto compile_option = CiderCompilationOption::defaults();

  exe_option.output_columnar_hint = is_columnar_layout;
  compile_option.max_groups_buffer_entry_guess = buffer_entry_num;
  compile_option.use_cider_groupby_hash = true;
  compile_option.use_default_col_range = true;
  compile_option.use_cider_data_format = true;

  std::vector<InputTableInfo> table_infos = {table_ptr->getInputTableInfo()};

  auto compile_result =
      cider_compile_module->compile(ra_exe_unit_ptr.get(),
                                    &table_infos,
                                    std::make_shared<CiderTableSchema>(),
                                    compile_option,
                                    exe_option);

  CiderRuntimeModule cider_runtime_module(compile_result, compile_option, exe_option);

  LOG(DEBUG1) << "EU:\n" << *ra_exe_unit_ptr;
  LOG(DEBUG1) << "MemInfo\n" << cider_runtime_module.convertQueryMemDescToString();
  LOG(DEBUG1) << "HashTable:\n"
              << cider_runtime_module.convertGroupByAggHashTableToString();

  std::unique_ptr<CiderBatch> input_batch =
      table_ptr->generateStructBatch(input_cols_name);
  for (size_t i = 0; i < input_batch->getChildrenNum(); ++i) {
    auto child = input_batch->getChildAt(i);
    const uint8_t* given_nulls = nulls[i].as<uint8_t>();
    uint8_t* child_nulls = child->getMutableNulls();
    int64_t null_count = 0;
    for (size_t j = 0; j < child->getLength(); ++j) {
      if (!all_null && CiderBitUtils::isBitSetAt(given_nulls, j)) {
        CiderBitUtils::setBitAt(child_nulls, j);
      } else {
        CiderBitUtils::clearBitAt(child_nulls, j);
        ++null_count;
      }
      child->setNullCount(null_count);
    }
  }

  cider_runtime_module.processNextBatch(*input_batch);

  LOG(DEBUG1) << "---------------------------Execution "
                 "Success-----------------------------------";

  std::vector<SQLTypes> types(ra_exe_unit_ptr->target_exprs.size());
  for (size_t i = 0; i < types.size(); ++i) {
    auto type_info = ra_exe_unit_ptr->target_exprs[i]->get_type_info();
    types[i] = type_info.get_type();
  }
  auto [_, out_batch] = cider_runtime_module.fetchResults();

  std::stringstream ss;
  for (size_t i = 0; i < out_batch->row_num(); ++i) {
    for (size_t j = 0; j < types.size(); ++j) {
      switch (types[j]) {
        case kFLOAT: {
          auto ptr = reinterpret_cast<const float*>(out_batch->column(j));
          ss << ptr[i];
          break;
        }
        case kDOUBLE: {
          auto ptr = reinterpret_cast<const double*>(out_batch->column(j));
          ss << ptr[i];
          break;
        }
        case kTEXT:
        case kVARCHAR:
        case kCHAR:
        case kINT: {
          auto ptr = reinterpret_cast<const int*>(out_batch->column(j));
          ss << ptr[i];
          break;
        }
        case kDECIMAL:
        case kBIGINT: {
          auto ptr = reinterpret_cast<const int64_t*>(out_batch->column(j));
          ss << ptr[i];
          break;
        }
        case kBOOLEAN: {
          auto ptr = reinterpret_cast<const bool*>(out_batch->column(j));
          ss << ptr[i];
          break;
        }
        default:
          LOG(ERROR) << "Unsupported type: " << types[j];
      }
      ss << " ";
    }
    ss << "\n";
  }

  LOG(DEBUG1) << ss.str();
  verifyResult(ra_exe_unit_ptr->target_exprs[2]->get_type_info().get_type(),
               out_batch.get(),
               expect_result);
}
