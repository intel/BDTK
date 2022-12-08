#include <arrow/api.h>
#include <arrow/compute/kernel.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/stl.h>
#include <arrow/util/iterator.h>
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"
template <typename NativeType, typename ArrowArrayType>
void constructBuilder(substrait::Type s_type,
                      ArrowArrayBuilder& builder,
                      std::shared_ptr<arrow::ChunkedArray> field,
                      const std::string& name) {
  std::vector<NativeType> cider_array(field->length());
  int p = 0;
  for (int j = 0; j < field->num_chunks(); j++) {
    auto arrow_array = std::static_pointer_cast<ArrowArrayType>(field->chunk(j));
    for (int k = 0; k < field->chunk(j)->length(); k++) {
      cider_array[p++] = arrow_array->Value(k);
    }
  }
  builder.addColumn<NativeType>(name, s_type, cider_array, {});
}

// template <typename ArrowArrayType>
// void constructStringBuilder(substrait::Type s_type,
//                             ArrowArrayBuilder& builder,
//                             std::shared_ptr<arrow::ChunkedArray> field,
//                             const std::string& name) {
//   std::vector<std::string> cider_array(field->length());
//   std::vector<std::string> offset(field->length());
//   int p = 0;
//   for (int j = 0; j < field->num_chunks(); j++) {
//     auto arrow_array = std::static_pointer_cast<ArrowArrayType>(field->chunk(j));
//     for (int k = 0; k < field->chunk(j)->length(); k++) {
//       cider_array[p++] = arrow_array->GetString(k);
//     }
//   }
//   auto [data, offsets] = ArrowBuilderUtils::createDataAndOffsetFromStrVector(cider_array);
//   builder.addUTF8Column(name, data, offsets, {});
// }

void convertToCider(ArrowArrayBuilder& builder,
                    std::shared_ptr<arrow::ChunkedArray> field,
                    const std::string& name) {
  auto tag = field->type()->id();
  switch (tag) {
    case arrow::Type::type::INT32:
      return constructBuilder<int32_t, arrow::Int32Array>(
          CREATE_SUBSTRAIT_TYPE(I32), builder, field, name);
    case arrow::Type::type::INT64:
      return constructBuilder<int64_t, arrow::Int64Array>(
          CREATE_SUBSTRAIT_TYPE(I64), builder, field, name);
    case arrow::Type::type::FLOAT:
      return constructBuilder<float, arrow::FloatArray>(
          CREATE_SUBSTRAIT_TYPE(Fp32), builder, field, name);
    case arrow::Type::type::DOUBLE:
      return constructBuilder<double, arrow::DoubleArray>(
          CREATE_SUBSTRAIT_TYPE(Fp64), builder, field, name);
    case arrow::Type::type::STRING:
     return constructBuilder<int64_t, arrow::Int64Array>(
          CREATE_SUBSTRAIT_TYPE(I64), builder, field, name);
  }
}

std::shared_ptr<CiderBatch> readFromCsv(std::string csv_file,
                                        std::vector<std::string>& col_name) {
  std::shared_ptr<arrow::io::InputStream> input;
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto cvsFile = arrow::io::ReadableFile::Open(csv_file);
  if (cvsFile.ok()) {
    input = std::move(cvsFile).ValueOrDie();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    // Instantiate TableReader from input stream and options
    auto reader = arrow::csv::TableReader::Make(
        io_context, input, read_options, parse_options, convert_options);
    if (reader.ok()) {
      std::shared_ptr<arrow::csv::TableReader> tableReader =
          std::move(reader).ValueOrDie();
      auto t = tableReader->Read();
      if (t.ok()) {
        std::shared_ptr<arrow::Table> table = std::move(t).ValueOrDie();
        int64_t rows = table->num_rows();
        int64_t columns = table->num_columns();
        std::cout << "rows:- " << rows << " columns:- " << columns << '\n';
        auto builder = ArrowArrayBuilder().setRowNum(rows);
        for (int i = 0; i < table->num_columns(); i++) {
          std::shared_ptr<arrow::ChunkedArray> field = table->column(i);
          std::cout << field->type()->ToString() << ' ';
          convertToCider(builder, field, col_name[i]);
        }
        std::cout << '\n';
        auto schema_and_array = builder.build();
        return std::make_shared<CiderBatch>(std::get<0>(schema_and_array),
                                            std::get<1>(schema_and_array),
                                            std::make_shared<CiderDefaultAllocator>());
      }
    }
  }
}