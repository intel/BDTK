#include "DuckDbArrowAdaptor.h"

// function interfaces from duckdb are different when enabling/disabling velox
// for interface stability, we maintain a copy of relevant codes in duckdb
// most of the following codes are copied from duckdb sources

void DuckDbArrowSchemaAdaptor::ReleaseDuckDBArrowSchema(ArrowSchema* schema) {
  if (!schema || !schema->release) {
    return;
  }
  schema->release = nullptr;
  auto holder = static_cast<DuckDBArrowSchemaHolder*>(schema->private_data);
  delete holder;
}

void DuckDbArrowSchemaAdaptor::SetArrowFormat(DuckDBArrowSchemaHolder& root_holder,
                                              ArrowSchema& child,
                                              const ::duckdb::LogicalType& type) {
  switch (type.id()) {
    case ::duckdb::LogicalTypeId::BOOLEAN:
      child.format = "b";
      break;
    case ::duckdb::LogicalTypeId::TINYINT:
      child.format = "c";
      break;
    case ::duckdb::LogicalTypeId::SMALLINT:
      child.format = "s";
      break;
    case ::duckdb::LogicalTypeId::INTEGER:
      child.format = "i";
      break;
    case ::duckdb::LogicalTypeId::BIGINT:
      child.format = "l";
      break;
    case ::duckdb::LogicalTypeId::UTINYINT:
      child.format = "C";
      break;
    case ::duckdb::LogicalTypeId::USMALLINT:
      child.format = "S";
      break;
    case ::duckdb::LogicalTypeId::UINTEGER:
      child.format = "I";
      break;
    case ::duckdb::LogicalTypeId::UBIGINT:
      child.format = "L";
      break;
    case ::duckdb::LogicalTypeId::FLOAT:
      child.format = "f";
      break;
    case ::duckdb::LogicalTypeId::HUGEINT:
      child.format = "d:38,0";
      break;
    case ::duckdb::LogicalTypeId::DOUBLE:
      child.format = "g";
      break;
    case ::duckdb::LogicalTypeId::UUID:
    case ::duckdb::LogicalTypeId::JSON:
    case ::duckdb::LogicalTypeId::VARCHAR:
      child.format = "u";
      break;
    case ::duckdb::LogicalTypeId::DATE:
      child.format = "tdD";
      break;
    case ::duckdb::LogicalTypeId::TIME:
    case ::duckdb::LogicalTypeId::TIME_TZ:
      child.format = "ttu";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP:
    case ::duckdb::LogicalTypeId::TIMESTAMP_TZ:
      child.format = "tsu:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_SEC:
      child.format = "tss:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_NS:
      child.format = "tsn:";
      break;
    case ::duckdb::LogicalTypeId::TIMESTAMP_MS:
      child.format = "tsm:";
      break;
    case ::duckdb::LogicalTypeId::INTERVAL:
      child.format = "tDm";
      break;
    case ::duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width, scale;
      type.GetDecimalProperties(width, scale);
      std::string format = "d:" + to_string(width) + "," + to_string(scale);
      std::unique_ptr<char[]> format_ptr =
          std::unique_ptr<char[]>(new char[format.size() + 1]);
      for (size_t i = 0; i < format.size(); i++) {
        format_ptr[i] = format[i];
      }
      format_ptr[format.size()] = '\0';
      root_holder.owned_type_names.push_back(move(format_ptr));
      child.format = root_holder.owned_type_names.back().get();
      break;
    }
    case ::duckdb::LogicalTypeId::SQLNULL: {
      child.format = "n";
      break;
    }
    case ::duckdb::LogicalTypeId::BLOB: {
      child.format = "z";
      break;
    }
    case ::duckdb::LogicalTypeId::LIST: {
      child.format = "+l";
      child.n_children = 1;
      root_holder.nested_children.emplace_back();
      root_holder.nested_children.back().resize(1);
      root_holder.nested_children_ptr.emplace_back();
      root_holder.nested_children_ptr.back().push_back(
          &root_holder.nested_children.back()[0]);
      InitializeChild(root_holder.nested_children.back()[0]);
      child.children = &root_holder.nested_children_ptr.back()[0];
      child.children[0]->name = "l";
      SetArrowFormat(
          root_holder, **child.children, ::duckdb::ListType::GetChildType(type));
      break;
    }
    case ::duckdb::LogicalTypeId::STRUCT: {
      child.format = "+s";
      auto& child_types = ::duckdb::StructType::GetChildTypes(type);
      child.n_children = child_types.size();
      root_holder.nested_children.emplace_back();
      root_holder.nested_children.back().resize(child_types.size());
      root_holder.nested_children_ptr.emplace_back();
      root_holder.nested_children_ptr.back().resize(child_types.size());
      for (idx_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
        root_holder.nested_children_ptr.back()[type_idx] =
            &root_holder.nested_children.back()[type_idx];
      }
      child.children = &root_holder.nested_children_ptr.back()[0];
      for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
        InitializeChild(*child.children[type_idx]);

        auto& struct_col_name = child_types[type_idx].first;
        std::unique_ptr<char[]> name_ptr =
            std::unique_ptr<char[]>(new char[struct_col_name.size() + 1]);
        for (size_t i = 0; i < struct_col_name.size(); i++) {
          name_ptr[i] = struct_col_name[i];
        }
        name_ptr[struct_col_name.size()] = '\0';
        root_holder.owned_type_names.push_back(move(name_ptr));

        child.children[type_idx]->name = root_holder.owned_type_names.back().get();
        SetArrowFormat(
            root_holder, *child.children[type_idx], child_types[type_idx].second);
      }
      break;
    }
    case ::duckdb::LogicalTypeId::MAP: {
      SetArrowMapFormat(root_holder, child, type);
      break;
    }
    case ::duckdb::LogicalTypeId::ENUM:
      /// NOTE: (YBRua) GetPhysicalType() implementation of DuckDB is again different
      /// it accepts different types of parameter when enabling/disabling velox
      /// since we are not daling with ENUM types for the moment
      /// we skip the following code to make the code compilable in both scenarios
      // switch (::duckdb::EnumType::GetPhysicalType(::duckdb::EnumType::GetSize(type)))
      // {
      //   case ::duckdb::PhysicalType::UINT8:
      //     child.format = "C";
      //     break;
      //   case ::duckdb::PhysicalType::UINT16:
      //     child.format = "S";
      //     break;
      //   case ::duckdb::PhysicalType::UINT32:
      //     child.format = "I";
      //     break;
      //   default:
      //     CIDER_THROW(CiderCompileException,
      //                 "Unsupported Enum Internal Type for DuckDB");
      // }
      // root_holder.nested_children.emplace_back();
      // root_holder.nested_children.back().resize(1);
      // root_holder.nested_children_ptr.emplace_back();
      // root_holder.nested_children_ptr.back().push_back(
      //     &root_holder.nested_children.back()[0]);
      // InitializeChild(root_holder.nested_children.back()[0]);
      // child.dictionary = root_holder.nested_children_ptr.back()[0];
      // child.dictionary->format = "u";
      // break;
    default:
      CIDER_THROW(CiderCompileException,
                  "Unsupported Arrow type " + type.ToString() + " for DuckDB");
  }
}

void DuckDbArrowSchemaAdaptor::SetArrowMapFormat(DuckDBArrowSchemaHolder& root_holder,
                                                 ArrowSchema& child,
                                                 const ::duckdb::LogicalType& type) {
  child.format = "+m";
  //! Map has one child which is a struct
  child.n_children = 1;
  root_holder.nested_children.emplace_back();
  root_holder.nested_children.back().resize(1);
  root_holder.nested_children_ptr.emplace_back();
  root_holder.nested_children_ptr.back().push_back(
      &root_holder.nested_children.back()[0]);
  InitializeChild(root_holder.nested_children.back()[0]);
  child.children = &root_holder.nested_children_ptr.back()[0];
  child.children[0]->name = "entries";
  ::duckdb::child_list_t<::duckdb::LogicalType> struct_child_types;
  struct_child_types.push_back(std::make_pair(
      "key",
      ::duckdb::ListType::GetChildType(::duckdb::StructType::GetChildType(type, 0))));
  struct_child_types.push_back(std::make_pair(
      "value",
      ::duckdb::ListType::GetChildType(::duckdb::StructType::GetChildType(type, 1))));
  auto struct_type = ::duckdb::LogicalType::STRUCT(move(struct_child_types));
  SetArrowFormat(root_holder, *child.children[0], struct_type);
}
