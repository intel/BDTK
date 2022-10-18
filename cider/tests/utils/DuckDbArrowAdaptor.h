#ifndef CIDER_DUCKDBARROWADAPTOR_H
#define CIDER_DUCKDBARROWADAPTOR_H

#include <vector>
#include "cider/CiderBatch.h"
#include "duckdb.hpp"

// function interfaces from duckdb are different when enabling/disabling velox
// for interface stability, we maintain a copy of relevant codes in duckdb
// most of the following codes are copied from duckdb sources

struct DuckDBArrowSchemaHolder {
  // unused in children
  std::vector<ArrowSchema> children;
  // unused in children
  std::vector<ArrowSchema*> children_ptrs;
  //! used for nested structures
  std::list<std::vector<ArrowSchema>> nested_children;
  std::list<std::vector<ArrowSchema*>> nested_children_ptr;
  //! This holds strings created to represent decimal types
  std::vector<std::unique_ptr<char[]>> owned_type_names;
};

class DuckDbArrowSchemaAdaptor {
 public:
  static void ReleaseDuckDBArrowSchema(ArrowSchema* schema);

  static void InitializeChild(ArrowSchema& child, const std::string& name = "") {
    //! Child is cleaned up by parent
    child.private_data = nullptr;
    child.release = ReleaseDuckDBArrowSchema;

    //! Store the child schema
    child.flags = ARROW_FLAG_NULLABLE;
    child.name = name.c_str();
    child.n_children = 0;
    child.children = nullptr;
    child.metadata = nullptr;
    child.dictionary = nullptr;
  }

  static void SetArrowMapFormat(DuckDBArrowSchemaHolder& root_holder,
                                ArrowSchema& child,
                                const ::duckdb::LogicalType& type);

  static void SetArrowFormat(DuckDBArrowSchemaHolder& root_holder,
                             ArrowSchema& child,
                             const ::duckdb::LogicalType& type);
};

#endif  // CIDER_DUCKDBARROWADAPTOR_H
