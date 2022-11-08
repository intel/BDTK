
list(OPERATORS_SOURCE)

macro(add_OpNode op_node_name)
  list(APPEND OPERATORS_SOURCE ${CMAKE_CURRENT_LIST_DIR}/${op_node_name}.cpp)
endmacro(add_OpNode)

add_OpNode(FilterNode)

add_library(cider_operators OBJECT ${OPERATORS_SOURCE})
target_link_libraries(cider_operators ${NEXTGEN_DEPS})
