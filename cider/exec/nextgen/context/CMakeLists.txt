set(CONTEXT_SOURCE ${CMAKE_CURRENT_LIST_DIR}/CodegenContext.cpp ${CMAKE_CURRENT_LIST_DIR}/RuntimeContext.cpp
${CMAKE_CURRENT_LIST_DIR}/Batch.cpp)

add_library(cider_context OBJECT ${CONTEXT_SOURCE})