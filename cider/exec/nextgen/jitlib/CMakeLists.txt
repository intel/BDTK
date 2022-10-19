
set(LLVMJIT_SOURCE 
${CMAKE_CURRENT_LIST_DIR}/llvmjit/LLVMJITEngine.cpp 
${CMAKE_CURRENT_LIST_DIR}/llvmjit/LLVMJITFunction.cpp 
${CMAKE_CURRENT_LIST_DIR}/llvmjit/LLVMJITModule.cpp 
)

add_library(jitlib STATIC ${LLVMJIT_SOURCE})
target_link_libraries(jitlib 
${llvm_libs}
cider_util
)