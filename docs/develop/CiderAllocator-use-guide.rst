========================
CiderAllocator use guide
========================

1 introduction to CiderAllocator
---------------------------------------------------------------------------------------------------------

We provide the allocation interface in CiderAllocator.h and two default implementations

1.1 CiderDefaultAllocator provide the default allocate and deallocate function via system library
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1.2 AlignAllocator provide the function to perform memory alignment
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

2 how to use
---------------------------------------------------------------------------------------------------------

user can construct an allocator from upstream memory pool, and then memory allocated will be in the pool.
or use the CiderDefaultAllocator, AlignAllocator if they don't want to do it, 
but this allocator must be present and passed throughout the calling logic
because Only in this way can you know the memory usage during the entire operation for memory managerment

A typical usage in CiderRunTimeModule

2.1  add the member variables in .h file
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

``std::shared_ptr<CiderAllocator> allocator``
 and add it in the constructor::
    
    CiderRuntimeModule(
      std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
      CiderCompilationOption ciderCompilationOption = CiderCompilationOption::defaults(),
      CiderExecutionOption ciderExecutionOption = CiderExecutionOption::defaults(),
      std::shared_ptr<CiderAllocator> allocator = std::make_shared<CiderDefaultAllocator>());

2.2 in .cpp implementation class
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

::

    CiderRuntimeModule::CiderRuntimeModule(
        std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
        CiderCompilationOption ciderCompilationOption,
        CiderExecutionOption ciderExecutionOption,
        std::shared_ptr<CiderAllocator> allocator)
        : ciderCompilationResult_(ciderCompilationResult)
        , ciderCompilationOption_(ciderCompilationOption)
        , ciderExecutionOption_(ciderExecutionOption)
        , allocator_(allocator)

2.3 Then we can use it in the implementation class for memory allocation and release
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

::
    
    const int8_t** col_buffers = reinterpret_cast<const int8_t**>(allocator_->allocate(sizeof(int8_t**) * (total_col_num)));
    allocator_->deallocate(reinterpret_cast<int8_t*>(col_buffers),sizeof(int8_t**) * (total_col_num));

2.4 Finally we can pass a custom allocator when creating CiderRunTimeModule or use the default
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

::

    auto customAllocator = std::make_shared<CustomAllocator>();
    cider_runtime_module_ = std::make_shared<CiderRuntimeModule>(compile_res_, compile_option, exe_option, customAllocator);
