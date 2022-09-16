# Copyright (c) 2022 Intel Corporation.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -Wall -Wno-attributes"
)

set_source_files_properties(RuntimeFunctionsCodegenWithIncludes.cpp
                            PROPERTIES COMPILE_FLAGS -O0)
set(query_engine_source_files
    AggregatedColRange.cpp
    ArithmeticIR.cpp
    ArrayIR.cpp
    BitmapGenerators.cpp
    CardinalityEstimator.cpp
    CaseIR.cpp
    CastIR.cpp
    CgenState.cpp
    Codec.cpp
    ColumnarResults.cpp
    ColumnFetcher.cpp
    ColumnIR.cpp
    CompareIR.cpp
    ConstantIR.cpp
    DateTimeIR.cpp
    DateTimePlusRewrite.cpp
    DateTimeTranslator.cpp
    common/descriptors/ColSlotContext.cpp
    common/descriptors/QueryMemoryDescriptor.cpp
    Execute.cpp
    ExpressionRange.cpp
    ExpressionRewrite.cpp
    ExtensionsIR.cpp
    FromTableReordering.cpp
    InValuesIR.cpp
    IRCodegen.cpp
    GroupByAndAggregate.cpp
    InValuesBitmap.cpp
    InputMetadata.cpp
    operator/join/JoinFilterPushDown.cpp
    operator/join/EquiJoinCondition.cpp
    operator/join/JoinLoop.cpp
    operator/join/hashtable/BaselineJoinHashTable.cpp
    operator/join/hashtable/HashJoin.cpp
    operator/join/hashtable/HashTable.cpp
    operator/join/hashtable/OverlapsJoinHashTable.cpp
    operator/join/hashtable/PerfectJoinHashTable.cpp
    operator/join/hashtable/runtime/HashJoinRuntime.cpp
    operator/join/hashtable/RangeJoinHashTable.cpp
    LogicalIR.cpp
    LLVMFunctionAttributesUtil.cpp
    LLVMGlobalContext.cpp
    NativeCodegen.cpp
    OutputBufferInitialization.cpp
    PlanState.cpp
    QueryRewrite.cpp
    QueryTemplateGenerator.cpp
    QueryExecutionContext.cpp
    QueryMemoryInitializer.cpp
    ResultSet.cpp
    DynamicWatchdog.cpp
    ScalarCodeGenerator.cpp
    SpeculativeTopN.cpp
    StreamingTopN.cpp
    StringDictionaryGenerations.cpp
    TableGenerations.cpp
    TargetExprBuilder.cpp
    common/DiamondCodegen.cpp
    StringOpsIR.cpp
    RegexpFunctions.cpp
    WindowContext.cpp
    WindowExpressionRewrite.cpp
    WindowFunctionIR.cpp
    QueryPlanDagCache.cpp
    common/recycler/HashtableRecycler.cpp
    common/recycler/HashingSchemeRecycler.cpp
    common/recycler/OverlapsTuningParamRecycler.cpp
    ../operator/aggregate/CiderAggHashTable.cpp
    ../operator/aggregate/CiderAggSpillBufferMgr.cpp
    ../operator/aggregate/CiderAggTargetColExtractorBuilder.cpp
    Codec.h
    Execute.h
    QueryTemplateGenerator.h
    operator/aggregate/CiderAggregateCodeGenerator.cpp)

if(NOT PREFER_STATIC_LIBS)
  list(APPEND MAPD_DEFINITIONS "-DBOOST_LOG_DYN_LINK")
endif()

if(SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
  list(APPEND MAPD_DEFINITIONS "-DSUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS")
endif()

find_program(llvm_clangpp_cmd NAME clang++ HINTS ${LLVM_TOOLS_BINARY_DIR})

list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/CiderTypes.h
        ${CMAKE_CURRENT_BINARY_DIR}/RuntimeFunctions.bc
        ${CMAKE_CURRENT_BINARY_DIR}/ExtensionFunctions.ast
  DESTINATION QueryEngine
  COMPONENT "QE")

add_library(QueryEngine ${query_engine_source_files})

target_precompile_headers(QueryEngine PRIVATE [["exec/template/Execute.h"]])

find_package(FMT REQUIRED)

set(QUERY_ENGINE_LIBS cider_function cider_util cider_type_plan cider_type_data
                      cider_schema fmt::fmt)

list(APPEND QUERY_ENGINE_LIBS ${llvm_libs} ${ZLIB_LIBRARIES})

if(ENABLE_VELOX_FUNCTION)
  find_library(GLOG glog)
  find_package(gflags COMPONENTS shared)
  find_library(FMT fmt)
  find_library(EVENT event)
  find_package(folly REQUIRED)
  set(FOLLY_WITH_DEPENDENCIES ${FOLLY_LIBRARIES} ${FMT} ${GFLAGS_LIBRARIES}
                              ${GLOG_LIBRARIES} ${EVENT})
  list(APPEND QUERY_ENGINE_LIBS ${FOLLY_WITH_DEPENDENCIES})
endif()

target_link_libraries(QueryEngine memory ${QUERY_ENGINE_LIBS})
