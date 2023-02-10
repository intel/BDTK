/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#define SHARED_LOGGER_H  // ignore util/Logger.h
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "Allocator.h"
#include "cider/CiderOptions.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "util/CiderBitUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// #include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This file refers velox/velox/benchmarks/basic/SimpleArithmetic.cpp
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(batch_size, 1'000, "batch size for one loop");
DEFINE_int64(loop_count, 2'000'000, "loop count for benchmark");
DEFINE_bool(dump_ir, false, "dump llvm ir");

using namespace cider::exec::processor;
using namespace cider::exec::nextgen::context;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::functions;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

namespace {

std::pair<ArrowArray*, ArrowSchema*> veloxVectorToArrow(RowVectorPtr vec,
                                                        MemoryPool* pool) {
  for (size_t i = 0; i < vec->childrenSize(); i++) {
    vec->childAt(i)->mutableRawNulls();
  }
  ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
  exportToArrow(vec, *inputArrowArray, pool);
  ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
  exportToArrow(vec, *inputArrowSchema);

  return {inputArrowArray, inputArrowSchema};
}

template <class T>
ArrowArray* allocArrowArray(int64_t length) {
  ArrowArray* array = new ArrowArray();

  array->n_buffers = 2;
  array->buffers = (const void**)std::malloc(sizeof(void*) * 2);
  size_t null_size = (length + 7) >> 3;
  void* null_buf = std::malloc(null_size);
  // void* null_buf = std::aligned_alloc(64, null_size);
  std::memset(null_buf, 0xFF, null_size);
  array->buffers[0] = null_buf;
  array->buffers[1] = std::malloc(sizeof(T) * length);
  // array->buffers[1] = std::aligned_alloc(64, sizeof(T) * length);

  return array;
}

void releaseArrowArray(ArrowArray* array) {
  free((void*)array->buffers[0]);
  free((void*)array->buffers[1]);
  free((void*)array->buffers);
  delete array;
}

class ArithmeticAndComparisonBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  using ArrowArrayReleaser = void (*)(struct ArrowArray*);
  using KernelType = void (*)(uint8_t* null_input1,
                              uint8_t* input1,
                              uint8_t* null_input2,
                              uint8_t* input2,
                              uint8_t* null_output,
                              uint8_t* output,
                              size_t n);

  explicit ArithmeticAndComparisonBenchmark(size_t vectorSize) : FunctionBenchmarkBase() {
    // registerAllScalarFunctions() just register checked version for integer
    // we register uncheck version for integer and checkedPlus for compare
    registerFunction<MultiplyFunction, int8_t, int8_t, int8_t>({"multiply"});
    registerFunction<MultiplyFunction, int16_t, int16_t, int16_t>({"multiply"});
    registerFunction<MultiplyFunction, int32_t, int32_t, int32_t>({"multiply"});
    registerFunction<MultiplyFunction, int64_t, int64_t, int64_t>({"multiply"});
    registerFunction<MultiplyFunction, double, double, double>({"multiply"});
    registerFunction<PlusFunction, int8_t, int8_t, int8_t>({"plus"});
    registerFunction<PlusFunction, int16_t, int16_t, int16_t>({"plus"});
    registerFunction<PlusFunction, int32_t, int32_t, int32_t>({"plus"});
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    registerFunction<PlusFunction, double, double, double>({"plus"});
    // compare with uncheck plus
    registerFunction<CheckedPlusFunction, int64_t, int64_t, int64_t>({"checkedPlus"});

    // for comparision
    registerBinaryScalar<EqFunction, bool>({"eq"});
    registerBinaryScalar<NeqFunction, bool>({"neq"});
    registerBinaryScalar<LtFunction, bool>({"lt"});
    registerBinaryScalar<GtFunction, bool>({"gt"});
    registerBinaryScalar<LteFunction, bool>({"lte"});
    registerBinaryScalar<GteFunction, bool>({"gte"});
    registerFunction<BetweenFunction, bool, double, double, double>({"btw"});

    // Set input schema.
    inputType_ = ROW({
        {"i8", TINYINT()},
        {"i16", SMALLINT()},
        {"i32", INTEGER()},
        {"i64", BIGINT()},
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"constant", DOUBLE()},
        {"d", BOOLEAN()},
        {"e", BOOLEAN()},
    });
    vectorSize_ = vectorSize;
    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.nullRatio = 0.5;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(TINYINT()));   // i8
    children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));  // i16
    children.emplace_back(fuzzer.fuzzFlat(INTEGER()));   // i32
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));    // i64
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // B
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // fake constant
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));   // D
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));   // E

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));

    ArrowSchema* _schema;
    std::tie(inputArray_, _schema) = veloxVectorToArrow(rowVector_, execCtx_.pool());
    // hack: make processor don't release inputArray_, otherwise we can't use inputArray_
    // multi times.
    inputReleaser_ = inputArray_->release;
    inputArray_->release = nullptr;
    _schema->release(_schema);
  }

  ~ArithmeticAndComparisonBenchmark() { inputReleaser_(inputArray_); }

  __attribute__((noinline)) size_t nextgenCompile(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    google::protobuf::Arena arena;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);
    suspender.dismiss();

    auto allocator = std::make_shared<CiderDefaultAllocator>();
    auto context = std::make_shared<BatchProcessorContext>(allocator);
    auto processor = makeBatchProcessor(plan, context);
    return 1;
  }

  __attribute__((noinline)) size_t nextgenCompute(const std::string& expression,
                                                  CodegenOptions cgo = CodegenOptions{}) {
    folly::BenchmarkSuspender suspender;
    google::protobuf::Arena arena;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);

    cgo.co.dump_ir = FLAGS_dump_ir;
    cgo.co.enable_vectorize = true;
    cgo.co.enable_avx2 = true;
    cgo.co.enable_avx512 = true;

    auto allocator = std::make_shared<CiderDefaultAllocator>();
    auto context = std::make_shared<BatchProcessorContext>(allocator);
    auto processor = makeBatchProcessor(plan, context, cgo);

    suspender.dismiss();

    size_t rows_size = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      processor->processNextBatch(inputArray_);

      struct ArrowArray output_array;
      struct ArrowSchema output_schema;

      processor->getResult(output_array, output_schema);
      rows_size += output_array.length;

      output_array.release(&output_array);
      output_schema.release(&output_schema);
    }

    return rows_size;
  }

  __attribute__((noinline)) size_t veloxCompute(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

  __attribute__((noinline)) size_t kernelCompute(KernelType kernel) {
    folly::BenchmarkSuspender suspender;
    ArrowArray* i8_array = inputArray_->children[0];
    uint8_t* null_input = (uint8_t*)i8_array->buffers[0];
    uint8_t* input = (uint8_t*)i8_array->buffers[1];
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      ArrowArray* output_array = allocArrowArray<uint8_t>(inputArray_->length);

      kernel(null_input,
             input,
             null_input,
             input,
             (uint8_t*)output_array->buffers[0],
             (uint8_t*)output_array->buffers[1],
             inputArray_->length);

      count += inputArray_->length;
      releaseArrowArray(output_array);
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
  ArrowArray* inputArray_;
  ArrowArrayReleaser inputReleaser_;
  size_t vectorSize_;
};

std::unique_ptr<ArithmeticAndComparisonBenchmark> benchmark;

bool isBitSet(uint8_t* null, size_t n) {
  return null[n >> 3] & 1 << (n & 0x7);
}

void setBitAt(uint8_t* __restrict null_output, size_t index, bool is_null) {
  uint8_t idx = index >> 3;
  uint8_t bit = index & 0x7;
  null_output[idx] = (null_output[idx] & ~(1 << bit)) | (is_null << bit);
}

// specialized func used to check performance upper limit
// two loop to mimic null separate in codegen currently.
// can't process kleene logic which null result depends on data
__attribute__((noinline)) void mulI8Specialized1(uint8_t* null_input1,
                                                 uint8_t* input1,
                                                 uint8_t* null_input2,
                                                 uint8_t* input2,
                                                 uint8_t* __restrict null_output,
                                                 uint8_t* __restrict output,
                                                 size_t n) {
  for (int i = 0; i < n; ++i) {
    output[i] = input1[i] * input2[i];
  }

  // null process, ignore extra bit in last uint8
  auto num = n / 8 + 1;
  for (int i = 0; i < num; ++i) {
    // 8bit packed
    null_output[i] = null_input1[i] & null_input2[i];
  }
}

// nested loop with null bit vector as bool vector
__attribute__((noinline)) void mulI8Specialized2(uint8_t* null_input1,
                                                 uint8_t* input1,
                                                 uint8_t* null_input2,
                                                 uint8_t* input2,
                                                 uint8_t* __restrict null_output,
                                                 uint8_t* __restrict output,
                                                 size_t n) {
  auto num = n / 8;
  //#pragma clang loop vectorize(enable)
  for (int i = 0; i < num; ++i) {
    for (int j = 0; j < 8; ++j) {
      auto idx = i * 8 + j;
      output[idx] = input1[idx] * input2[idx];
    }
    // 8bit packed
    null_output[i] = null_input1[i] & null_input2[i];
  }

  // tail
  int i = num * 8;
  while (i++ < n) {
    output[i] = input1[i] * input2[i];
    CiderBitUtils::setBitAtUnified(
        null_output, i, isBitSet(null_input1, i) && isBitSet(null_input2, i));
  }
}

// nested loop, outer process null
__attribute__((noinline)) void mulI8Specialized3(uint8_t* null_input1,
                                                 uint8_t* input1,
                                                 uint8_t* null_input2,
                                                 uint8_t* input2,
                                                 uint8_t* __restrict null_output,
                                                 uint8_t* __restrict output,
                                                 size_t n) {
  auto num = n / 8;
  //#pragma clang loop vectorize(enable)
  for (int i = 0; i < num; ++i) {
    bool tmp[8] = {false};
    for (int j = 0; j < 8; ++j) {
      auto idx = i * 8 + j;
      output[idx] = input1[idx] * input2[idx];
      tmp[j] = isBitSet(null_input1, idx) && isBitSet(null_input2, idx);
    }

    uint8_t packed = 0;
    for (int j = 0; j < 8; ++j) {
      packed |= tmp[j] << j;
    }
    null_output[i] = packed;
  }
  // tail
  int i = num * 8;
  while (i++ < n) {
    output[i] = input1[i] * input2[i];
    CiderBitUtils::setBitAtUnified(
        null_output, i, isBitSet(null_input1, i) && isBitSet(null_input2, i));
  }
}

// for profile
// auto profile_expr = "d AND e";
auto profile_expr = "i8 * i8";
BENCHMARK(velox) {
  benchmark->veloxCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen) {
  benchmark->nextgenCompute(profile_expr);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(nextgen____base) {
  benchmark->nextgenCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen_autorization) {
  // null separate
  CodegenOptions cgo;
  cgo.enable_vectorize = true;
  benchmark->nextgenCompute(profile_expr, cgo);
}
BENCHMARK_RELATIVE(specifialize1) {
  benchmark->kernelCompute(mulI8Specialized1);
}
BENCHMARK_RELATIVE(specifialize2) {
  benchmark->kernelCompute(mulI8Specialized2);
}
BENCHMARK_RELATIVE(specifialize3) {
  benchmark->kernelCompute(mulI8Specialized3);
}
BENCHMARK_DRAW_LINE();

#define BENCHMARK_GROUP(name, expr)                                        \
  BENCHMARK(name##Velox_________Base) { benchmark->veloxCompute(expr); }   \
  BENCHMARK_RELATIVE(name##NextGen) { benchmark->nextgenCompute(expr); }   \
  BENCHMARK(name##NextGen_______Base) { benchmark->nextgenCompute(expr); } \
  BENCHMARK_RELATIVE(name##NextgenIsBitClear) {                            \
    CodegenOptions cgo;                                                    \
    cgo.check_bit_vector_clear_opt = true;                                 \
    benchmark->nextgenCompute(expr, cgo);                                  \
  }                                                                        \
  BENCHMARK_RELATIVE(name##NextgenBranchlessSetNull) {                     \
    CodegenOptions cgo;                                                    \
    cgo.set_null_bit_vector_opt = true;                                    \
    benchmark->nextgenCompute(expr, cgo);                                  \
  }                                                                        \
  BENCHMARK_RELATIVE(name##NextgenVectorization) {                         \
    CodegenOptions cgo;                                                    \
    cgo.enable_vectorize = true;                                           \
    benchmark->nextgenCompute(expr, cgo);                                  \
  }                                                                        \
  BENCHMARK_RELATIVE(name##NextgenAllOpt) {                                \
    CodegenOptions cgo;                                                    \
    cgo.check_bit_vector_clear_opt = true;                                 \
    cgo.set_null_bit_vector_opt = true;                                    \
    cgo.enable_vectorize = true;                                           \
    benchmark->nextgenCompute(expr, cgo);                                  \
  }                                                                        \
  BENCHMARK(name##Compile) { benchmark->nextgenCompile(expr); }            \
  BENCHMARK_DRAW_LINE()

BENCHMARK_GROUP(mulI8, "multiply(i8, i8)");
BENCHMARK_GROUP(mulI8Nested, "i8*i8*i8");
BENCHMARK_GROUP(mulI8NestedDeep, "i8*i8*i8*i8");
BENCHMARK_GROUP(mulI16, "multiply(i16, i16)");
BENCHMARK_GROUP(mulI32, "multiply(i32, i32)");
BENCHMARK_GROUP(mulI64, "multiply(i64, i64)");

BENCHMARK_GROUP(mulDouble, "multiply(a, b)");
BENCHMARK_GROUP(mulDoubleSameColumn, "multiply(a, a)");
BENCHMARK_GROUP(mulDoubleConstant, "multiply(a, constant)");
BENCHMARK_GROUP(mulDoubleNested, "multiply(multiply(a, b), b)");
BENCHMARK_GROUP(mulDoubleNestedDeep,
                "multiply(multiply(multiply(a, b), a), "
                "multiply(a, multiply(a, b)))");

BENCHMARK(PlusCheckedVeloxI64) {
  benchmark->veloxCompute("checkedPlus(i64, i64)");
}
BENCHMARK_GROUP(plusI64, "plus(i64, i64)");

BENCHMARK_GROUP(mulAndAdd, "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0");

// comparison
BENCHMARK_GROUP(eq, "eq(a, b)");
BENCHMARK_GROUP(eqConstant, "eq(a, constant)");
BENCHMARK_GROUP(eqBool, "eq(d, e)");
BENCHMARK_GROUP(neq, "neq(a, b)");
BENCHMARK_GROUP(gt, "gt(a, b)");
BENCHMARK_GROUP(lt, "lt(a, b)");
BENCHMARK_GROUP(and, "d AND e");
BENCHMARK_GROUP(or, "d OR e");
BENCHMARK_GROUP(conjunctsNested,
                "(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))");
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<ArithmeticAndComparisonBenchmark>(FLAGS_batch_size);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
