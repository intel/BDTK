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

#include <benchmark/benchmark.h>
#include <cstdlib>

#include "exec/nextgen/jitlib/JITLib.h"
#include "type/data/funcannotations.h"

using namespace cider::jitlib;

namespace basic_arithmetric {

// Arithmetric Test ---- Disable auto-vectorization

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        kernel(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        func_ptr(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME) \
  void basic_arith_##OPNAME##_cpp(benchmark::State& state) {        \
    basicArithmeticExecFunc<JITTYPE>(                               \
        state,                                                      \
        [](auto a, auto b, auto out, int64_t len)                   \
            NEVER_INLINE DISABLE_AUTO_VECTORIZATION -> auto {       \
              for (int64_t i = 0; i < len; ++i) {                   \
                out[i] = a[i] OP b[i];                              \
              }                                                     \
              return out[len - 1];                                  \
            });                                                     \
  }                                                                 \
  BENCHMARK(basic_arith_##OPNAME##_cpp)                             \
      ->RangeMultiplier(1 << 10)                                    \
      ->Range(1 << 10, 1 << 10);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_##OPNAME##_jit(benchmark::State& state) {                             \
    LLVMJITModule module(                                                                \
        std::string("basic_arith_") + #OPNAME + getJITTypeName(JITTYPE) + "jit",         \
        false,                                                                           \
        CompilationOptions{.aggressive_jit_compile = true,                               \
                           .dump_ir = true,                                              \
                           .enable_vectorize = false,                                    \
                           .enable_avx2 = false,                                         \
                           .enable_avx512 = false});                                     \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_##OPNAME##_jit)                                                  \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 10);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric

namespace basic_arithmetric_sse {

// Arithmetric Test ---- Enable SSE

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        kernel(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        func_ptr(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME) \
  void basic_arith_sse_##OPNAME##_cpp(benchmark::State& state) {    \
    basicArithmeticExecFunc<JITTYPE>(                               \
        state,                                                      \
        [](auto a, auto b, auto out, int64_t len)                   \
            NEVER_INLINE ENABLE_SSE DISABLE_AVX256 -> auto {        \
              for (int64_t i = 0; i < len; ++i) {                   \
                out[i] = a[i] OP b[i];                              \
              }                                                     \
              return out[len - 1];                                  \
            });                                                     \
  }                                                                 \
  BENCHMARK(basic_arith_sse_##OPNAME##_cpp)                         \
      ->RangeMultiplier(1 << 10)                                    \
      ->Range(1 << 10, 1 << 10);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_sse_##OPNAME##_jit(benchmark::State& state) {                         \
    LLVMJITModule module(                                                                \
        std::string("basic_arith_sse_") + #OPNAME + getJITTypeName(JITTYPE) + "jit",     \
        false,                                                                           \
        CompilationOptions{.aggressive_jit_compile = true,                               \
                           .dump_ir = true,                                              \
                           .enable_vectorize = true,                                     \
                           .enable_avx2 = false,                                         \
                           .enable_avx512 = false});                                     \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_sse_##OPNAME##_jit)                                              \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 10);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric_sse

namespace basic_arithmetric_avx256 {

// Arithmetric Test ---- Enable avx256

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        kernel(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        func_ptr(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                    \
  void basic_arith_avx256_##OPNAME##_cpp(benchmark::State& state) {                    \
    basicArithmeticExecFunc<JITTYPE>(                                                  \
        state,                                                                         \
        [](auto a, auto b, auto out, int64_t len) NEVER_INLINE ENABLE_AVX256 -> auto { \
          for (int64_t i = 0; i < len; ++i) {                                          \
            out[i] = a[i] OP b[i];                                                     \
          }                                                                            \
          return out[len - 1];                                                         \
        });                                                                            \
  }                                                                                    \
  BENCHMARK(basic_arith_avx256_##OPNAME##_cpp)                                         \
      ->RangeMultiplier(1 << 10)                                                       \
      ->Range(1 << 10, 1 << 25);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_avx256_##OPNAME##_jit(benchmark::State& state) {                      \
    LLVMJITModule module(                                                                \
        std::string("basic_arith_avx256_") + #OPNAME + getJITTypeName(JITTYPE) + "jit",  \
        false,                                                                           \
        CompilationOptions{.aggressive_jit_compile = true,                               \
                           .dump_ir = true,                                              \
                           .enable_vectorize = true,                                     \
                           .enable_avx2 = true,                                          \
                           .enable_avx512 = false});                                     \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_avx256_##OPNAME##_jit)                                           \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 25);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric_avx256

namespace basic_arithmetric_avx256_align {

// Arithmetric Test ---- Enable avx256

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  NativeT* a_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(a_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* b_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(b_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* out_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(out_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  for (auto _ : state) {
    benchmark::DoNotOptimize(kernel(a_col, b_col, out_col, state.range(0)));
  }

  std::free(a_col);
  std::free(b_col);
  std::free(out_col);
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  NativeT* a_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(a_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* b_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(b_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* out_col =
      (NativeT*)std::aligned_alloc(32, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(out_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  for (auto _ : state) {
    benchmark::DoNotOptimize(func_ptr(a_col, b_col, out_col, state.range(0)));
  }

  std::free(a_col);
  std::free(b_col);
  std::free(out_col);
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                    \
  void basic_arith_avx256_align_##OPNAME##_cpp(benchmark::State& state) {              \
    basicArithmeticExecFunc<JITTYPE>(                                                  \
        state,                                                                         \
        [](auto a, auto b, auto out, int64_t len) NEVER_INLINE ENABLE_AVX256 -> auto { \
          for (int64_t i = 0; i < len; ++i) {                                          \
            out[i] = a[i] OP b[i];                                                     \
          }                                                                            \
          return out[len - 1];                                                         \
        });                                                                            \
  }                                                                                    \
  BENCHMARK(basic_arith_avx256_align_##OPNAME##_cpp)                                   \
      ->RangeMultiplier(1 << 10)                                                       \
      ->Range(1 << 10, 1 << 25);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_avx256_align_##OPNAME##_jit(benchmark::State& state) {                \
    LLVMJITModule module(std::string("basic_arith_avx256_align_") + #OPNAME +            \
                             getJITTypeName(JITTYPE) + "jit",                            \
                         false,                                                          \
                         CompilationOptions{.aggressive_jit_compile = true,              \
                                            .dump_ir = true,                             \
                                            .enable_vectorize = true,                    \
                                            .enable_avx2 = true,                         \
                                            .enable_avx512 = false});                    \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_avx256_align_##OPNAME##_jit)                                     \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 25);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric_avx256_align

#if defined(__AVX512F__)
namespace basic_arithmetric_avx512 {

// Arithmetric Test ---- Enable avx512

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        kernel(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  std::vector<NativeT> a_col(state.range(0), 1);
  std::vector<NativeT> b_col(state.range(0), 1);
  std::vector<NativeT> out_col(state.range(0), 1);

  for (auto _ : state) {
    benchmark::DoNotOptimize(
        func_ptr(a_col.data(), b_col.data(), out_col.data(), state.range(0)));
  }
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                    \
  void basic_arith_avx512_##OPNAME##_cpp(benchmark::State& state) {                    \
    basicArithmeticExecFunc<JITTYPE>(                                                  \
        state,                                                                         \
        [](auto a, auto b, auto out, int64_t len) NEVER_INLINE ENABLE_AVX512 -> auto { \
          for (int64_t i = 0; i < len; ++i) {                                          \
            out[i] = a[i] OP b[i];                                                     \
          }                                                                            \
          return out[len - 1];                                                         \
        });                                                                            \
  }                                                                                    \
  BENCHMARK(basic_arith_avx512_##OPNAME##_cpp)                                         \
      ->RangeMultiplier(1 << 10)                                                       \
      ->Range(1 << 10, 1 << 25);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_avx512_##OPNAME##_jit(benchmark::State& state) {                      \
    LLVMJITModule module(                                                                \
        std::string("basic_arith_avx512_") + #OPNAME + getJITTypeName(JITTYPE) + "jit",  \
        false,                                                                           \
        CompilationOptions{.aggressive_jit_compile = true,                               \
                           .dump_ir = true,                                              \
                           .enable_vectorize = true,                                     \
                           .enable_avx2 = true,                                          \
                           .enable_avx512 = true});                                      \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_avx512_##OPNAME##_jit)                                           \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 25);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric_avx512

namespace basic_arithmetric_avx512_align {

// Arithmetric Test ---- Enable avx512

using JITLibExecKernel = std::pair<LLVMJITModule, JITFunctionPointer>;

template <JITTypeTag JITType, typename ExecKernel>
void basicArithmeticExecFunc(benchmark::State& state, ExecKernel&& kernel) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;

  NativeT* a_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(a_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* b_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(b_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* out_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(out_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  for (auto _ : state) {
    benchmark::DoNotOptimize(kernel(a_col, b_col, out_col, state.range(0)));
  }

  std::free(a_col);
  std::free(b_col);
  std::free(out_col);
}

template <JITTypeTag JITType>
void jitlibBasicArithmeticExecFunc(benchmark::State& state, JITFunctionPointer& func) {
  using NativeT = typename JITTypeTraits<JITType>::NativeType;
  auto func_ptr = func->template getFunctionPointer<int,
                                                    NativeT*,
                                                    NativeT*,
                                                    NativeT*,
                                                    int64_t>();  // a_col, b_col, out, len

  NativeT* a_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(a_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* b_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(b_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  NativeT* out_col =
      (NativeT*)std::aligned_alloc(64, state.range(0) * JITTypeTraits<JITType>::width);
  std::memset(out_col, 1, state.range(0) * JITTypeTraits<JITType>::width);

  for (auto _ : state) {
    benchmark::DoNotOptimize(func_ptr(a_col, b_col, out_col, state.range(0)));
  }

  std::free(a_col);
  std::free(b_col);
  std::free(out_col);
}

#define CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                    \
  void basic_arith_avx512_align_##OPNAME##_cpp(benchmark::State& state) {              \
    basicArithmeticExecFunc<JITTYPE>(                                                  \
        state,                                                                         \
        [](auto a, auto b, auto out, int64_t len) NEVER_INLINE ENABLE_AVX512 -> auto { \
          for (int64_t i = 0; i < len; ++i) {                                          \
            out[i] = a[i] OP b[i];                                                     \
          }                                                                            \
          return out[len - 1];                                                         \
        });                                                                            \
  }                                                                                    \
  BENCHMARK(basic_arith_avx512_align_##OPNAME##_cpp)                                   \
      ->RangeMultiplier(1 << 10)                                                       \
      ->Range(1 << 10, 1 << 25);

#define CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTYPE, OP, OPNAME)                      \
  void basic_arith_avx512_align_##OPNAME##_jit(benchmark::State& state) {                \
    LLVMJITModule module(std::string("basic_arith_avx512_align_") + #OPNAME +            \
                             getJITTypeName(JITTYPE) + "jit",                            \
                         false,                                                          \
                         CompilationOptions{.aggressive_jit_compile = true,              \
                                            .dump_ir = true,                             \
                                            .enable_vectorize = true,                    \
                                            .enable_avx2 = true,                         \
                                            .enable_avx512 = true});                     \
    auto func =                                                                          \
        JITFunctionBuilder()                                                             \
            .registerModule(module)                                                      \
            .setFuncName("BasicArithmetic_func")                                         \
            .addParameter(JITTypeTag::POINTER, "a", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "b", JITTYPE)                             \
            .addParameter(JITTypeTag::POINTER, "out", JITTYPE)                           \
            .addParameter(JITTypeTag::INT64, "len")                                      \
            .addProcedureBuilder([](const JITFunctionPointer& func) {                    \
              auto index = func->createVariable(JITTypeTag::INT64, "index", 0);          \
              func->createLoopBuilder()                                                  \
                  ->condition([&index, &func] {                                          \
                    auto len = func->getArgument(3);                                     \
                    return index < len;                                                  \
                  })                                                                     \
                  ->loop([&func, &index]() {                                             \
                    auto out = func->getArgument(2);                                     \
                    auto a = func->getArgument(0);                                       \
                    auto b = func->getArgument(1);                                       \
                    out[index] = a[index] OP b[index];                                   \
                  })                                                                     \
                  ->update([&index]() { index = index + 1; })                            \
                  ->build();                                                             \
              return func->createReturn(func->getArgument(2)[func->getArgument(3) - 1]); \
            })                                                                           \
            .addReturn(JITTYPE)                                                          \
            .build();                                                                    \
    module.finish();                                                                     \
    jitlibBasicArithmeticExecFunc<JITTYPE>(state, func);                                 \
  }                                                                                      \
  BENCHMARK(basic_arith_avx512_align_##OPNAME##_jit)                                     \
      ->RangeMultiplier(1 << 10)                                                         \
      ->Range(1 << 10, 1 << 25);

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, +, sum_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, +, sum_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, +, sum_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, +, sum_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, +, sum_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, +, sum_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, -, sub_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, -, sub_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, -, sub_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, -, sub_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, -, sub_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, -, sub_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, *, mul_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, *, mul_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, *, mul_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, *, mul_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, *, mul_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, *, mul_double)

CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT8, /, div_i8)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT16, /, div_i16)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT32, /, div_i32)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::INT64, /, div_i64)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::FLOAT, /, div_float)
CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)
CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK(JITTypeTag::DOUBLE, /, div_double)

#undef CREATE_CPP_BASIC_ARITHMETRIC_BENCHMARK
#undef CREATE_JIT_BASIC_ARITHMETRIC_BENCHMARK
};  // namespace basic_arithmetric_avx512_align
#endif
BENCHMARK_MAIN();
