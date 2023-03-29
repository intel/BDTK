/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#define STATIC_QUAL static
#define FORCE_INLINE inline __attribute__((always_inline))

#define RUNTIME_FUNC __attribute__((__visibility__("protected")))

#define ALWAYS_INLINE \
  __attribute__((always_inline)) __attribute__((__visibility__("protected")))

#define NEVER_INLINE __attribute__((noinline))

#define SUFFIX(name) name

#ifdef _WIN32
#define RUNTIME_EXPORT __declspec(dllexport)
#else
#define RUNTIME_EXPORT

#define DISABLE_AUTO_VECTORIZATION __attribute__((optimize("no-tree-vectorize")))

#define DISABLE_SSE __attribute__((target("no-sse,no-sse2,no-sse3,no-sse4")))
#define ENABLE_SSE __attribute__((target("sse,sse2,sse3,sse4")))

#define DISABLE_AVX256 __attribute__((target("no-avx,no-avx2")))
#define ENABLE_AVX256 __attribute__((target("avx,avx2")))

#if defined(__AVX512F__)
#define ENABLE_AVX512 __attribute__((target("avx512f")))
#define DISABLE_AVX512 __attribute__((target("no-avx512f")))
#endif

#endif
