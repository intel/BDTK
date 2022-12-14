/*
 * Copyright (c) 2022 Intel Corporation.
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

#if (defined(__GNUC__) && defined(__SANITIZE_THREAD__)) || defined(WITH_JIT_DEBUG)
#define ALWAYS_INLINE
#elif defined(ENABLE_CIDER)
#define ALWAYS_INLINE __attribute__((inline)) __attribute__((__visibility__("protected")))
#else
#define ALWAYS_INLINE __attribute__((always_inline))
#endif

#define NEVER_INLINE __attribute__((noinline))
#define SUFFIX(name) name

#ifdef _WIN32
#define RUNTIME_EXPORT __declspec(dllexport)
#else
#define RUNTIME_EXPORT
#endif
