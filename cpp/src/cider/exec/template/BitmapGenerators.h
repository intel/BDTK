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
#ifndef BITMAP_GENERATORS_h
#define BITMAP_GENERATORS_h

#include <cstddef>
#include <cstdint>

// IMPORTANT NOTE: All function,  generating bitmaps, assume that the
//  allocated `*src' memory has size that is multiple of 64 bytes.

size_t gen_null_bitmap_8(uint8_t* bitmap,
                         const uint8_t* data,
                         size_t size,
                         const uint8_t null_val);
size_t gen_null_bitmap_16(uint8_t* bitmap,
                          const uint16_t* data,
                          size_t size,
                          const uint16_t null_val);
size_t gen_null_bitmap_32(uint8_t* bitmap,
                          const uint32_t* data,
                          size_t size,
                          const uint32_t null_val);
size_t gen_null_bitmap_64(uint8_t* bitmap,
                          const uint64_t* data,
                          size_t size,
                          const uint64_t null_val);
#endif