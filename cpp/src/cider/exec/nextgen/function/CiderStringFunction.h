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
#ifndef NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H
#define NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H

#include <cstdint>

extern "C" int8_t convert_string_to_tinyint(const char* str_ptr, const int32_t str_len);

extern "C" int16_t convert_string_to_smallint(const char* str_ptr, const int32_t str_len);

extern "C" int32_t convert_string_to_int(const char* str_ptr, const int32_t str_len);

extern "C" int64_t convert_string_to_bigint(const char* str_ptr, const int32_t str_len);

extern "C" float convert_string_to_float(const char* str_ptr, const int32_t str_len);

extern "C" int8_t convert_string_to_bool(const char* str_ptr, const int32_t str_len);

extern "C" double convert_string_to_double(const char* str_ptr, const int32_t str_len);

extern "C" int32_t convert_string_to_date(const char* str_ptr, const int32_t str_len);

extern "C" int64_t convert_string_to_timestamp(const char* str_ptr,
                                               const int32_t str_len,
                                               const int32_t dim);

extern "C" int64_t convert_string_to_time(const char* str_ptr,
                                          const int32_t str_len,
                                          const int32_t dim);
#endif  // NEXTGEN_FUNCTION_CIDERSTRINGFUNCTION_H
