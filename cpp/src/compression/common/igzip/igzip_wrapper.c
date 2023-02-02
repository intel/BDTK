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

#include "common/igzip/igzip_wrapper.h"

#include <stdio.h>
#include <stdlib.h>

#include "isa-l/igzip_lib.h"

static int level_size_buf[] = {
#ifdef ISAL_DEF_LVL0_DEFAULT
    ISAL_DEF_LVL0_DEFAULT,
#else
    0,
#endif
#ifdef ISAL_DEF_LVL1_DEFAULT
    ISAL_DEF_LVL1_DEFAULT,
#else
    0,
#endif
#ifdef ISAL_DEF_LVL2_DEFAULT
    ISAL_DEF_LVL2_DEFAULT,
#else
    0,
#endif
#ifdef ISAL_DEF_LVL3_DEFAULT
    ISAL_DEF_LVL3_DEFAULT,
#else
    0,
#endif
};

void* igzip_wrapper_init(int compression_level) {
  igzip_wrapper_context_t* context =
      (igzip_wrapper_context_t*)calloc(1, sizeof(igzip_wrapper_context_t));
  if (context) {
    if (compression_level < ISAL_DEF_MIN_LEVEL ||
        compression_level > ISAL_DEF_MAX_LEVEL) {
      compression_level = ISAL_DEF_MIN_LEVEL;
    }
    context->compression_level = compression_level;
    context->level_buf_size = level_size_buf[compression_level];
    if (context->level_buf_size > 0) {
      context->level_buf = (uint8_t*)malloc(context->level_buf_size);
    } else {
      context->level_buf = NULL;
    }
  }

  return context;
}

void igzip_wrapper_destroy(void* context) {
  igzip_wrapper_context_t* igzip_wrapper_context = (igzip_wrapper_context_t*)context;
  if (igzip_wrapper_context) {
    if (igzip_wrapper_context->level_buf) {
      free(igzip_wrapper_context->level_buf);
      igzip_wrapper_context->level_buf = NULL;
    }
    free(igzip_wrapper_context);
    igzip_wrapper_context = NULL;
  }
}

int64_t igzip_wrapper_compress(void* context,
                               int64_t input_length,
                               const uint8_t* input,
                               int64_t output_length,
                               uint8_t* output) {
  igzip_wrapper_context_t* igzip_wrapper_context = (igzip_wrapper_context_t*)context;

  struct isal_zstream stream;

  isal_deflate_stateless_init(&stream);

  stream.end_of_stream = 1; /* Do the entire file at once */
  stream.flush = NO_FLUSH;
  stream.next_in = input;
  stream.avail_in = input_length;
  stream.next_out = output;
  stream.avail_out = output_length;
  stream.level = igzip_wrapper_context->compression_level;
  stream.level_buf = igzip_wrapper_context->level_buf;
  stream.level_buf_size = igzip_wrapper_context->level_buf_size;
  int ret = isal_deflate_stateless(&stream);
  if (ret != COMP_OK) {
    if (ret == STATELESS_OVERFLOW) {
      fprintf(stderr, "IGZIP deflate: output buffer will not fit output\n");
      return -1;
    } else if (ret == INVALID_FLUSH) {
      fprintf(stderr, "IGZIP deflate: an invalid FLUSH is selected\n");
      return -1;
    } else if (ret == ISAL_INVALID_LEVEL) {
      fprintf(stderr, "IGZIP deflate: an invalid compression level is selected\n");
      return -1;
    }
  }
  if (stream.avail_in != 0) {
    fprintf(stderr, "IGZIP: Could not compress all of inbuf\n");
    return -1;
  }

  return stream.total_out;
}

int64_t igzip_wrapper_decompress(void* context,
                                 int64_t input_length,
                                 const uint8_t* input,
                                 int64_t output_length,
                                 uint8_t* output) {
  igzip_wrapper_context_t* igzip_wrapper_context = (igzip_wrapper_context_t*)context;

  struct inflate_state state;
  isal_inflate_init(&state);

  state.next_in = input;
  state.avail_in = input_length;
  state.next_out = output;
  state.avail_out = output_length;

  int ret = isal_inflate_stateless(&state);
  if (ret != ISAL_DECOMP_OK) {
    if (ret == ISAL_END_INPUT) {
      fprintf(stderr, "isal_inflate_stateless: End of input reached\n");
    } else if (ret == ISAL_OUT_OVERFLOW) {
      fprintf(stderr, "isal_inflate_stateless: output buffer ran out of space\n");
    } else if (ret == ISAL_INVALID_BLOCK) {
      fprintf(stderr, "isal_inflate_stateless: Invalid deflate block found\n");
    } else if (ret == ISAL_INVALID_SYMBOL) {
      fprintf(stderr, "isal_inflate_stateless: Invalid deflate symbol found\n");
    } else if (ret == ISAL_INVALID_LOOKBACK) {
      fprintf(stderr, "isal_inflate_stateless: Invalid lookback distance found\n");
    }
    return -1;
  }

  return state.total_out;
}

int64_t igzip_wrapper_max_compressed_len(int64_t input_length, const uint8_t* input) {
  return input_length * 2 + 1024;
}

int igzip_wrapper_minimum_compression_level() {
  return ISAL_DEF_MIN_LEVEL;
}

int igzip_wrapper_maximum_compression_level() {
  return ISAL_DEF_MAX_LEVEL;
}

int igzip_wrapper_default_compression_level() {
  return ISAL_DEF_MIN_LEVEL;
}
