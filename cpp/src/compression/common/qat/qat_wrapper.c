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

#include "common/qat/qat_wrapper.h"

#include <dlfcn.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <qatzip.h>

typedef int (*dlsym_qzCompress)(QzSession_T* sess,
                                const unsigned char* src,
                                unsigned int* src_len,
                                unsigned char* dest,
                                unsigned int* dest_len,
                                unsigned int last);
typedef int (*dlsym_qzDecompress)(QzSession_T* sess,
                                  const unsigned char* src,
                                  unsigned int* src_len,
                                  unsigned char* dest,
                                  unsigned int* dest_len);
typedef int (*dlsym_qzGetDefaults)(QzSessionParams_T* defaults);
typedef int (*dlsym_qzSetDefaults)(QzSessionParams_T* defaults);
typedef unsigned int (*dlsym_qzMaxCompressedLength)(unsigned int src_sz,
                                                    QzSession_T* sess);

typedef struct qatzip_wrapper_context {
  dlsym_qzCompress compress;
  dlsym_qzDecompress decompress;
  dlsym_qzGetDefaults getDefaults;
  dlsym_qzSetDefaults setDefaults;
  dlsym_qzMaxCompressedLength maxCompressedLength;
} qatzip_wrapper_context_t;

static qatzip_wrapper_context_t s_qatzip_wrapper_context;

static void qatzip_init(void) {
  const char* library = "libqatzip.so";
  qatzip_wrapper_context_t* qatzip_wrapper_context = &s_qatzip_wrapper_context;
  void* lib = dlopen(library, RTLD_LAZY | RTLD_GLOBAL);
  if (!lib) {
    fprintf(stderr, "Can't load %s due to %s", library, dlerror());
    goto Out;
  }

  dlerror();  // Clear any existing error

  qatzip_wrapper_context->compress = dlsym(lib, "qzCompress");
  if (qatzip_wrapper_context->compress == NULL) {
    fprintf(stderr, "Failed to load qzCompress\n");
    goto Out;
  }

  qatzip_wrapper_context->decompress = dlsym(lib, "qzDecompress");
  if (qatzip_wrapper_context->compress == NULL) {
    fprintf(stderr, "Failed to load qzCompress\n");
    goto Out;
  }

  qatzip_wrapper_context->getDefaults = dlsym(lib, "qzGetDefaults");
  if (qatzip_wrapper_context->getDefaults == NULL) {
    fprintf(stderr, "Failed to load qzCompress\n");
    goto Out;
  }

  qatzip_wrapper_context->setDefaults = dlsym(lib, "qzSetDefaults");
  if (qatzip_wrapper_context->setDefaults == NULL) {
    fprintf(stderr, "Failed to load qzCompress\n");
    goto Out;
  }

  qatzip_wrapper_context->maxCompressedLength = dlsym(lib, "qzMaxCompressedLength");
  if (qatzip_wrapper_context->setDefaults == NULL) {
    fprintf(stderr, "Failed to load qzMaxCompressedLength\n");
    goto Out;
  }

Out:
  return;
}

int qatzip_initialized = 0;
pthread_once_t once_qatzip_initialized = PTHREAD_ONCE_INIT;

__thread QzSession_T g_qzSession = {
    .internal = NULL,
};

void* qat_wrapper_init(int compression_level) {
  int status = pthread_once(&once_qatzip_initialized, qatzip_init);
  if (status < 0) {
    fprintf(stderr, "pthread_once failed\n");
    return NULL;
  }
  qat_wrapper_context_t* context =
      (qat_wrapper_context_t*)calloc(1, sizeof(qat_wrapper_context_t));
  if (context) {
    if (compression_level < QZ_DEFLATE_COMP_LVL_MINIMUM ||
        compression_level > QZ_DEFLATE_COMP_LVL_MAXIMUM) {
      compression_level = QZ_COMP_LEVEL_DEFAULT;
    }
    context->compression_level = compression_level;
    context->private_data = &s_qatzip_wrapper_context;
  }

  return context;
}

void qat_wrapper_destroy(void* context) {
  qat_wrapper_context_t* qat_wrapper_context = (qat_wrapper_context_t*)context;
  if (qat_wrapper_context) {
    free(qat_wrapper_context);
    qat_wrapper_context = NULL;
  }
}

int64_t qat_wrapper_compress(void* context,
                             int64_t input_length,
                             const uint8_t* input,
                             int64_t output_length,
                             uint8_t* output) {
  qat_wrapper_context_t* qat_wrapper_context = (qat_wrapper_context_t*)context;
  qatzip_wrapper_context_t* qatzip_wrapper_context =
      (qatzip_wrapper_context_t*)(qat_wrapper_context->private_data);
  unsigned int uncompressed_size = (unsigned int)input_length;
  unsigned int compressed_size = (unsigned int)output_length;
  int32_t status = qatzip_wrapper_context->compress(
      &g_qzSession, input, &uncompressed_size, output, &compressed_size, 1);
  if (status == QZ_OK) {
    return compressed_size;
  } else if (status == QZ_PARAMS) {
    fprintf(stderr, "QAT compress failed due to params is invalid\n");
  } else if (status == QZ_FAIL) {
    fprintf(stderr, "QAT compress failed due to Function did not succeed\n");
  } else {
    fprintf(stderr, "QAT compress failed with error code %d\n", status);
  }

  return 0;
}

int64_t qat_wrapper_decompress(void* context,
                               int64_t input_length,
                               const uint8_t* input,
                               int64_t output_length,
                               uint8_t* output) {
  qat_wrapper_context_t* qat_wrapper_context = (qat_wrapper_context_t*)context;
  qatzip_wrapper_context_t* qatzip_wrapper_context =
      (qatzip_wrapper_context_t*)(qat_wrapper_context->private_data);
  unsigned int compressed_size = (unsigned int)input_length;
  unsigned int uncompressed_size = (unsigned int)output_length;
  int32_t status = qatzip_wrapper_context->decompress(
      &g_qzSession, input, &compressed_size, output, &uncompressed_size);
  if (status == QZ_OK) {
    return uncompressed_size;
  } else if (status == QZ_PARAMS) {
    fprintf(stderr, "QAT decompress failed due to params is invalid\n");
  } else if (status == QZ_FAIL) {
    fprintf(stderr, "QAT decompress failed due to Function did not succeed\n");
  } else {
    fprintf(stderr, "QAT decompress failed with error code %d\n", status);
  }

  return 0;
}

int64_t qat_wrapper_max_compressed_len(void* context,
                                       int64_t input_length,
                                       const uint8_t* input) {
  qat_wrapper_context_t* qat_wrapper_context = (qat_wrapper_context_t*)context;
  qatzip_wrapper_context_t* qatzip_wrapper_context =
      (qatzip_wrapper_context_t*)(qat_wrapper_context->private_data);
  return qatzip_wrapper_context->maxCompressedLength(input_length, &g_qzSession);
}

int qat_wrapper_minimum_compression_level() {
  return QZ_DEFLATE_COMP_LVL_MINIMUM;
}

int qat_wrapper_maximum_compression_level() {
  return QZ_DEFLATE_COMP_LVL_MAXIMUM;
}

int qat_wrapper_default_compression_level() {
  return QZ_COMP_LEVEL_DEFAULT;
}
