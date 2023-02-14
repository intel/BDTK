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
#pragma once
#include <stdint.h>
#include "exec/operator/join/HashTableUtils.h"

namespace cider_hashtable {

// There are murmurhash functions declared in omnisci'codes: "function/hash/MurmurHash.h". 
// But omnisci's codes will be removed in future and most functions in that dir will not be used. 
// So redefine murmurhash function in this file
namespace hash_functions {

#define HASH_SEED 0xa4b3c7d6

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
#define	FORCE_INLINE inline __attribute__((always_inline))

FORCE_INLINE uint32_t murmurhash32(int32_t rawHash)
{   
    rawHash ^= rawHash >> 16;
    rawHash *= 0x85ebca6b;
    rawHash ^= rawHash >> 16;
    rawHash *= 0xc2b2ae35;
    rawHash ^= rawHash >> 16;
	return rawHash;
}

FORCE_INLINE uint64_t murmurhash64(int64_t rawHash)
{
    rawHash ^= unsigned(rawHash) >> 33;
    rawHash *= 0xff51afd7ed558ccdL;
    rawHash ^= unsigned(rawHash) >> 33;
    rawHash *= 0xc4ceb9fe1a85ec53L;
    rawHash ^= unsigned(rawHash) >> 33;
    return rawHash;
}

FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i )
{
  return p[i];
}

FORCE_INLINE uint32_t rotl32 ( uint32_t x, int8_t r )
{
  return (x << r) | (x >> (32 - r));
}

#define	ROTL32(x,y)	rotl32(x,y)

FORCE_INLINE uint32_t MurmurHash3_32 (const int8_t * key, int len,
                          uint32_t seed = HASH_SEED)
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;

  uint32_t h1 = seed;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;
  //----------
  // body
  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);
  for(int i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock32(blocks,i);
    k1 *= c1;
    k1 = ROTL32(k1,15);
    k1 *= c2;
    h1 ^= k1;
    h1 = ROTL32(h1,13); 
    h1 = h1*5+0xe6546b64;
  }

  //----------
  // tail

  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);
  uint32_t k1 = 0;
  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization
  h1 ^= len;
  h1 = murmurhash32(h1);
  return h1;
}

struct MurmurHash {
  uint32_t operator()(int32_t rawHash) {
    return murmurhash32(rawHash);
  }
  uint64_t operator()(int64_t rawHash) {
    return murmurhash64(rawHash);
  }
};

struct RowHash {
  uint64_t operator()(const int8_t* key, int32_t keysize)
{
	if( keysize == 4 ){
    return murmurhash32(*((const int32_t*) key));
  }
  else if( keysize == 8 ){
    return murmurhash64(*((const int64_t*) key));
  }else{
    // seed 0xa4b3c7d6
    return MurmurHash3_32( key, keysize, HASH_SEED);
  }
}

  uint64_t operator()(const HT_Row& ht_row)
{
	if( ht_row.key_size_ == 4 ){
    return murmurhash32(*((const int32_t*) ht_row.key_ptr_));
  }
  else if( ht_row.key_size_ == 8 ){
    return murmurhash64(*((const int64_t*) ht_row.key_ptr_));
  }else{
    // seed 0xa4b3c7d6
    return MurmurHash3_32( ht_row.key_ptr_, ht_row.key_size_, HASH_SEED);
  }
}
};
}

}  // namespace cider_hashtable