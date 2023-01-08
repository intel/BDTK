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
#include "cider/CiderException.h"

namespace cider_hashtable {

template <typename HashTableType_t, typename HashTableImpl_t>
std::unique_ptr<HashTableType_t>
HashTableRegistrar<HashTableType_t, HashTableImpl_t>::createHashTable() {
  return std::make_unique<HashTableImpl_t>();
}

template <typename HashTableType_t, typename HashTableImpl_t>
std::unique_ptr<HashTableType_t>
HashTableRegistrar<HashTableType_t, HashTableImpl_t>::createHashTable(int initial_size) {
  return std::make_unique<HashTableImpl_t>(initial_size);
}

template <typename HashTableType_t>
void HashTableFactory<HashTableType_t>::registerHashTable(
    IHashTableRegistrar<HashTableType_t>* registrar,
    hashtableName name) {
  m_HashTableRegistry[name] = registrar;
}

template <typename HashTableType_t>
template <typename... Args>
std::unique_ptr<HashTableType_t> HashTableFactory<HashTableType_t>::getHashTable(
    hashtableName name,
    Args&&... args) {
  if (m_HashTableRegistry.find(name) != m_HashTableRegistry.end()) {
    // todo: based on name, use different override createhashtable() function
    return m_HashTableRegistry[name]->createHashTable(std::forward<Args>(args)...);
  }
  CIDER_THROW(CiderRuntimeException, "No hashtable found for " + name);
  return NULL;
}
}  // namespace cider_hashtable
