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

#include <map>
#include <memory>
#include <string>

namespace cider_hashtable {

// To be added
// This enum is used for cider internal only
// Outside cider will need to define their own enum
enum hashtableName {
  // value type is INT, used for test
  LINEAR_PROBING_INT,
  // value type is BATCH, used for codegen
  LINEAR_PROBING_BATCH,
  LINEAR_PROBING_STRING,
  CHAINED_INT,
  CHAINED_BATCH
};

template <typename HashTableType_t>
class IHashTableRegistrar {
 public:
  virtual std::unique_ptr<HashTableType_t> createHashTable() = 0;

  virtual std::unique_ptr<HashTableType_t> createHashTable(int initial_size) = 0;

 protected:
  IHashTableRegistrar() {}
  virtual ~IHashTableRegistrar() {}

 private:
  IHashTableRegistrar(const IHashTableRegistrar&);
  const IHashTableRegistrar& operator=(const IHashTableRegistrar&);
};

template <typename HashTableType_t>
class HashTableFactory {
 public:
  static HashTableFactory<HashTableType_t>& Instance() {
    static HashTableFactory<HashTableType_t> instance;
    return instance;
  }

  void registerHashTable(IHashTableRegistrar<HashTableType_t>* registrar,
                         hashtableName name);

  // may add getHashTableForJoin and getHashTableForAgg
  template <typename... Args>
  std::unique_ptr<HashTableType_t> getHashTable(hashtableName name, Args&&... args);

 private:
  HashTableFactory() {}
  ~HashTableFactory() { m_HashTableRegistry.clear(); }

  HashTableFactory(const HashTableFactory&);
  const HashTableFactory& operator=(const HashTableFactory&);

  std::map<hashtableName, IHashTableRegistrar<HashTableType_t>*> m_HashTableRegistry;
};

template <typename HashTableType_t, typename HashTableImpl_t>
class HashTableRegistrar : public IHashTableRegistrar<HashTableType_t> {
 public:
  explicit HashTableRegistrar(hashtableName name) {
    HashTableFactory<HashTableType_t>::Instance().registerHashTable(this, name);
  }

  std::unique_ptr<HashTableType_t> createHashTable();

  std::unique_ptr<HashTableType_t> createHashTable(int initial_size);
};
}  // namespace cider_hashtable

// separate the implementations into cpp files instead of h file
// to isolate the implementation from codegen.
// use include cpp as a method to avoid maintaining too many template
// declaration in cpp file.
#include <exec/operator/join/HashTableFactory.cpp>