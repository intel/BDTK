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
template <class HashTableType_t>
class IHashTableRegistrar {
 public:
  virtual std::unique_ptr<HashTableType_t> createHashTable() = 0;

 protected:
  IHashTableRegistrar() {}
  virtual ~IHashTableRegistrar() {}

 private:
  IHashTableRegistrar(const IHashTableRegistrar&);
  const IHashTableRegistrar& operator=(const IHashTableRegistrar&);
};

template <class HashTableType_t>
class HashTableFactory {
 public:
  static HashTableFactory<HashTableType_t>& Instance() {
    static HashTableFactory<HashTableType_t> instance;
    return instance;
  }

  void registerHashTable(IHashTableRegistrar<HashTableType_t>* registrar,
                         std::string name);

  std::unique_ptr<HashTableType_t> getHashTable(std::string name);

 private:
  HashTableFactory() {}
  ~HashTableFactory() { m_HashTableRegistry.clear(); }

  HashTableFactory(const HashTableFactory&);
  const HashTableFactory& operator=(const HashTableFactory&);

  std::map<std::string, IHashTableRegistrar<HashTableType_t>*> m_HashTableRegistry;
};

template <class HashTableType_t, class HashTableImpl_t>
class HashTableRegistrar : public IHashTableRegistrar<HashTableType_t> {
 public:
  explicit HashTableRegistrar(std::string name) {
    HashTableFactory<HashTableType_t>::Instance().registerHashTable(this, name);
  }

  std::unique_ptr<HashTableType_t> createHashTable();
};
}  // namespace cider_hashtable
#include <exec/operator/join/HashTableFactory.cpp>