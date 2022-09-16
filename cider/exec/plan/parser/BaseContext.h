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

/**
 * @file    BaseContext.h
 * @brief   Base context element
 **/

#pragma once

#include <memory>

namespace generator {

class RelVisitor;
class GeneratorContext;

enum ContextElementType {
  InputDescContextType,
  GroupbyContextType,
  JoinQualContextType,
  TargetContextType,
  FilterQualContextType,
  OrderEntryContextType
};

class BaseContext {
 public:
  virtual ~BaseContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) = 0;
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr) = 0;
};

}  // namespace generator
