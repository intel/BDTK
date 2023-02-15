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

/**
 * @file    Translator.h
 * @brief
 **/
#ifndef TRANSLATOR_H
#define TRANSLATOR_H

#include <memory>
#include <vector>
#include "type/plan/Analyzer.h"

// Migrated from RelAlgTranslator.h
struct QualsConjunctiveForm {
  const std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  const std::list<std::shared_ptr<Analyzer::Expr>> quals;
};

QualsConjunctiveForm qual_to_conjunctive_form(
    const std::shared_ptr<Analyzer::Expr> qual_expr);

std::vector<std::shared_ptr<Analyzer::Expr>> qual_to_disjunctive_form(
    const std::shared_ptr<Analyzer::Expr>& qual_expr);

#endif  // TRANSLATOR_H
