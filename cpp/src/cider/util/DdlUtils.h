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

#pragma once

#include "type/data/sqltypes.h"

namespace ddl_utils {
class SqlType {
 public:
  /**
   * Encapsulates column definition type information.
   *
   * @param type - Column type.
   * @param param1 - For column types followed by parenthesis with more information,
   * this represents the first parameter in the parenthesis. For example, in
   * DECIMAL(5, 2), this would represent the precision value of 5.
   * @param param2 - For column types followed by parenthesis with more information,
   * this represents the second parameter in the parenthesis. For example, in
   * DECIMAL(5, 2), this would represent the scale value of 2.
   * @param is_array - Flag that indicates whether or not column type is an array.
   * @param array_size - For array column types, this is the specified size of the array.
   */
  SqlType(SQLTypes type, int param1, int param2, bool is_array, int array_size);

  virtual SQLTypes get_type() const;
  virtual int get_param1() const;
  virtual void set_param1(int param);
  virtual int get_param2() const;
  virtual bool get_is_array() const;
  virtual void set_is_array(bool a);
  virtual int get_array_size() const;
  virtual void set_array_size(int s);
  virtual std::string to_string() const;
  virtual void check_type();

 protected:
  SQLTypes type;
  int param1;  // e.g. for NUMERIC(10).  -1 means unspecified.
  int param2;  // e.g. for NUMERIC(10,3). 0 is default value.
  bool is_array;
  int array_size;
};

class Encoding {
 public:
  /**
   * Encapsulates column definition encoding information.
   *
   * @param encoding_name - Type of encoding. For example, "DICT", "FIXED", etc.
   * @param encoding_param - Encoding size.
   */
  Encoding(std::string* encoding_name, int encoding_param);
  virtual ~Encoding() {}

  virtual const std::string* get_encoding_name() const;
  virtual int get_encoding_param() const;

 protected:
  std::unique_ptr<std::string> encoding_name;
  int encoding_param;
};

}  // namespace ddl_utils
