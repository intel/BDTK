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

#ifndef EXPR_TYPE_H
#define EXPR_TYPE_H

enum OpSupportExprType {
  kCOLUMN_VAR = 0,
  kEXPRESSION_TUPLE = 1,
  kCONSTANT = 2,
  kU_OPER = 3,
  kBIN_OPER = 4,
  kRANGE_OPER = 5,
  kSUBQUERY = 6,
  kIN_VALUES = 7,
  kIN_INTEGER_SET = 8,
  kCHAR_LENGTH_OPER = 9,
  kKEY_FOR_STRING_EXPR = 10,
  kSAMPLE_RATIO_EXPR = 11,
  kLOWER_EXPR = 12,
  kCARDINALITY_EXPR = 13,
  kLIKE_EXPR = 14,
  kREGEXP_EXPR = 15,
  kWIDTH_BUCKET_EXPR = 16,
  kLIKELIHOOD_EXPR = 17,
  kAGG_EXPR = 18,
  kCASE_EXPR = 19,
  kEXTRACT_EXPR = 20,
  kDATEADD_EXPR = 21,
  kDATEDIFF_EXPR = 22,
  kDATETRUNC_EXPR = 23,
  kSTRING_OPER = 24,
  kLOWER_STRING_OPER = 25,
  kFUNCTION_OPER = 26,
  kOFFSET_IN_FRAGMENT = 27,
  kWINDOW_FUNCTION = 28,
  kARRAY_EXPR = 29,
  kIS_NOT_NULL = 30,
  kSUBSTRING_STRING_OPER = 31,
  kUPPER_STRING_OPER = 32,
  kTRIM_STRING_OPER = 33,
  kCONCAT_STRING_OPER = 34,
  kREGEXP_REPLACE_OPER = 35,
  kSPLIT_PART_OPER = 36,
  kSTRING_SPLIT_OPER = 37,
  kREGEXP_EXTRACT_OPER = 38,
  kREGEXP_SUBSTR_OPER = 39,
  kUNDEFINED_EXPR = -1,
};

#endif
