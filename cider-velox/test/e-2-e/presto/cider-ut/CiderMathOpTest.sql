-- Copyright (c) 2022 Intel Corporation.
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- SELECT 2 + 1 FROM cider_math_op_test;
-- SELECT 2 - 1 FROM cider_math_op_test;
-- SELECT 2 * 1 FROM cider_math_op_test;
-- SELECT 2 / 1 FROM cider_math_op_test;
-- SELECT 2 / 1 + 2 * 1 FROM cider_math_op_test;
-- SELECT 2 * 1 - 2 / 1 FROM cider_math_op_test;
-- SELECT 4 * 1.5 - 23 / 11 FROM cider_math_op_test;
-- SELECT 4 * 2 + 23 / 11 FROM cider_math_op_test;
-- SELECT 2 % 1 FROM cider_math_op_test;
SELECT tinyint_col + 2 FROM cider_math_op_test;
SELECT tinyint_col - 2 FROM cider_math_op_test;
SELECT tinyint_col * 2 FROM cider_math_op_test;
SELECT tinyint_col / 2 FROM cider_math_op_test;
SELECT tinyint_col % 2 FROM cider_math_op_test;
SELECT smallint_col + 2 FROM cider_math_op_test;
SELECT smallint_col - 2 FROM cider_math_op_test;
SELECT smallint_col * 2 FROM cider_math_op_test;
SELECT smallint_col / 2 FROM cider_math_op_test;
SELECT smallint_col % 2 FROM cider_math_op_test;
SELECT integer_col + 2 FROM cider_math_op_test;
SELECT integer_col - 2 FROM cider_math_op_test;
SELECT integer_col * 2 FROM cider_math_op_test;
SELECT integer_col / 2 FROM cider_math_op_test;
SELECT integer_col % 2 FROM cider_math_op_test;
SELECT bigint_col + 2 FROM cider_math_op_test;
SELECT bigint_col - 2 FROM cider_math_op_test;
SELECT bigint_col * 2 FROM cider_math_op_test;
SELECT bigint_col / 2 FROM cider_math_op_test;
SELECT bigint_col % 2 FROM cider_math_op_test;
SELECT float_col + 2 FROM cider_math_op_test;
SELECT float_col - 2 FROM cider_math_op_test;
SELECT float_col * 2 FROM cider_math_op_test;
SELECT float_col / 2 FROM cider_math_op_test;
SELECT double_col + 2 FROM cider_math_op_test;
SELECT double_col - 2 FROM cider_math_op_test;
SELECT double_col * 2 FROM cider_math_op_test;
SELECT double_col / 2 FROM cider_math_op_test;
SELECT double_col + float_col, double_col - float_col FROM cider_math_op_test;
-- SELECT integer_col * bigint_col, bigint_col / integer_col FROM cider_math_op_test where integer_col <> 0;
SELECT bigint_col * double_col + integer_col * float_col FROM cider_math_op_test where bigint_col * double_col > integer_col * float_col ;
SELECT bigint_col * double_col - integer_col * float_col FROM cider_math_op_test where bigint_col * double_col > integer_col * float_col ;
SELECT (double_col - float_col) * (integer_col - bigint_col) FROM cider_math_op_test where double_col > float_col and integer_col > bigint_col;
SELECT (double_col - float_col) / (integer_col - bigint_col) FROM cider_math_op_test where double_col > float_col and integer_col > bigint_col;
-- SELECT double_col + integer_col / bigint_col FROM cider_math_op_test where bigint_col <> 0;
SELECT float_col + double_col FROM cider_math_op_test;
SELECT double_col + 1.23e1, double_col - 1.23e1 FROM cider_math_op_test;
SELECT bigint_col * 1.23e1, bigint_col / 1.23e1 FROM cider_math_op_test;
SELECT 3 * (1.23e1 + bigint_col) / 2 FROM cider_math_op_test;
SELECT (double_col + 1.23e1) / 2 - 2.5e1 FROM cider_math_op_test;
SELECT integer_col + 0.123 FROM cider_math_op_test;
SELECT integer_col - 0.123 FROM cider_math_op_test;
SELECT integer_col * 0.123 FROM cider_math_op_test;
SELECT (integer_col + 0.8) / 2 FROM cider_math_op_test;
SELECT tinyint_col + 0.123 FROM cider_math_op_test;
SELECT smallint_col - 0.123 FROM cider_math_op_test;
SELECT double_col + 0.123 FROM cider_math_op_test;
SELECT double_col - 0.123 FROM cider_math_op_test;
SELECT double_col * 0.123 FROM cider_math_op_test;
SELECT double_col / 0.123 FROM cider_math_op_test;
SELECT bigint_col + 0.123 FROM cider_math_op_test;
