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

SELECT c0 FROM scalar_func_expr_test_1 WHERE c0 between 0 and 5;
SELECT c0 FROM scalar_func_expr_test_1 WHERE c0 between 0 and 5;

SELECT c0 FROM scalar_func_expr_test_2 WHERE c0 between 0 and 5;
SELECT c0 FROM scalar_func_expr_test_2 WHERE c0 between 0 and 5;

SELECT c0 FROM scalar_func_expr_test_3 WHERE c0 between 0 and 5;
SELECT c0 FROM scalar_func_expr_test_3 WHERE c0 between 0 and 5;

SELECT c0 FROM scalar_func_expr_test_4 WHERE c0 between 0 and 5;
SELECT c0 FROM scalar_func_expr_test_4 WHERE c0 between 0 and 5;

SELECT c0 FROM scalar_func_expr_test_5 WHERE c0 between 0.0 and 5.0;
SELECT c0 FROM scalar_func_expr_test_5 WHERE c0 between 0.0 and 5.0;
SELECT c0 FROM scalar_func_expr_test_5 WHERE c0 between 0.0 and 5.0;
SELECT c0 FROM scalar_func_expr_test_5 WHERE c0 between 0.0 and 5.0;