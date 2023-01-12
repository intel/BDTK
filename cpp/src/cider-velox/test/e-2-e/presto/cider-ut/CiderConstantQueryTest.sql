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

SELECT true FROM cider_constant_query_test;
SELECT 1 FROM cider_constant_query_test;
SELECT true FROM cider_constant_query_test where col_a > 10;
SELECT true, col_b FROM cider_constant_query_test where col_a > 10;
SELECT 3 < 2 FROM cider_constant_query_test;
SELECT 3 > 2 FROM cider_constant_query_test;
SELECT 3 = 2 FROM cider_constant_query_test;
SELECT 3 <= 2 FROM cider_constant_query_test;
SELECT 3 >= 2 FROM cider_constant_query_test;
SELECT 3 <> 2 FROM cider_constant_query_test;
SELECT CAST(null AS boolean) FROM cider_constant_query_test;
SELECT NOT CAST(null AS boolean) FROM cider_constant_query_test;
SELECT CAST(null AS boolean) AND true FROM cider_constant_query_test;
SELECT CAST(null AS boolean) AND false FROM cider_constant_query_test;
SELECT CAST(null AS boolean) OR true FROM cider_constant_query_test;
SELECT CAST(null AS boolean) OR false FROM cider_constant_query_test;
SELECT null and true FROM cider_constant_query_test;
SELECT null and false FROM cider_constant_query_test;
SELECT null or true FROM cider_constant_query_test;
SELECT null or false FROM cider_constant_query_test;
SELECT col_a = 2 AND col_b > 10 FROM cider_constant_query_test;
SELECT col_a = 2 OR col_b > 10 FROM cider_constant_query_test;
