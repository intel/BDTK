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

SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_2;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_2;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_2;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_2;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_2;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_2;
-- SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_2 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_2 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_2;
SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_3;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_3;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_3;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_3;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_3;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_3;
-- SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_3 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_3 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_3;
SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_1;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_1;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_1;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_1 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_1 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_1;
SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_1;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_1;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_1;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_1;
SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_1 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_1 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_1;
SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_4;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_4;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_4;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_4;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_4;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_4;
SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_4 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_4 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_4;
SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM cider_cast_test_5;
SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM cider_cast_test_5;
SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM cider_cast_test_5;
SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM cider_cast_test_5;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_5;
SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM cider_cast_test_5;
SELECT CAST(col_a as DOUBLE) FROM cider_cast_test_5 where CAST(col_b as INTEGER) > 20 ;
SELECT CAST(col_a as INTEGER), count(col_b) FROM cider_cast_test_5 GROUP BY col_a;
SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM cider_cast_test_5;
SELECT CAST(col_4 as TINYINT) FROM cider_cast_test_6;
SELECT CAST(col_4 as INTEGER) FROM cider_cast_test_6;
