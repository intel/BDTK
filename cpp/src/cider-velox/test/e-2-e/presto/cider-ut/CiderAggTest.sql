-- Copyright(c) 2022-2023 Intel Corporation.
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

SELECT SUM(col_i8) FROM cider_agg_test_1;
SELECT SUM(col_i16) FROM cider_agg_test_1;
SELECT SUM(col_i32) FROM cider_agg_test_1;
SELECT SUM(col_i64) FROM cider_agg_test_1;
SELECT SUM(col_fp32) FROM cider_agg_test_1;
SELECT SUM(col_fp64) FROM cider_agg_test_1;
SELECT SUM(half_null_i8) FROM cider_agg_test_1;
SELECT SUM(half_null_i8) FROM cider_agg_test_1 where half_null_i8 IS NOT NULL;
SELECT SUM(half_null_i16) FROM cider_agg_test_1;
SELECT SUM(half_null_i16) FROM cider_agg_test_1 where half_null_i16 IS NOT NULL;
SELECT SUM(half_null_i32) FROM cider_agg_test_1;
SELECT SUM(half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i64) FROM cider_agg_test_1;
SELECT SUM(half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
SELECT SUM(half_null_fp32) FROM cider_agg_test_1;
SELECT SUM(half_null_fp32) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL;
SELECT SUM(half_null_fp64) FROM cider_agg_test_1;
SELECT SUM(half_null_fp64) FROM cider_agg_test_1 where half_null_fp64 IS NOT NULL;

SELECT col_i32, COUNT(col_i32) FROM cider_agg_test_1 GROUP BY col_i32;
SELECT half_null_fp32, COUNT(*) FROM cider_agg_test_1 GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(*) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(1) FROM cider_agg_test_1 GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(1) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(half_null_fp32) FROM cider_agg_test_1 GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(half_null_fp32) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT COUNT(*) FROM cider_agg_test_1;
SELECT COUNT(*), MIN(half_null_fp64) FROM cider_agg_test_1;
SELECT half_null_fp32, COUNT(*) FROM cider_agg_test_1 GROUP BY half_null_fp32;
SELECT COUNT(1) FROM cider_agg_test_1;
-- SELECT COUNT(1), MAX(col_i8) FROM cider_agg_test_1;
SELECT half_null_fp32, COUNT(1) FROM cider_agg_test_1 GROUP BY half_null_fp32;
SELECT COUNT(col_i32) FROM cider_agg_test_1;
SELECT COUNT(col_i32) FROM cider_agg_test_1 where col_i32 IS NOT NULL;
SELECT COUNT(col_i64) FROM cider_agg_test_1;
SELECT COUNT(col_i64) FROM cider_agg_test_1 where col_i64 IS NOT NULL;
SELECT COUNT(col_fp32) FROM cider_agg_test_1;
SELECT COUNT(col_fp32) FROM cider_agg_test_1 where col_fp32 IS NOT NULL;
SELECT COUNT(col_fp64) FROM cider_agg_test_1;
SELECT COUNT(col_fp64) FROM cider_agg_test_1 where col_fp64 IS NOT NULL;
SELECT COUNT(half_null_i32) FROM cider_agg_test_1;
SELECT COUNT(half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT COUNT(half_null_i64) FROM cider_agg_test_1;
SELECT COUNT(half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
SELECT COUNT(half_null_fp32) FROM cider_agg_test_1;
SELECT COUNT(half_null_fp32) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL;
SELECT COUNT(half_null_fp64) FROM cider_agg_test_1;
SELECT COUNT(half_null_fp64) FROM cider_agg_test_1 where half_null_fp64 IS NOT NULL;
SELECT COUNT(half_null_i16) FROM cider_agg_test_1;
SELECT COUNT(half_null_i16) FROM cider_agg_test_1 where half_null_i16 IS NOT NULL;
SELECT COUNT(half_null_i8) FROM cider_agg_test_1;
SELECT COUNT(half_null_i8) FROM cider_agg_test_1 where half_null_i8 IS NOT NULL;

SELECT COUNT(DISTINCT col_i8) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT col_i16) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT col_i32) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT col_i64) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT half_null_i8) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT half_null_i8) FROM cider_agg_test_1 where half_null_i8 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i16) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT half_null_i16) FROM cider_agg_test_1 where half_null_i16 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i32) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1;
SELECT COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
-- SELECT SUM(col_i8), COUNT(DISTINCT col_i8) FROM cider_agg_test_1;
-- SELECT SUM(col_i16), COUNT(DISTINCT col_i16) FROM cider_agg_test_1;
-- SELECT SUM(col_i32), COUNT(DISTINCT col_i32) FROM cider_agg_test_1;
-- SELECT SUM(col_i64), COUNT(DISTINCT col_i64) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM cider_agg_test_1 where half_null_i8 IS NOT NULL;
-- SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM cider_agg_test_1 where half_null_i16 IS NOT NULL;
-- SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
-- SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
-- SELECT SUM(col_i32), COUNT(DISTINCT col_i32), COUNT(DISTINCT col_i64) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1;
-- SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT half_null_i64) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL AND half_null_i64 IS NOT NULL;
SELECT COUNT(DISTINCT col_i32) FROM cider_agg_test_1 GROUP BY col_i8;

-- SELECT MIN(col_i8), MIN(col_i64), MIN(col_fp32), MIN(col_fp64) FROM cider_agg_test_1;
-- SELECT MIN(half_null_i32), MIN(half_null_i64), MIN(half_null_fp32), MIN(half_null_fp64) FROM cider_agg_test_1;
-- SELECT MIN(col_i8) FROM cider_agg_test_1;
-- SELECT MIN(col_i8) FROM cider_agg_test_1 where col_i8 IS NOT NULL;
SELECT MIN(col_i64) FROM cider_agg_test_1;
SELECT MIN(col_i64) FROM cider_agg_test_1 where col_i64 IS NOT NULL;
SELECT MIN(col_fp32) FROM cider_agg_test_1;
SELECT MIN(col_fp32) FROM cider_agg_test_1 where col_fp32 IS NOT NULL;
SELECT MIN(col_fp64) FROM cider_agg_test_1;
SELECT MIN(col_fp64) FROM cider_agg_test_1 where col_fp64 IS NOT NULL;
-- SELECT MIN(half_null_i32) FROM cider_agg_test_1;
-- SELECT MIN(half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT MIN(half_null_i64) FROM cider_agg_test_1;
SELECT MIN(half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
SELECT MIN(half_null_fp32) FROM cider_agg_test_1;
SELECT MIN(half_null_fp32) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL;
SELECT MIN(half_null_fp64) FROM cider_agg_test_1;
SELECT MIN(half_null_fp64) FROM cider_agg_test_1 where half_null_fp64 IS NOT NULL;

-- SELECT MAX(col_i8), MAX(col_i64), MAX(col_fp32), MAX(col_fp64) FROM cider_agg_test_1;
-- SELECT MAX(half_null_i32), MAX(half_null_i64), MAX(half_null_fp32), MAX(half_null_fp64) FROM cider_agg_test_1;
-- SELECT MAX(col_i8) FROM cider_agg_test_1;
-- SELECT MAX(col_i8) FROM cider_agg_test_1 where col_i8 IS NOT NULL;
SELECT MAX(col_i64) FROM cider_agg_test_1;
SELECT MAX(col_i64) FROM cider_agg_test_1 where col_i64 IS NOT NULL;
SELECT MAX(col_fp32) FROM cider_agg_test_1;
SELECT MAX(col_fp32) FROM cider_agg_test_1 where col_fp32 IS NOT NULL;
SELECT MAX(col_fp64) FROM cider_agg_test_1;
SELECT MAX(col_fp64) FROM cider_agg_test_1 where col_fp64 IS NOT NULL;
-- SELECT MAX(half_null_i32) FROM cider_agg_test_1;
-- SELECT MAX(half_null_i32) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT MAX(half_null_i64) FROM cider_agg_test_1;
SELECT MAX(half_null_i64) FROM cider_agg_test_1 where half_null_i64 IS NOT NULL;
SELECT MAX(half_null_fp32) FROM cider_agg_test_1;
SELECT MAX(half_null_fp32) FROM cider_agg_test_1 where half_null_fp32 IS NOT NULL;
SELECT MAX(half_null_fp64) FROM cider_agg_test_1;
SELECT MAX(half_null_fp64) FROM cider_agg_test_1 where half_null_fp64 IS NOT NULL;

-- SELECT SUM(col_i8), MAX(col_i64), SUM(col_fp32), MIN(col_fp64), COUNT(DISTINCT col_i32) FROM cider_agg_test_1 where col_i8 > 0 and col_i32 > 0 and col_i64 > 0;
-- SELECT SUM(half_null_i32), MAX(half_null_i64), SUM(half_null_fp32), MIN(half_null_fp64) , COUNT(DISTINCT half_null_i16)FROM cider_agg_test_1 where half_null_i32 > 0 or half_null_i64 > 0 or half_null_fp32 > 0 or half_null_fp64 > 0 and half_null_i16 > 0;

SELECT SUM(col_i32 + col_i8), SUM(col_i8 + col_i32) FROM cider_agg_test_1;
SELECT SUM(col_i32 - col_i8) FROM cider_agg_test_1;
SELECT SUM(col_i32 * col_i8) FROM cider_agg_test_1;
SELECT SUM(col_i32 + 10) FROM cider_agg_test_1;
SELECT SUM(col_i32 - 10) FROM cider_agg_test_1;
SELECT SUM(col_i32 * 10) FROM cider_agg_test_1;
SELECT SUM(col_i32 / 10) FROM cider_agg_test_1;
SELECT SUM(col_i32 * 20 + 10) FROM cider_agg_test_1;
SELECT SUM(col_i32 * 10 + col_i8 * 5) FROM cider_agg_test_1;
SELECT SUM(col_i32 * (1 + col_i8)) FROM cider_agg_test_1;
SELECT SUM(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 + col_i8), SUM(col_i8 + half_null_i32) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 - col_i8) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 * col_i8) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 + 10) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 - 10) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 * 10) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 / 10) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 + 10) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 - 10) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 * 10) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 / 10) FROM cider_agg_test_1 where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 * 20 + 10) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 * 10 + col_i8 * 5) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 * (1 + col_i8)) FROM cider_agg_test_1;
SELECT SUM(half_null_i32 * (1 + col_i8) * (1 - col_i8)) FROM cider_agg_test_1;

-- SELECT MIN(col_i32 + col_i8), MIN(col_i8 + col_i32) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 - col_i8) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * col_i8) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 + 10) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 - 10) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * 10) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 / 10) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * 20 + 10) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * 10 + col_i8 * 5) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * (1 + col_i8)) FROM cider_agg_test_1;
-- SELECT MIN(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM cider_agg_test_1;

-- SELECT MAX(col_i32 + col_i8), MAX(col_i8 + col_i32) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 - col_i8) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 * col_i8) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 + 10) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 - 10) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 * 10) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 / 10) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 + 10) FROM cider_agg_test_1 where col_i32 IS NOT NULL;
-- SELECT MAX(col_i32 - 10) FROM cider_agg_test_1 where col_i32 IS NOT NULL;
-- SELECT MAX(col_i32 * 10) FROM cider_agg_test_1 where col_i32 IS NOT NULL;
-- SELECT MAX(col_i32 / 10) FROM cider_agg_test_1 where col_i32 IS NOT NULL;
-- SELECT MAX(col_i32 * 20 + 10) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 * 10 + col_i8 * 5) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 * (1 + col_i8)) FROM cider_agg_test_1;
-- SELECT MAX(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM cider_agg_test_1;

SELECT CAST(col_i32 as tinyint) FROM cider_agg_test_1;
SELECT CAST(col_i32 as smallint) FROM cider_agg_test_1;

SELECT SUM(cast(col_i32 as tinyint)) FROM cider_agg_test_1;
SELECT SUM(cast(col_i32 as smallint)) FROM cider_agg_test_1;
SELECT SUM(cast(col_i32 as double)) FROM cider_agg_test_1;
SELECT SUM(cast(col_i64 as tinyint)) FROM cider_agg_test_1;
SELECT SUM(cast(col_i64 as smallint)) FROM cider_agg_test_1;
SELECT SUM(cast(col_i64 as int)) FROM cider_agg_test_1;
SELECT SUM(cast(col_fp32 as int)) FROM cider_agg_test_1;
SELECT SUM(cast(col_fp32 as double)) FROM cider_agg_test_1;
SELECT SUM(cast(col_fp64 as int)) FROM cider_agg_test_1;
SELECT SUM(cast(col_fp64 as bigint)) FROM cider_agg_test_1;
SELECT SUM(cast(col_fp64 as double)) FROM cider_agg_test_1;


-- SELECT COUNT(DISTINCT col_i8), COUNT(DISTINCT col_i32) FROM cider_agg_test_2;