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

SELECT float_not_null_a, COUNT(*) FROM cider_group_by_test_1 GROUP BY float_not_null_a;
SELECT float_not_null_a, SUM(double_not_null_c) FROM cider_group_by_test_1 GROUP BY float_not_null_a;
SELECT float_not_null_a, AVG(double_not_null_c) FROM cider_group_by_test_1 GROUP BY float_not_null_a;
SELECT double_not_null_c, COUNT(*) FROM cider_group_by_test_1 GROUP BY double_not_null_c;
SELECT tinyint_not_null_e, COUNT(*) FROM cider_group_by_test_1 GROUP BY tinyint_not_null_e;
SELECT smallint_not_null_g, COUNT(*) FROM cider_group_by_test_1 GROUP BY smallint_not_null_g;
SELECT integer_not_null_i, COUNT(*) FROM cider_group_by_test_1 GROUP BY integer_not_null_i;
SELECT bigint_not_null_k, COUNT(*) FROM cider_group_by_test_1 GROUP BY bigint_not_null_k;
SELECT boolean_not_null_m, COUNT(*) FROM cider_group_by_test_1 GROUP BY boolean_not_null_m;
SELECT float_half_null_b, COUNT(*) FROM cider_group_by_test_1 GROUP BY float_half_null_b;
SELECT double_half_null_d, COUNT(*) FROM cider_group_by_test_1 GROUP BY double_half_null_d;
SELECT tinyint_half_null_f, COUNT(*) FROM cider_group_by_test_1 GROUP BY tinyint_half_null_f;
SELECT smallint_half_null_h, COUNT(*) FROM cider_group_by_test_1 GROUP BY smallint_half_null_h;
SELECT integer_half_null_j, COUNT(*) FROM cider_group_by_test_1 GROUP BY integer_half_null_j;
SELECT bigint_half_null_l, COUNT(*) FROM cider_group_by_test_1 GROUP BY bigint_half_null_l;
SELECT boolean_half_null_n, COUNT(*) FROM cider_group_by_test_1 GROUP BY boolean_half_null_n;
SELECT bigint_not_null_k, boolean_not_null_m, COUNT(*), SUM(bigint_not_null_k) FROM cider_group_by_test_1 GROUP BY bigint_not_null_k, boolean_not_null_m;
SELECT integer_not_null_i, smallint_not_null_g, COUNT(*), sum(integer_not_null_i), sum(smallint_not_null_g) FROM cider_group_by_test_1 GROUP BY integer_not_null_i, smallint_not_null_g;
SELECT integer_not_null_i, bigint_not_null_k, boolean_not_null_m, COUNT(*), sum(integer_not_null_i), sum(bigint_not_null_k) FROM cider_group_by_test_1 GROUP BY integer_not_null_i, bigint_not_null_k, boolean_not_null_m;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_2 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_2 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_2 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_2 GROUP BY col_a;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_2 GROUP BY col_a HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_2 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_2 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_2 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_2 WHERE col_a > 5 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_2 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_2 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 WHERE col_a > 5 and col_b > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 WHERE col_a > 5 and col_b > 5 and col_c > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_2 WHERE col_a > 5 and col_b < 5 GROUP BY col_a, col_b, col_c;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_2 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b;
SELECT col_a, SUM(col_a) FROM cider_group_by_test_2 WHERE col_a < 8 GROUP BY col_a HAVING col_a > 2;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_2 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_2 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_2 WHERE col_a < 500 GROUP BY col_a,col_b HAVING col_a > 0 AND col_b < 500;
SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS col_c_sum FROM cider_group_by_test_2 where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = col_b AND col_c > 500;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b > 5 and col_c > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b < 5 GROUP BY col_a, col_b, col_c;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b;
SELECT col_a, SUM(col_a) FROM cider_group_by_test_3 WHERE col_a < 8 GROUP BY col_a HAVING col_a > 2;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 WHERE col_a < 500 GROUP BY col_a,col_b HAVING col_a > 0 AND col_b < 500;
SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS col_c_sum FROM cider_group_by_test_3 where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = col_b AND col_c > 500;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 GROUP BY col_a HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b > 5 and col_c > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_3 WHERE col_a > 5 and col_b < 5 GROUP BY col_a, col_b, col_c;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b;
SELECT col_a, SUM(col_a) FROM cider_group_by_test_3 WHERE col_a < 8 GROUP BY col_a HAVING col_a > 2;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_3 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_3 WHERE col_a < 500 GROUP BY col_a,col_b HAVING col_a > 0 AND col_b < 500;
SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS col_c_sum FROM cider_group_by_test_3 where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = col_b AND col_c > 500;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_4 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_4 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_4 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_4 GROUP BY col_a;
-- SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_4 GROUP BY col_a HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_4 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_4 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_4 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_4 GROUP BY col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_4 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_5 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_5 GROUP BY col_a;
-- SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_5 GROUP BY col_a HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_5 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_5 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b HAVING col_a > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
-- SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_5 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_5 GROUP BY col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_5 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_6 GROUP BY col_a;
SELECT col_a, SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_6 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_6 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b, col_c;
SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM cider_group_by_test_6 GROUP BY col_a;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_6 GROUP BY col_a HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_6 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_6 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b, col_c HAVING col_a > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b > 5 and col_c > 5;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 GROUP BY col_a, col_b, col_c HAVING col_a > 5 and col_b < 5;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_6 GROUP BY col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_6 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_6 WHERE col_a > 5 GROUP BY col_a;
SELECT col_a, col_b, SUM(col_a) FROM cider_group_by_test_6 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM cider_group_by_test_6 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 WHERE col_a > 5 GROUP BY col_a, col_b;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 WHERE col_a > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 WHERE col_a > 5 and col_b > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 WHERE col_a > 5 and col_b > 5 and col_c > 5 GROUP BY col_a, col_b, col_c;
SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM cider_group_by_test_6 WHERE col_a > 5 and col_b < 5 GROUP BY col_a, col_b, col_c;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_6 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b;
SELECT col_a, SUM(col_a) FROM cider_group_by_test_6 WHERE col_a < 8 GROUP BY col_a HAVING col_a > 2;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_6 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b;
-- SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM cider_group_by_test_6 WHERE col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL;
SELECT SUM(col_a) AS col_a_sum FROM cider_group_by_test_6 WHERE col_a < 500 GROUP BY col_a,col_b HAVING col_a > 0 AND col_b < 500;
SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS col_c_sum FROM cider_group_by_test_6 where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = col_b AND col_c > 500;
