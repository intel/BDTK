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

SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c < r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d < r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a > r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b > r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c > r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d > r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c <= r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d <= r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a >= r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b >= r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c >= r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d >= r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <> r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <> r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c <> r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d <> r_d;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c < r_c;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d < r_d;
SELECT max(r_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a;
SELECT max(r_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b;
SELECT max(r_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c <= r_c;
SELECT max(r_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d <= r_d;
SELECT sum(r_b) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a >= r_a;
SELECT sum(r_b) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b >= r_b;
SELECT sum(r_b) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c >= r_c;
SELECT sum(r_b) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d >= r_d;
SELECT sum(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <> r_a;
SELECT sum(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <> r_b;
SELECT sum(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c <> r_c;
SELECT sum(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d <> r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a +1 < r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b +1 < r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c +1 < r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d +1 < r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a > 1 + r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b > 1 + r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c > 1 + r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d > 1 + r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a -1 < 1 + r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b -1 < 1 + r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c -1 < 1 + r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d -1 < 1 + r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a *2 <= r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b *2 <= r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c *2 <= r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d *2 <= r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a >= 2 * r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b >= 2 * r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c >= 2 * r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d >= 2 * r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a *2 <= 2 * r_a;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b *2 <= 2 * r_b;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_c *2 <= 2 * r_c;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_d *2 <= 2 * r_d;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b AND l_c > r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a AND l_d > r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a AND l_d > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b AND r_c > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b AND l_c >= r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a AND l_d >= r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a AND l_d >= 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b AND r_c >= 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b AND l_c <> r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a AND l_d <> r_d ;
-- SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a AND l_d <> 10 ;
-- SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b AND r_c <> 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <> r_b AND l_c >= r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <> r_a AND l_d >= r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <> r_a AND l_d >= 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <> r_b AND r_c >= 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b OR l_c > r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a OR l_d > r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a OR l_d > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b OR r_c > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b > r_b OR l_c = r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a > r_a OR l_d = r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a > r_a OR l_d = 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b > r_b OR r_c = 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b = r_b OR l_c > r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a = r_a OR l_d > r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a = r_a OR l_d > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b = r_b OR r_c > 10 ;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b AND l_c > r_c ;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a AND l_d > r_d ;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a < r_a AND l_d > 10 ;
SELECT sum(l_a) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b < r_b AND r_c > 10 ;
SELECT min(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b OR l_c = r_c ;
SELECT min(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a OR l_d = r_d ;
SELECT min(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_a <= r_a OR l_d = 10 ;
SELECT min(l_c) from cider_non_equal_loop_join_test_table_probe JOIN cider_non_equal_loop_join_test_table_hash ON l_b <= r_b OR r_c = 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_b = r_b AND l_c > r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_a = r_a AND l_d > r_d ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_a = r_a AND l_d > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_b = r_b AND r_c > 10 ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_b > r_b AND l_c = r_c ;
SELECT * from cider_non_equal_loop_join_test_table_probe LEFT JOIN cider_non_equal_loop_join_test_table_hash ON l_a > r_a AND l_d = r_d ;
