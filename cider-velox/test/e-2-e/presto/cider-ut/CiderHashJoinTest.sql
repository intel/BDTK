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

SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_b = r_b;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_b = r_b;
SELECT sum(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(l_a) from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(l_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(r_a) from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(r_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a;
SELECT sum(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT sum(l_a) from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT sum(l_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT sum(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT sum(r_a) from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT sum(r_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a +1 = r_a;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_a +1 = r_a;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a +1 = r_a;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = 1 + r_a;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_a = 1 + r_a;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = 1 + r_a;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b -1 = 1 + r_b;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_b -1 = 1 + r_b;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_b -1 = 1 + r_b;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL AND l_a < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL AND r_a < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL AND l_c < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL AND r_c < 10 ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a < 10 ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL AND l_a < 10 ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a < 10 ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL AND r_a < 10 ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c < 10 ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL AND l_c < 10 ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c < 10 ;
-- SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL ;
SELECT SUM(l_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL AND r_c < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_c = r_c;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_c = r_c;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_c = r_c;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_d = r_d;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_c *2 = r_c;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_c *2 = r_c;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_c *2 = r_c;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_d = 2 * r_d;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_d = 2 * r_d;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_d = 2 * r_d;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_d *2 = 2 * r_d;
SELECT * from cider_hash_join_table_probe INNER JOIN cider_hash_join_table_hash ON l_d *2 = 2 * r_d;
SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_d *2 = 2 * r_d;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT * from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_c = r_c ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b OR l_b = 10 ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b AND l_b IS NOT NULL ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe LEFT JOIN cider_hash_join_table_hash ON l_a = r_a AND l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL AND l_a < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL AND r_a < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL AND l_c < 10 ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c < 10 ;
-- SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL ;
SELECT * from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL AND r_c < 10 ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a < 10 ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE l_a IS NOT NULL AND l_a < 10 ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a < 10 ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_a = r_a WHERE r_a IS NOT NULL AND r_a < 10 ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c < 10 ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE l_c IS NOT NULL AND l_c < 10 ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c < 10 ;
-- SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL ;
SELECT SUM(r_a) from cider_hash_join_table_probe JOIN cider_hash_join_table_hash ON l_b = r_b WHERE r_c IS NOT NULL AND r_c < 10 ;
