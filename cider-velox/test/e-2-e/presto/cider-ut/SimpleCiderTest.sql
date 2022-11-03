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

SELECT * FROM simple_cider_test;
SELECT col_a FROM simple_cider_test;
SELECT col_b FROM simple_cider_test;
SELECT col_a, col_b FROM simple_cider_test;
SELECT col_b, col_a FROM simple_cider_test;
-- SELECT * FROM simple_cider_test WHERE col_a IS NULL;
-- SELECT * FROM simple_cider_test WHERE col_b IS NOT NULL;
-- SELECT col_a, col_b, col_c FROM simple_cider_test WHERE col_a IS NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_a IS NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_a IS NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_c IS NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_c IS NULL;
-- SELECT col_a, col_b, col_c FROM simple_cider_test WHERE col_b IS NOT NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NOT NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_a IS NOT NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_b IS NOT NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_b IS NOT NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_b IS NOT NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_c IS NOT NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_c IS NOT NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NOT NULL;
-- SELECT col_a FROM simple_cider_test WHERE col_a IS NOT NULL AND col_c IS NOT NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NOT NULL;
-- SELECT col_b FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_a IS NULL AND col_c IS NOT NULL;
-- SELECT col_c FROM simple_cider_test WHERE col_a IS NOT NULL AND col_c IS NOT NULL;
SELECT col_a FROM simple_cider_test WHERE col_a IS NULL OR col_c IS NULL;
SELECT col_a FROM simple_cider_test WHERE col_a IS NULL OR col_c IS NOT NULL;
SELECT col_a FROM simple_cider_test WHERE col_a IS NOT NULL OR col_c IS NOT NULL;
SELECT col_c FROM simple_cider_test WHERE col_a IS NULL OR col_c IS NULL;
SELECT col_c FROM simple_cider_test WHERE col_a IS NULL OR col_c IS NOT NULL;
SELECT col_c FROM simple_cider_test WHERE col_a IS NOT NULL OR col_c IS NOT NULL;
-- SELECT * FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NULL AND col_c IS NULL;
-- SELECT * FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NULL AND col_c IS NOT NULL;
-- SELECT * FROM simple_cider_test WHERE col_a IS NULL AND col_b IS NOT NULL AND col_c IS NOT NULL;
-- SELECT * FROM simple_cider_test WHERE col_a IS NOT NULL AND col_b IS NOT NULL AND col_c IS NOT NULL;
