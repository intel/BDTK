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

select coalesce(COL_INT, COL_BIGINT, 7) from cider_case_when_test_1;
select coalesce(COL_INT, COL_BIGINT, COL_DOUBLE) from cider_case_when_test_1;
select sum(coalesce(COL_INT, COL_BIGINT, COL_DOUBLE, 7)) from cider_case_when_test_1;
SELECT COALESCE(col_int, col_bigint, 777) FROM cider_case_when_test_1;
SELECT COALESCE(col_int, col_double, 777) FROM cider_case_when_test_1;
SELECT COALESCE(col_int, col_bigint, col_double, 777) FROM cider_case_when_test_1;
SELECT COALESCE(col_double, col_int) FROM cider_case_when_test_1;
SELECT COALESCE(col_int, col_double) FROM cider_case_when_test_1;
SELECT COALESCE(col_int, col_bigint, col_double, col_float) FROM cider_case_when_test_1;
SELECT count(COALESCE(col_double, 777)) FROM cider_case_when_test_1;
SELECT sum(COALESCE(col_double, 777)) FROM cider_case_when_test_1;
SELECT sum(COALESCE(col_int, col_bigint, col_double, 777)) FROM cider_case_when_test_1;
SELECT sum(COALESCE(col_int, col_bigint, col_double)) FROM cider_case_when_test_1;
SELECT col_int, CASE WHEN col_int > 3 THEN 1 ELSE 0 END FROM cider_case_when_test_1;
SELECT col_int, CASE WHEN col_bigint < 9 THEN 2 ELSE 1 END FROM cider_case_when_test_1;
SELECT col_bigint, CASE WHEN col_double > 5 THEN 3 ELSE 2 END FROM cider_case_when_test_1;
SELECT col_double, CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_1;
SELECT col_int, CASE WHEN col_int > 30 THEN 1 ELSE 0 END FROM cider_case_when_test_1;
SELECT col_bigint, CASE WHEN col_double > 50 THEN 3 ELSE 2 END FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int >50 THEN 10 ELSE 1 END) FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_bigint < 30 THEN 10 WHEN col_bigint > 70 THEN 20 WHEN col_bigint = 50 THEN 30 ELSE 1 END) FROM cider_case_when_test_1;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 ELSE 0 END FROM cider_case_when_test_1;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 ELSE 0 END FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int=1 THEN 10 ELSE 1 END) FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 ELSE 1 END) FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int=3 THEN 11 ELSE 2 END), SUM(CASE WHEN col_double=3 THEN 12 ELSE 3 END) FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int < 10 THEN 11 END), SUM(CASE WHEN col_double > 10 THEN 12 END) FROM cider_case_when_test_1;
SELECT SUM(CASE col_int WHEN 3 THEN 11 ELSE 2 END), SUM(CASE col_double WHEN 3 THEN 12 ELSE 3 END) FROM cider_case_when_test_1;
SELECT SUM(CASE WHEN col_int > 5 THEN 4 ELSE 3 END), CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_1 GROUP BY CASE WHEN col_int > 5 THEN 4 ELSE 3 END;
SELECT col_int, CASE WHEN col_int > 3 THEN 1 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_bigint < 9 THEN 2 ELSE 1 END FROM cider_case_when_test_2;
SELECT col_bigint, CASE WHEN col_double > 5 THEN 3 ELSE 2 END FROM cider_case_when_test_2;
SELECT col_double, CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 END FROM cider_case_when_test_2;
SELECT SUM(CASE WHEN col_int=1 THEN 10 ELSE 1 END) FROM cider_case_when_test_2;
SELECT SUM(CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 ELSE 1 END) FROM cider_case_when_test_2;
SELECT SUM(CASE WHEN col_int=3 THEN 11 ELSE 2 END), SUM(CASE WHEN col_double=3 THEN 12 ELSE 3 END) FROM cider_case_when_test_2;
SELECT SUM(CASE col_int WHEN 3 THEN 11 ELSE 2 END), SUM(CASE col_double WHEN 3 THEN 12 ELSE 3 END) FROM cider_case_when_test_2;
SELECT sum(col_double), CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_2 GROUP BY CASE WHEN col_int > 5 THEN 4 ELSE 3 END;
SELECT sum(col_double), col_bigint, CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_2 GROUP BY col_bigint, (CASE WHEN col_int > 5 THEN 4 ELSE 3 END);
SELECT col_int, CASE WHEN col_int > 3 THEN 1 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_bigint < 9 THEN 2 ELSE 1 END FROM cider_case_when_test_2;
SELECT col_bigint, CASE WHEN col_double > 5 THEN 3 ELSE 2 END FROM cider_case_when_test_2;
SELECT col_double, CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int > 30 THEN 1 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_bigint, CASE WHEN col_double > 50 THEN 3 ELSE 2 END FROM cider_case_when_test_2;
SELECT SUM(CASE WHEN col_int >50 THEN 10 ELSE 1 END) FROM cider_case_when_test_2;
SELECT SUM(CASE WHEN col_bigint < 30 THEN 10 WHEN col_bigint > 70 THEN 20 WHEN col_bigint = 50 THEN 30 ELSE 1 END) FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 ELSE 0 END FROM cider_case_when_test_2;
SELECT col_int, CASE WHEN col_int=1 THEN 10 WHEN col_int=2 THEN 20 WHEN col_int=3 THEN 30 ELSE 0 END FROM cider_case_when_test_2;
