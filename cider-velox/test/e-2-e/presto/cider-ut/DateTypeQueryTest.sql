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

SELECT col_a FROM date_type_query_test_1 where col_b > '1970-01-01' ;
SELECT col_a FROM date_type_query_test_1 where col_b >= '1970-01-01' ;
SELECT col_b FROM date_type_query_test_1 where col_b < '1970-02-01' ;
SELECT col_b FROM date_type_query_test_1 where col_b <= '1970-02-01' ;
SELECT SUM(col_a) FROM date_type_query_test_1 where col_b <= '1980-01-01' ;
-- SELECT col_a, col_b FROM date_type_query_test_1 where col_b <> '1970-01-01' ;
SELECT col_a FROM date_type_query_test_1 where col_b >= '1970-01-01' and col_b < '1970-02-01' ;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '1' year ;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '2' year ;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '1' month ;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '10' month ;
SELECT col_a FROM date_type_query_test_1 where col_b > date '1971-01-01' - interval '1' year ;
SELECT col_a FROM date_type_query_test_1 where col_b > date '1971-01-01' - interval '12' month ;
SELECT col_a FROM date_type_query_test_1 where col_b >= date '1970-01-01' + interval '1' month and col_b < date '1970-01-01' + interval '2' month;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '1' day ;
SELECT col_a FROM date_type_query_test_1 where col_b < date '1970-01-01' + interval '80' day ;
SELECT col_a FROM date_type_query_test_1 where col_b > date '1970-02-01' - interval '10' day ;
SELECT col_a FROM date_type_query_test_2 where col_b > '1999-12-01' ;
SELECT col_b FROM date_type_query_test_2 where col_a < '1999-12-01' ;
SELECT col_b FROM date_type_query_test_2 where col_b < '2077-07-07' ;
SELECT col_a FROM date_type_query_test_2 where col_b >= '1900-01-01' and col_b < '2077-07-07' ;
SELECT col_a FROM date_type_query_test_2 where col_b > '1990-11-03' ;
SELECT col_b FROM date_type_query_test_2 where col_a < '1990-11-03' ;
SELECT col_b FROM date_type_query_test_2 where col_b < '2027-07-07' ;
SELECT col_a FROM date_type_query_test_2 where col_b >= '1900-01-01' and col_b < '2077-02-01' ;
