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

SELECT * FROM cider_filter_test_1;
SELECT * FROM cider_filter_test_1 where TRUE;
-- SELECT * FROM cider_filter_test_1 where (col_3 > 0 and col_4 > 0) or (col_5 < 0 and col_6 < 0);
-- SELECT * FROM cider_filter_test_1 where col_2 <> 0 and col_7 > '1972-02-01';
-- SELECT *, 3 >= 2 FROM cider_filter_test_1 where col_8 = true;
-- SELECT * , (2*col_1) as col_8, (col_7 + interval '1' year) as col_9 FROM cider_filter_test_1 where col_7 > '1972-02-01';


-- SELECT * FROM cider_filter_test_2 WHERE col_1 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_2 WHERE col_2 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_2 WHERE col_3 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_2 WHERE col_4 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_2 WHERE col_3 not in (24, 25, 26);

SELECT col_1 FROM cider_filter_test_2 WHERE col_1 < 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 > 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 = 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 <= 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 >= 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 <> 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 IS NULL;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 IS NOT NULL;

SELECT col_1 FROM cider_filter_test_2 WHERE TRUE;
SELECT col_1 FROM cider_filter_test_2 WHERE FALSE;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 = 2;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 > 2;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <> 2;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 = 3;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <= 3;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <> 3;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <= 3 AND 2 >= 1;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <= 3 OR 2 >= 1;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 <= 3 AND col_1 <> 77;
SELECT col_1 FROM cider_filter_test_2 WHERE 2 = 3 OR col_1 <> 77;

SELECT col_1 FROM cider_filter_test_2 WHERE col_1 - 10 <= 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 + 10 >= 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 * 2 < 77;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 / 2 > 77;

SELECT col_2 FROM cider_filter_test_2 WHERE col_2 < 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 > 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 = 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 <= 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 >= 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 <> 77;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 IS NULL;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 IS NOT NULL;

SELECT col_3 FROM cider_filter_test_2 WHERE col_3 < 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 > 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 = 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 <= 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 >= 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 <> 77;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 IS NULL;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 IS NOT NULL;

SELECT col_4 FROM cider_filter_test_2 WHERE col_4 < 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 > 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 = 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 <= 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 >= 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 <> 77;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 IS NULL;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 IS NOT NULL;

SELECT col_1 FROM cider_filter_test_2 WHERE col_1 > 50 or col_1 < 5;
SELECT col_1 FROM cider_filter_test_2 WHERE col_1 IS NULL or col_1 < 5;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 > 50 or col_2 < 5;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 IS NULL or col_2 < 5;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 > 50 or col_3 < 5;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 IS NULL or col_3 < 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 > 50 or col_4 < 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 IS NULL or col_4 < 5;

SELECT col_1 FROM cider_filter_test_2 WHERE col_1 < 50 and col_1 > 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_1 IS NOT NULL and col_1 > 5;
SELECT col_2 FROM cider_filter_test_2 WHERE col_2 < 50 and col_2 > 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_2 IS NOT NULL and col_2 > 5;
SELECT col_3 FROM cider_filter_test_2 WHERE col_3 < 50 and col_3 > 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_3 IS NOT NULL and col_3 > 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 < 50 and col_4 > 5;
SELECT col_4 FROM cider_filter_test_2 WHERE col_4 IS NOT NULL and col_4 > 5;

SELECT col_1, col_2 FROM cider_filter_test_2 WHERE col_1 = col_2;
SELECT col_2, col_3 FROM cider_filter_test_2 WHERE col_2 = col_3;
SELECT col_2, col_4 FROM cider_filter_test_2 WHERE col_2 = col_4;
SELECT col_3, col_4 FROM cider_filter_test_2 WHERE col_3 = col_4;


SELECT col_1, col_5 FROM cider_filter_test_3 WHERE col_1 < col_5;
SELECT col_2, col_6 FROM cider_filter_test_3 WHERE col_2 < col_6;
SELECT col_3, col_7 FROM cider_filter_test_3 WHERE col_3 <= col_7;
SELECT col_4, col_8 FROM cider_filter_test_3 WHERE col_4 <= col_8;
SELECT col_1, col_5 FROM cider_filter_test_3 WHERE col_1 <> col_5;
SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_2 <= col_3 and col_2 >= col_1;
SELECT col_1, col_2, col_3 FROM cider_filter_test_3 WHERE col_2 >= col_3 or col_2 <= col_1;
SELECT col_1, col_5 FROM cider_filter_test_3 WHERE col_1 < col_5 AND col_5 > 0;

SELECT * FROM cider_filter_test_3 WHERE (col_1 > 0 AND col_2 < 0) OR (col_1 < 0 AND col_2 > 0);

SELECT * FROM cider_filter_test_3 WHERE col_1 between 0 AND 1000 ;

-- SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_1 in (24, 25, 26);
-- SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_2 in (24, 25, 26);
-- SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_3 in (24, 25, 26);
-- SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_4 in (24, 25, 26);
-- SELECT col_1, col_2, col_3, col_4 FROM cider_filter_test_3 WHERE col_3 not in (24, 25, 26);

SELECT col_1 FROM cider_filter_test_4 WHERE col_1 < 77;
SELECT col_2 FROM cider_filter_test_4 WHERE col_2 > 77;
SELECT col_3 FROM cider_filter_test_4 WHERE col_3 <= 77;
SELECT col_4 FROM cider_filter_test_4 WHERE col_4 >= 77;
SELECT col_1 FROM cider_filter_test_4 WHERE col_1 IS NOT NULL AND col_1 < 77;
SELECT col_2 FROM cider_filter_test_4 WHERE col_2 IS NOT NULL AND col_2 > 77;
SELECT col_3 FROM cider_filter_test_4 WHERE col_3 IS NOT NULL AND col_3 <= 77;
SELECT col_4 FROM cider_filter_test_4 WHERE col_4 IS NOT NULL AND col_4 >= 77;

-- SELECT * FROM cider_filter_test_4 WHERE col_1 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_2 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_3 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_4 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_1 IS NOT NULL AND col_1 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_2 IS NOT NULL AND col_2 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_3 IS NOT NULL AND col_3 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_4 IS NOT NULL AND col_4 in (24, 25, 26);
-- SELECT * FROM cider_filter_test_4 WHERE col_3 not in (24, 25, 26);
