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

SELECT SUBSTR(col_2, 1, 10) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 1, 8) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 4, 8) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 0, 12) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 12, 0) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 12, 2) FROM cider_string_test_1 ;
SELECT SUBSTRING(col_2 from 2 for 8) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 4, 0) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, -4, 2) FROM cider_string_test_1 ;
SELECT col_2 FROM cider_string_test_1 ;
SELECT col_1, col_2 FROM cider_string_test_1 ;
SELECT * FROM cider_string_test_1 ;
SELECT col_2 FROM cider_string_test_1 where col_2 = 'aaaa';
SELECT col_2 FROM cider_string_test_1 where col_2 = '0000000000';
SELECT col_2 FROM cider_string_test_1 where col_2 <> '0000000000';
SELECT col_1 FROM cider_string_test_1 where col_2 <> '1111111111';
SELECT col_1, col_2 FROM cider_string_test_1 where col_2 <> '2222222222';
SELECT * FROM cider_string_test_1 where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM cider_string_test_1 where col_2 <> 'abcdefghijklmn';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1234%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '22%22';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '_33%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '44_%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';
SELECT col_2 FROM cider_string_test_1 ;
SELECT col_1, col_2 FROM cider_string_test_1 ;
SELECT * FROM cider_string_test_1 ;
SELECT col_2 FROM cider_string_test_1 where col_2 = 'aaaa';
SELECT col_2 FROM cider_string_test_1 where col_2 = '0000000000';
SELECT col_2 FROM cider_string_test_1 where col_2 <> '0000000000';
SELECT col_1 FROM cider_string_test_1 where col_2 <> '1111111111';
SELECT col_1, col_2 FROM cider_string_test_1 where col_2 <> '2222222222';
SELECT * FROM cider_string_test_1 where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM cider_string_test_1 where col_2 <> 'abcdefghijklmn';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1234%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '22%22';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '_33%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '44_%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';
SELECT SUBSTR(col_2, 1, 10) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 1, 8) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 4, 8) FROM cider_string_test_1 ;
SELECT SUBSTRING(col_2 from 2 for 8) FROM cider_string_test_1 ;
SELECT SUBSTR(col_2, 4, 0) FROM cider_string_test_1 ;
SELECT col_2 FROM cider_string_test_1 ;
SELECT col_1, col_2 FROM cider_string_test_1 ;
SELECT * FROM cider_string_test_1 ;
SELECT col_2 FROM cider_string_test_1 where col_2 = 'aaaa';
SELECT col_2 FROM cider_string_test_1 where col_2 = '0000000000';
SELECT col_2 FROM cider_string_test_1 where col_2 <> '0000000000';
SELECT col_1 FROM cider_string_test_1 where col_2 <> '1111111111';
SELECT col_1, col_2 FROM cider_string_test_1 where col_2 <> '2222222222';
SELECT * FROM cider_string_test_1 where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM cider_string_test_1 where col_2 <> 'abcdefghijklmn';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1234%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '22%22';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '_33%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '44_%';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM cider_string_test_1 where col_2 LIKE '%1111';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '1111%';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM cider_string_test_1 where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';

SELECT col_1 FROM cider_string_test_2 where col_1 LIKE '[aa]%';

SELECT col_1, COUNT(*) FROM cider_string_test_3 GROUP BY col_1;
SELECT col_1, COUNT(*), col_2 FROM cider_string_test_3 GROUP BY col_1, col_2;
