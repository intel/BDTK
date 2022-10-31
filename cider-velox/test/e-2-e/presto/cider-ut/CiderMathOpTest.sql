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

SELECT 2 + 1 FROM cider_math_op_test;
SELECT 2 - 1 FROM cider_math_op_test;
SELECT 2 * 1 FROM cider_math_op_test;
SELECT 2 / 1 FROM cider_math_op_test;
SELECT 2 / 1 + 2 * 1 FROM cider_math_op_test;
SELECT 2 * 1 - 2 / 1 FROM cider_math_op_test;
SELECT 4 * 1.5 - 23 / 11 FROM cider_math_op_test;
SELECT 4 * 2 + 23 / 11 FROM cider_math_op_test;
SELECT 2 % 1 FROM cider_math_op_test;

SELECT tinyint_col + 2 FROM cider_math_op_test;
SELECT tinyint_col - 2 FROM cider_math_op_test;
SELECT tinyint_col * 2 FROM cider_math_op_test;
SELECT tinyint_col / 2 FROM cider_math_op_test;
SELECT tinyint_col % 2 FROM cider_math_op_test;
SELECT smallint_col + 2 FROM cider_math_op_test;
SELECT smallint_col - 2 FROM cider_math_op_test;
SELECT smallint_col * 2 FROM cider_math_op_test;
SELECT smallint_col / 2 FROM cider_math_op_test;
SELECT smallint_col % 2 FROM cider_math_op_test;
SELECT integer_col + 2 FROM cider_math_op_test;
SELECT integer_col - 2 FROM cider_math_op_test;
SELECT integer_col * 2 FROM cider_math_op_test;
SELECT integer_col / 2 FROM cider_math_op_test;
SELECT integer_col % 2 FROM cider_math_op_test;
SELECT bigint_col + 2 FROM cider_math_op_test;
SELECT bigint_col - 2 FROM cider_math_op_test;
SELECT bigint_col * 2 FROM cider_math_op_test;
SELECT bigint_col / 2 FROM cider_math_op_test;
SELECT bigint_col % 2 FROM cider_math_op_test;
SELECT float_col + 2 FROM cider_math_op_test;
SELECT float_col - 2 FROM cider_math_op_test;
SELECT float_col * 2 FROM cider_math_op_test;
SELECT float_col / 2 FROM cider_math_op_test;
SELECT double_col + 2 FROM cider_math_op_test;
SELECT double_col - 2 FROM cider_math_op_test;
SELECT double_col * 2 FROM cider_math_op_test;
SELECT double_col / 2 FROM cider_math_op_test;

DROP TABLE iF EXISTS cider_math_op_test;
CREATE TABLE cider_math_op_test(integer_col INTEGER, bigint_col BIGINT, float_col DOUBLE, double_col DOUBLE, tinyint_col TINYINT, smallint_col SMALLINT);
INSERT INTO cider_math_op_test VALUES (110,null,7768.708984,7091.453125,null,cast(-7934 as smallint)), (1251,null,null,null,cast(-10 as tinyint),cast(-3898 as smallint)), (null,2775,null,7907.302734,cast(15 as tinyint),cast(9529 as smallint)), (-4633,null,-8715.534180,null,null,cast(-4623 as smallint)), (5918,-9797,8270.191406,7071.136719,null,null), (-9433,-1781,-1697.322266,331.916016,cast(-16 as tinyint),null), (-173,null,null,-297.736328,cast(8 as tinyint),cast(-9501 as smallint)), (35,4090,null,null,cast(-7 as tinyint),null), (null,3691,9822.195312,null,cast(-4 as tinyint),null), (null,null,null,null,null,cast(-8038 as smallint)), (null,4878,null,2072.517578,null,null), (-893,207,null,null,null,null), (null,null,-2074.532715,null,null,cast(-2961 as smallint)), (null,9724,3378.944336,null,null,null), (null,-792,9250.734375,8028.441406,cast(12 as tinyint),null), (-2808,7631,-5958.703125,null,null,null), (null,null,-8750.675781,null,cast(9 as tinyint),cast(1890 as smallint)), (6175,null,null,8422.064453,cast(1 as tinyint),cast(9382 as smallint)), (4594,null,null,null,null,cast(9374 as smallint)), (-3778,null,5899.535156,-6235.794922,cast(-15 as tinyint),null), (2436,null,-6988.193359,9611.949219,cast(13 as tinyint),cast(3063 as smallint)), (null,null,null,null,cast(-6 as tinyint),null), (null,2578,-5229.538574,null,cast(-11 as tinyint),cast(4683 as smallint)), (null,-6895,null,9938.531250,cast(9 as tinyint),null), (-5517,null,null,-8709.837891,cast(-14 as tinyint),cast(-8760 as smallint)), (-8543,8593,-3853.421875,null,null,null), (5252,4483,8826.761719,null,cast(16 as tinyint),cast(8522 as smallint)), (null,null,8780.423828,null,cast(11 as tinyint),null), (8200,9170,null,null,cast(-12 as tinyint),cast(7867 as smallint)), (null,null,null,-9501.836914,null,cast(-8054 as smallint)), (-7820,-2049,null,1610.014648,null,cast(6830 as smallint)), (null,null,null,3020.741211,null,cast(6456 as smallint)), (6172,null,5163.326172,-6419.448242,cast(-10 as tinyint),cast(-6644 as smallint)), (null,null,6431.998047,null,null,cast(7301 as smallint)), (null,null,null,null,null,cast(-6233 as smallint)), (9525,-6958,-14.990234,2374.964844,cast(-4 as tinyint),null), (null,-6580,7363.601562,-1082.932617,null,cast(-2141 as smallint)), (-8914,1566,9011.957031,9874.812500,null,null), (946,2610,null,null,null,cast(-4563 as smallint)), (4044,6084,855.248047,-9857.375977,cast(-5 as tinyint),null), (null,null,7311.128906,null,null,null), (9489,null,null,4674.705078,cast(7 as tinyint),cast(-5261 as smallint)), (-6433,-466,null,3984.947266,null,cast(22 as smallint)), (null,-9768,null,6944.335938,null,null), (2061,-7091,-7622.089355,7944.078125,null,cast(3656 as smallint)), (null,4426,-6466.673828,null,cast(-9 as tinyint),cast(8698 as smallint)), (-6299,null,-8774.199219,-7509.854980,cast(16 as tinyint),cast(-5332 as smallint)), (null,null,null,null,null,null), (-2754,6054,null,null,null,null), (null,4467,7555.382812,null,null,cast(-4308 as smallint)), (null,null,null,-6082.022461,null,cast(2358 as smallint)), (-9199,null,null,null,cast(5 as tinyint),cast(6594 as smallint)), (null,null,1757.919922,2812.840820,cast(-11 as tinyint),cast(-6632 as smallint)), (-7088,null,null,null,null,null), (null,null,7754.914062,2123.408203,cast(7 as tinyint),cast(-4809 as smallint)), (null,null,2235.360352,null,cast(9 as tinyint),cast(5982 as smallint)), (null,null,-1481.468750,null,null,cast(7629 as smallint)), (-1126,null,4924.875977,-779.456055,null,cast(-6341 as smallint)), (-8662,null,-5931.394531,null,cast(-7 as tinyint),cast(-4712 as smallint)), (null,2908,2660.405273,null,cast(15 as tinyint),null), (-9282,-3268,127.937500,-186.609375,cast(-1 as tinyint),cast(-2491 as smallint)), (null,null,null,9996.111328,null,null), (1020,-445,null,null,null,null), (6079,null,-5538.239258,1782.019531,cast(1 as tinyint),null), (null,null,null,9413.861328,null,cast(-7971 as smallint)), (4314,null,null,null,null,cast(5988 as smallint)), (8919,null,null,null,null,null), (9602,null,6351.077148,-4233.574219,null,null), (-2508,-7599,null,null,null,cast(1258 as smallint)), (9353,-8540,null,null,null,cast(-4777 as smallint)), (-5953,null,null,null,null,null), (-1891,-3497,-6442.012695,-6090.495117,cast(-9 as tinyint),null), (null,null,5297.423828,null,null,null), (null,3730,null,-1590.172852,null,null), (4867,null,1150.038086,null,null,cast(-5002 as smallint)), (null,-4167,null,null,cast(-9 as tinyint),cast(1993 as smallint)), (null,851,6214.761719,null,cast(7 as tinyint),null), (null,null,6948.562500,null,null,cast(-2582 as smallint)), (-3061,2073,-8158.150391,-2260.228027,null,cast(-1432 as smallint)), (null,4241,-1163.552734,-6810.748047,cast(4 as tinyint),null), (null,null,null,-639.222656,cast(11 as tinyint),cast(8061 as smallint)), (null,-6943,8247.214844,354.925781,null,null), (null,null,null,null,null,cast(-5935 as smallint)), (null,-9491,null,-3813.010254,cast(14 as tinyint),cast(-214 as smallint)), (-3720,-5720,3626.958984,4098.553711,null,cast(-7243 as smallint)), (null,887,null,null,cast(10 as tinyint),cast(7970 as smallint)), (-6162,null,6650.429688,null,cast(-15 as tinyint),cast(-9553 as smallint)), (null,null,6280.193359,8943.544922,null,null), (4049,6396,-6498.392578,285.594727,cast(2 as tinyint),null), (null,null,null,3213.143555,null,null), (null,null,null,5570.930664,null,cast(-2073 as smallint)), (7483,null,8776.242188,815.579102,null,cast(-5148 as smallint)), (null,null,null,-3328.990234,null,cast(-5778 as smallint)), (null,null,null,null,null,null), (null,-652,-2096.309570,-181.547852,cast(-9 as tinyint),null), (null,940,null,5492.613281,cast(3 as tinyint),cast(-9598 as smallint)), (null,1998,-9097.066406,null,null,null), (411,null,null,-9323.464844,null,null), (null,-5409,null,null,null,cast(-5806 as smallint)), (null,null,-130.948242,-7426.599609,null,cast(4142 as smallint)) ;
SELECT double_col + float_col, double_col - float_col FROM cider_math_op_test;
SELECT integer_col * bigint_col, bigint_col / integer_col FROM cider_math_op_test where integer_col <> 0;
SELECT bigint_col * double_col + integer_col * float_col FROM cider_math_op_test where bigint_col * double_col > integer_col * float_col ;
SELECT bigint_col * double_col - integer_col * float_col FROM cider_math_op_test where bigint_col * double_col > integer_col * float_col ;
SELECT (double_col - float_col) * (integer_col - bigint_col) FROM cider_math_op_test where double_col > float_col and integer_col > bigint_col;
SELECT (double_col - float_col) / (integer_col - bigint_col) FROM cider_math_op_test where double_col > float_col and integer_col > bigint_col;
SELECT double_col + integer_col / bigint_col FROM cider_math_op_test where bigint_col <> 0;
SELECT float_col + double_col FROM cider_math_op_test;

SELECT double_col + 1.23e1, double_col - 1.23e1 FROM cider_math_op_test;
SELECT bigint_col * 1.23e1, bigint_col / 1.23e1 FROM cider_math_op_test;
SELECT 3 * (1.23e1 + bigint_col) / 2 FROM cider_math_op_test;
SELECT (double_col + 1.23e1) / 2 - 2.5e1 FROM cider_math_op_test;

SELECT integer_col + 0.123 FROM cider_math_op_test;
SELECT integer_col - 0.123 FROM cider_math_op_test;
SELECT integer_col * 0.123 FROM cider_math_op_test;
SELECT (integer_col + 0.8) / 2 FROM cider_math_op_test;
SELECT tinyint_col + 0.123 FROM cider_math_op_test;
SELECT smallint_col - 0.123 FROM cider_math_op_test;
SELECT double_col + 0.123 FROM cider_math_op_test;
SELECT double_col - 0.123 FROM cider_math_op_test;
SELECT double_col * 0.123 FROM cider_math_op_test;
SELECT double_col / 0.123 FROM cider_math_op_test;
SELECT bigint_col + 0.123 FROM cider_math_op_test;
