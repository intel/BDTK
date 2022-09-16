DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,cast(0 as smallint),null,null,0.000000,null), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,null,null,1,null,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,cast(2 as smallint),null,null,2.000000,2.000000), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,null,3,3,null,3.000000), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,cast(4 as tinyint),null,4,null,null,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,null,6,null,null,6.000000), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),cast(7 as smallint),null,null,7.000000,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,null,null,null,8.000000,8.000000), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),cast(9 as smallint),9,9,null,9.000000) 
;
SELECT SUM(col_i8) FROM test;
SELECT SUM(col_i16) FROM test;
SELECT SUM(col_i32) FROM test;
SELECT SUM(col_i64) FROM test;
SELECT SUM(col_fp32) FROM test;
SELECT SUM(col_fp64) FROM test;
SELECT SUM(half_null_i8) FROM test;
SELECT SUM(half_null_i8) FROM test where half_null_i8 IS NOT NULL;
SELECT SUM(half_null_i16) FROM test;
SELECT SUM(half_null_i16) FROM test where half_null_i16 IS NOT NULL;
SELECT SUM(half_null_i32) FROM test;
SELECT SUM(half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i64) FROM test;
SELECT SUM(half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT SUM(half_null_fp32) FROM test;
SELECT SUM(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL;
SELECT SUM(half_null_fp64) FROM test;
SELECT SUM(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,cast(0 as tinyint),null,null,null,0.000000,null), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,cast(1 as tinyint),cast(1 as smallint),1,1,null,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,null,null,2,null,2.000000), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,cast(3 as tinyint),cast(3 as smallint),3,null,null,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,null,null,4,null,null), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),null,null,5,5.000000,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,cast(6 as smallint),null,6,6.000000,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),null,null,7,7.000000,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,cast(8 as tinyint),null,8,null,8.000000,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),null,9,9,9.000000,9.000000) 
;
SELECT col_i32, COUNT(col_i32) FROM test GROUP BY col_i32;
SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(*) FROM test where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(1) FROM test where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(half_null_fp32) FROM test GROUP BY half_null_fp32;
SELECT half_null_fp32, COUNT(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL GROUP BY half_null_fp32;
SELECT COUNT(*) FROM test;
SELECT COUNT(*), MIN(half_null_fp64) FROM test;
SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32;
SELECT COUNT(1) FROM test;
SELECT COUNT(1), MAX(col_i8) FROM test;
SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32;
SELECT COUNT(col_i32) FROM test;
SELECT COUNT(col_i32) FROM test where col_i32 IS NOT NULL;
SELECT COUNT(col_i64) FROM test;
SELECT COUNT(col_i64) FROM test where col_i64 IS NOT NULL;
SELECT COUNT(col_fp32) FROM test;
SELECT COUNT(col_fp32) FROM test where col_fp32 IS NOT NULL;
SELECT COUNT(col_fp64) FROM test;
SELECT COUNT(col_fp64) FROM test where col_fp64 IS NOT NULL;
SELECT COUNT(half_null_i32) FROM test;
SELECT COUNT(half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT COUNT(half_null_i64) FROM test;
SELECT COUNT(half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT COUNT(half_null_fp32) FROM test;
SELECT COUNT(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL;
SELECT COUNT(half_null_fp64) FROM test;
SELECT COUNT(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL;
SELECT COUNT(half_null_i16) FROM test;
SELECT COUNT(half_null_i16) FROM test where half_null_i16 IS NOT NULL;
SELECT COUNT(half_null_i8) FROM test;
SELECT COUNT(half_null_i8) FROM test where half_null_i8 IS NOT NULL;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,cast(0 as tinyint),cast(0 as smallint),null,0,null,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,cast(1 as smallint),null,1,1.000000,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,cast(2 as tinyint),null,2,null,2.000000,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,null,3,null,3.000000,3.000000), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,cast(4 as tinyint),cast(4 as smallint),null,4,null,null), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,cast(6 as tinyint),null,null,null,6.000000,6.000000), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,null,null,7,7,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,null,cast(9 as smallint),9,9,null,9.000000) 
;
SELECT COUNT(DISTINCT col_i8) FROM test;
SELECT COUNT(DISTINCT col_i16) FROM test;
SELECT COUNT(DISTINCT col_i32) FROM test;
SELECT COUNT(DISTINCT col_i64) FROM test;
SELECT COUNT(DISTINCT half_null_i8) FROM test;
SELECT COUNT(DISTINCT half_null_i8) FROM test where half_null_i8 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i16) FROM test;
SELECT COUNT(DISTINCT half_null_i16) FROM test where half_null_i16 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i32) FROM test;
SELECT COUNT(DISTINCT half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT COUNT(DISTINCT half_null_i64) FROM test;
SELECT COUNT(DISTINCT half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT SUM(col_i8), COUNT(DISTINCT col_i8) FROM test;
SELECT SUM(col_i16), COUNT(DISTINCT col_i16) FROM test;
SELECT SUM(col_i32), COUNT(DISTINCT col_i32) FROM test;
SELECT SUM(col_i64), COUNT(DISTINCT col_i64) FROM test;
SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM test;
SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM test where half_null_i8 IS NOT NULL;
SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM test;
SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM test where half_null_i16 IS NOT NULL;
SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM test;
SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM test;
SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT SUM(col_i32), COUNT(DISTINCT col_i32), COUNT(DISTINCT col_i64) FROM test;
SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT half_null_i64) FROM test;
SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT half_null_i64) FROM test where half_null_i32 IS NOT NULL AND half_null_i64 IS NOT NULL;
SELECT COUNT(DISTINCT col_i32) FROM test GROUP BY col_i8;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,cast(0 as tinyint),null,null,0,null,null), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,null,null,1,null,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,cast(2 as tinyint),null,2,null,2.000000,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,cast(3 as tinyint),cast(3 as smallint),3,null,null,3.000000), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,cast(4 as smallint),null,4,4.000000,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,null,null,5,5,5.000000,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,cast(6 as smallint),null,6,6.000000,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),null,null,null,7.000000,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,null,null,8,null,8.000000), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,null,null,null,9,9.000000,9.000000) 
;
SELECT MIN(col_i8), MIN(col_i64), MIN(col_fp32), MIN(col_fp64) FROM test;
SELECT MIN(half_null_i32), MIN(half_null_i64), MIN(half_null_fp32), MIN(half_null_fp64) FROM test;
SELECT MIN(col_i8) FROM test;
SELECT MIN(col_i8) FROM test where col_i8 IS NOT NULL;
SELECT MIN(col_i64) FROM test;
SELECT MIN(col_i64) FROM test where col_i64 IS NOT NULL;
SELECT MIN(col_fp32) FROM test;
SELECT MIN(col_fp32) FROM test where col_fp32 IS NOT NULL;
SELECT MIN(col_fp64) FROM test;
SELECT MIN(col_fp64) FROM test where col_fp64 IS NOT NULL;
SELECT MIN(half_null_i32) FROM test;
SELECT MIN(half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT MIN(half_null_i64) FROM test;
SELECT MIN(half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT MIN(half_null_fp32) FROM test;
SELECT MIN(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL;
SELECT MIN(half_null_fp64) FROM test;
SELECT MIN(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,cast(0 as tinyint),cast(0 as smallint),null,0,0.000000,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,null,1,1,1.000000,1.000000), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,null,null,2,2.000000,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,cast(3 as smallint),3,3,3.000000,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,null,4,4,4.000000,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),null,5,5,null,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,cast(6 as smallint),null,null,null,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),cast(7 as smallint),7,null,7.000000,7.000000), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,cast(8 as tinyint),cast(8 as smallint),8,8,null,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,null,null,null,null,null,9.000000) 
;
SELECT MAX(col_i8), MAX(col_i64), MAX(col_fp32), MAX(col_fp64) FROM test;
SELECT MAX(half_null_i32), MAX(half_null_i64), MAX(half_null_fp32), MAX(half_null_fp64) FROM test;
SELECT MAX(col_i8) FROM test;
SELECT MAX(col_i8) FROM test where col_i8 IS NOT NULL;
SELECT MAX(col_i64) FROM test;
SELECT MAX(col_i64) FROM test where col_i64 IS NOT NULL;
SELECT MAX(col_fp32) FROM test;
SELECT MAX(col_fp32) FROM test where col_fp32 IS NOT NULL;
SELECT MAX(col_fp64) FROM test;
SELECT MAX(col_fp64) FROM test where col_fp64 IS NOT NULL;
SELECT MAX(half_null_i32) FROM test;
SELECT MAX(half_null_i32) FROM test where half_null_i32 IS NOT NULL;
SELECT MAX(half_null_i64) FROM test;
SELECT MAX(half_null_i64) FROM test where half_null_i64 IS NOT NULL;
SELECT MAX(half_null_fp32) FROM test;
SELECT MAX(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL;
SELECT MAX(half_null_fp64) FROM test;
SELECT MAX(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,null,0,null,null,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,cast(1 as tinyint),null,null,null,1.000000,1.000000), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,cast(2 as smallint),2,2,null,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,null,3,3,null,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,cast(4 as smallint),4,null,null,null), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),null,5,null,5.000000,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,cast(6 as smallint),null,null,null,6.000000), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,null,cast(7 as smallint),7,null,7.000000,7.000000), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,null,8,8,8.000000,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,null) 
;
SELECT SUM(col_i8), MAX(col_i64), SUM(col_fp32), MIN(col_fp64), COUNT(DISTINCT col_i32) FROM test where col_i8 > 0 and col_i32 > 0 and col_i64 > 0;
SELECT SUM(half_null_i32), MAX(half_null_i64), SUM(half_null_fp32), MIN(half_null_fp64) , COUNT(DISTINCT half_null_i16)FROM test where half_null_i32 > 0 or half_null_i64 > 0 or half_null_fp32 > 0 or half_null_fp64 > 0 and half_null_i16 > 0;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,cast(0 as smallint),null,0,0.000000,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,cast(1 as tinyint),cast(1 as smallint),null,null,1.000000,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,cast(2 as tinyint),cast(2 as smallint),null,null,null,2.000000), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,cast(3 as tinyint),cast(3 as smallint),null,3,null,3.000000), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,cast(4 as tinyint),null,4,null,null,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,null,null,null,null,5.000000,5.000000), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,cast(6 as tinyint),null,null,6,6.000000,6.000000), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,null,null,7,null,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,cast(8 as tinyint),cast(8 as smallint),null,8,8.000000,8.000000), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,null,cast(9 as smallint),9,9,9.000000,null) 
;
SELECT SUM(col_i32 + col_i8), SUM(col_i8 + col_i32) FROM test;
SELECT SUM(col_i32 - col_i8) FROM test;
SELECT SUM(col_i32 * col_i8) FROM test;
SELECT SUM(col_i32 + 10) FROM test;
SELECT SUM(col_i32 - 10) FROM test;
SELECT SUM(col_i32 * 10) FROM test;
SELECT SUM(col_i32 / 10) FROM test;
SELECT SUM(col_i32 * 20 + 10) FROM test;
SELECT SUM(col_i32 * 10 + col_i8 * 5) FROM test;
SELECT SUM(col_i32 * (1 + col_i8)) FROM test;
SELECT SUM(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test;
SELECT SUM(half_null_i32 + col_i8), SUM(col_i8 + half_null_i32) FROM test;
SELECT SUM(half_null_i32 - col_i8) FROM test;
SELECT SUM(half_null_i32 * col_i8) FROM test;
SELECT SUM(half_null_i32 + 10) FROM test;
SELECT SUM(half_null_i32 - 10) FROM test;
SELECT SUM(half_null_i32 * 10) FROM test;
SELECT SUM(half_null_i32 / 10) FROM test;
SELECT SUM(half_null_i32 + 10) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 - 10) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 * 10) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 / 10) FROM test where half_null_i32 IS NOT NULL;
SELECT SUM(half_null_i32 * 20 + 10) FROM test;
SELECT SUM(half_null_i32 * 10 + col_i8 * 5) FROM test;
SELECT SUM(half_null_i32 * (1 + col_i8)) FROM test;
SELECT SUM(half_null_i32 * (1 + col_i8) * (1 - col_i8)) FROM test;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,cast(0 as smallint),0,null,null,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,cast(1 as tinyint),null,null,null,1.000000,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,cast(2 as tinyint),null,2,null,2.000000,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,null,null,3,3.000000,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,null,null,4,null,null), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),cast(5 as smallint),null,null,5.000000,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,null,null,6,6,null,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),cast(7 as smallint),7,null,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,cast(8 as smallint),8,8,null,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,null) 
;
SELECT MIN(col_i32 + col_i8), MIN(col_i8 + col_i32) FROM test;
SELECT MIN(col_i32 - col_i8) FROM test;
SELECT MIN(col_i32 * col_i8) FROM test;
SELECT MIN(col_i32 + 10) FROM test;
SELECT MIN(col_i32 - 10) FROM test;
SELECT MIN(col_i32 * 10) FROM test;
SELECT MIN(col_i32 / 10) FROM test;
SELECT MIN(col_i32 * 20 + 10) FROM test;
SELECT MIN(col_i32 * 10 + col_i8 * 5) FROM test;
SELECT MIN(col_i32 * (1 + col_i8)) FROM test;
SELECT MIN(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,null,0,null,0.000000,null), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,null,1,1,null,null), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,cast(2 as tinyint),cast(2 as smallint),null,2,null,null), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,cast(3 as tinyint),null,3,null,3.000000,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,null,null,null,null,null,null), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,null,cast(5 as smallint),5,null,null,5.000000), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,cast(6 as tinyint),null,null,null,6.000000,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,cast(7 as tinyint),cast(7 as smallint),null,7,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,null,null,null,8.000000,null), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),null,null,null,null,null) 
;
SELECT MAX(col_i32 + col_i8), MAX(col_i8 + col_i32) FROM test;
SELECT MAX(col_i32 - col_i8) FROM test;
SELECT MAX(col_i32 * col_i8) FROM test;
SELECT MAX(col_i32 + 10) FROM test;
SELECT MAX(col_i32 - 10) FROM test;
SELECT MAX(col_i32 * 10) FROM test;
SELECT MAX(col_i32 / 10) FROM test;
SELECT MAX(col_i32 + 10) FROM test where col_i32 IS NOT NULL;
SELECT MAX(col_i32 - 10) FROM test where col_i32 IS NOT NULL;
SELECT MAX(col_i32 * 10) FROM test where col_i32 IS NOT NULL;
SELECT MAX(col_i32 / 10) FROM test where col_i32 IS NOT NULL;
SELECT MAX(col_i32 * 20 + 10) FROM test;
SELECT MAX(col_i32 * 10 + col_i8 * 5) FROM test;
SELECT MAX(col_i32 * (1 + col_i8)) FROM test;
SELECT MAX(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,cast(0 as tinyint),null,null,0,null,null), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,null,null,1,1,1.000000,1.000000), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,cast(2 as smallint),null,null,null,2.000000), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,null,null,null,3,3.000000,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,cast(4 as tinyint),null,4,null,4.000000,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,cast(5 as tinyint),cast(5 as smallint),5,null,5.000000,null), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,null,null,7,null,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,cast(8 as smallint),null,null,8.000000,8.000000), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),null,null,null,null,9.000000) 
;
SELECT CAST(col_i32 as tinyint) FROM test;
SELECT CAST(col_i32 as smallint) FROM test;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, half_null_fp32 FLOAT, half_null_fp64 DOUBLE);
INSERT INTO test VALUES 
 (cast(0 as tinyint),cast(0 as smallint),0,0,0.000000,0.000000,null,cast(0 as smallint),0,null,null,0.000000), 
 (cast(1 as tinyint),cast(1 as smallint),1,1,1.000000,1.000000,cast(1 as tinyint),null,null,null,null,1.000000), 
 (cast(2 as tinyint),cast(2 as smallint),2,2,2.000000,2.000000,null,cast(2 as smallint),null,2,null,2.000000), 
 (cast(3 as tinyint),cast(3 as smallint),3,3,3.000000,3.000000,cast(3 as tinyint),cast(3 as smallint),null,null,3.000000,null), 
 (cast(4 as tinyint),cast(4 as smallint),4,4,4.000000,4.000000,cast(4 as tinyint),null,null,4,null,4.000000), 
 (cast(5 as tinyint),cast(5 as smallint),5,5,5.000000,5.000000,null,null,null,null,null,5.000000), 
 (cast(6 as tinyint),cast(6 as smallint),6,6,6.000000,6.000000,cast(6 as tinyint),cast(6 as smallint),6,6,null,null), 
 (cast(7 as tinyint),cast(7 as smallint),7,7,7.000000,7.000000,null,cast(7 as smallint),null,null,null,null), 
 (cast(8 as tinyint),cast(8 as smallint),8,8,8.000000,8.000000,null,null,null,null,null,8.000000), 
 (cast(9 as tinyint),cast(9 as smallint),9,9,9.000000,9.000000,cast(9 as tinyint),null,9,null,null,9.000000) 
;
SELECT SUM(cast(col_i32 as tinyint)) FROM test;
SELECT SUM(cast(col_i32 as smallint)) FROM test;
SELECT SUM(cast(col_i32 as float)) FROM test;
SELECT SUM(cast(col_i64 as tinyint)) FROM test;
SELECT SUM(cast(col_i64 as smallint)) FROM test;
SELECT SUM(cast(col_i64 as int)) FROM test;
SELECT SUM(cast(col_fp32 as int)) FROM test;
SELECT SUM(cast(col_fp32 as double)) FROM test;
SELECT SUM(cast(col_fp64 as int)) FROM test;
SELECT SUM(cast(col_fp64 as bigint)) FROM test;
SELECT SUM(cast(col_fp64 as float)) FROM test;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_i8 TINYINT, col_i32 INT);
INSERT INTO test VALUES 
 (cast(5 as tinyint),500), 
 (cast(3 as tinyint),500), 
 (cast(3 as tinyint),500) 
;
SELECT COUNT(DISTINCT col_i8), COUNT(DISTINCT col_i32) FROM test;
