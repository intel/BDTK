DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 TINYINT NOT NULL);
INSERT INTO tmp VALUES 
 (cast(0 as tinyint)), 
 (cast(1 as tinyint)), 
 (cast(2 as tinyint)), 
 (cast(3 as tinyint)), 
 (cast(4 as tinyint)), 
 (cast(5 as tinyint)), 
 (cast(6 as tinyint)), 
 (cast(7 as tinyint)), 
 (cast(8 as tinyint)), 
 (cast(9 as tinyint)) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 TINYINT);
INSERT INTO tmp VALUES 
 (cast(0 as tinyint)), 
 (null), 
 (null), 
 (cast(3 as tinyint)), 
 (null), 
 (null), 
 (cast(6 as tinyint)), 
 (null), 
 (null), 
 (null) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 SMALLINT NOT NULL);
INSERT INTO tmp VALUES 
 (cast(0 as smallint)), 
 (cast(1 as smallint)), 
 (cast(2 as smallint)), 
 (cast(3 as smallint)), 
 (cast(4 as smallint)), 
 (cast(5 as smallint)), 
 (cast(6 as smallint)), 
 (cast(7 as smallint)), 
 (cast(8 as smallint)), 
 (cast(9 as smallint)) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 SMALLINT);
INSERT INTO tmp VALUES 
 (null), 
 (null), 
 (null), 
 (cast(3 as smallint)), 
 (null), 
 (cast(5 as smallint)), 
 (cast(6 as smallint)), 
 (null), 
 (null), 
 (cast(9 as smallint)) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 INTEGER NOT NULL);
INSERT INTO tmp VALUES 
 (0), 
 (1), 
 (2), 
 (3), 
 (4), 
 (5), 
 (6), 
 (7), 
 (8), 
 (9) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 INTEGER);
INSERT INTO tmp VALUES 
 (0), 
 (null), 
 (2), 
 (3), 
 (null), 
 (null), 
 (null), 
 (null), 
 (8), 
 (9) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 BIGINT NOT NULL);
INSERT INTO tmp VALUES 
 (0), 
 (1), 
 (2), 
 (3), 
 (4), 
 (5), 
 (6), 
 (7), 
 (8), 
 (9) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 BIGINT NOT NULL);
INSERT INTO tmp VALUES 
 (0), 
 (1), 
 (2), 
 (3), 
 (4), 
 (5), 
 (6), 
 (7), 
 (8), 
 (9) 
;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 BIGINT);
INSERT INTO tmp VALUES 
 (0), 
 (null), 
 (null), 
 (3), 
 (null), 
 (null), 
 (null), 
 (null), 
 (8), 
 (9) 
;
SELECT c0 FROM tmp WHERE c0 between 0 and 5;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 FLOAT NOT NULL);
INSERT INTO tmp VALUES 
 (0.000000), 
 (1.000000), 
 (2.000000), 
 (3.000000), 
 (4.000000), 
 (5.000000), 
 (6.000000), 
 (7.000000), 
 (8.000000), 
 (9.000000) 
;
SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 FLOAT);
INSERT INTO tmp VALUES 
 (null), 
 (1.000000), 
 (null), 
 (null), 
 (null), 
 (null), 
 (6.000000), 
 (null), 
 (8.000000), 
 (null) 
;
SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 DOUBLE NOT NULL);
INSERT INTO tmp VALUES 
 (0.000000), 
 (1.000000), 
 (2.000000), 
 (3.000000), 
 (4.000000), 
 (5.000000), 
 (6.000000), 
 (7.000000), 
 (8.000000), 
 (9.000000) 
;
SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0;
DROP TABLE iF EXISTS tmp;
CREATE TABLE tmp(c0 DOUBLE);
INSERT INTO tmp VALUES 
 (0.000000), 
 (null), 
 (2.000000), 
 (null), 
 (null), 
 (null), 
 (null), 
 (null), 
 (8.000000), 
 (9.000000) 
;
SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0;
