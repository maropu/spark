----
-- # Check the PostgreSQL implementation (https://www.postgresql.org/docs/current/static/sql-prepare.html)
--
-- postgres=# SELECT * FROM t;
--  id | value
-- ----+-------
--   1 |     1
--   1 |     2
--   1 |     2
--   2 |     1
--   2 |     2
--   2 |     2
-- (6 rows)
--
-- postgres=# PREPARE p1(int) AS SELECT * FROM t WHERE id = $1;
-- PREPARE
--
-- postgres=# EXECUTE p1(1);
--  id | value
-- ----+-------
--   1 |     1
--   1 |     2
--   1 |     2
-- (3 rows)
--
-- postgres=# EXECUTE p1(1, 2);
-- ERROR:  wrong number of parameters for prepared statement "p1"
-- DETAIL:  Expected 1 parameters but got 2.
--
-- postgres=# PREPARE p1(int, int) AS SELECT * FROM t WHERE id = $1 AND value = $2;
-- ERROR:  prepared statement "p1" already exists
--
-- postgres=# EXECUTE p2(3);
-- ERROR:  prepared statement "p2" does not exist
--
-- #Check the MySQL implementation (https://dev.mysql.com/doc/refman/5.7/en/sql-syntax-prepared-statements.html)
--
-- mysql> SELECT * FROM r5;
-- +------+-------+
-- | id   | value |
-- +------+-------+
-- |    1 |     1 |
-- |    1 |     2 |
-- |    1 |     2 |
-- |    2 |     1 |
-- |    2 |     2 |
-- |    2 |     2 |
-- +------+-------+
-- 6 rows in set (0.00 sec)
--
-- mysql> PREPARE p1 FROM 'SELECT * FROM r5 WHERE id = ?';
-- Query OK, 0 rows affected (0.00 sec)
-- Statement prepared
--
-- mysql> SET @1 = 1;
-- Query OK, 0 rows affected (0.00 sec)
--
-- mysql> EXECUTE p1 USING @1;
-- +------+-------+
-- | id   | value |
-- +------+-------+
-- |    1 |     1 |
-- |    1 |     2 |
-- |    1 |     2 |
-- +------+-------+
-- 3 rows in set (0.01 sec)
--
-- mysql> SET @2 = 2;
-- Query OK, 0 rows affected (0.00 sec)
--
-- mysql> EXECUTE p1 USING @1, @2;
-- ERROR 1210 (HY000): Incorrect arguments to EXECUTE
--
-- mysql> PREPARE p1 FROM 'SELECT * FROM r5 WHERE id = ?';
-- Query OK, 0 rows affected (0.00 sec)
-- Statement prepared
--
-- mysql> EXECUTE p1 USING @1, @2;
-- ERROR 1210 (HY000): Incorrect arguments to EXECUTE
-- mysql> PREPARE p1 FROM 'SELECT * FROM r5 WHERE id = ?';
-- Query OK, 0 rows affected (0.00 sec)
-- Statement prepared
--
-- mysql> EXECUTE p2 USING @1;
-- ERROR 1243 (HY000): Unknown prepared statement handler (p2) given to EXECUTE
----

-- Prepare an INSERT statement
CREATE TABLE t1(col0 SHORT, col1 INT, col2 LONG, col3 DOUBLE, col4 STRING) USING parquet;

PREPARE p1(SHORT, INT, LONG, DOUBLE, STRING) AS INSERT INTO t1 VALUES ($1, $2, $3, $4, $5);

EXECUTE p1(1S, 2, 3L, 3.8, "a");

EXECUTE p1(2S, 8, 5L, 9.1, "b");

SELECT * FROM t1;

-- Check type coercion cases
EXECUTE p1(3Y, 3, 9Y, 1S, 3.8);

EXECUTE p1(4S, 0Y, 2, 6S, 5L);

PREPARE p2(SHORT, SHORT, INT, INT, DOUBLE) AS INSERT INTO t1 VALUES ($1, $2, $3, $4, $5);

EXECUTE p2(0S, 5S, 8, 7, 8.1);

EXECUTE p2(9Y, 1S, 8, 4Y, 2S);

SELECT * FROM t1;

-- Check exception cases in an INSERT statement
PREPARE p1(INT) AS INSERT INTO t1 VALUES (1S, $1, 8L, 5.7, "c");

EXECUTE noexist(8);

EXECUTE p1(2S, 8, 5L, 9.1, "b", 1);

EXECUTE p1(1S, 2);

PREPARE p3(SHORT, INT) AS INSERT INTO t1 VALUES ($1, $2, $3, $4, $5);

PREPARE p4(SHORT, INT, LONG, DOUBLE, STRING) AS INSERT INTO t1 VALUES ($1, $2, $3, $4);

PREPARE p5(SHORT, INT, LONG, DOUBLE, STRING) AS INSERT INTO t1 VALUES ($3, $4, $5, $6, $7);

-- Prepare an SELECT statement
CREATE TABLE t2 USING parquet AS SELECT * FROM VALUES (1, 3.0, "5"), (2, 7.0, "3") AS t2(col0, col1, col2);

PREPARE p6(INT, DOUBLE, STRING) AS SELECT * FROM t2 WHERE col0 = $1 AND col1 > $2 AND col2 = $3;

EXECUTE p6(1, 0.0, "a");

EXECUTE p6(2, 0.0, "b");

EXECUTE p6(0, 10.0, "c");

PREPARE p7(INT, DOUBLE) AS SELECT col0, pow(col1, $1) value FROM t2 WHERE col0 = $2;

EXECUTE p7(1, 2.0);

EXECUTE p7(2, -1.0);

-- Check type coercion cases
EXECUTE p6(1Y, 0, 5);

PREPARE p8(SHORT, LONG, INT) AS SELECT * FROM t2 WHERE col0 = $1 AND col1 > $2 AND col2 = $3;

EXECUTE p8(2S, 0L, 3);

EXECUTE p8(1Y, 1Y, 5S);

-- Check exception cases
EXECUTE p6("a", "b", "c");

PREPARE p9(INT, DOUBLE) AS SELECT * FROM t2 WHERE col0 = $1 AND col1 > $2 AND col2 = $3;

PREPARE p9(INT, DOUBLE, STRING) AS SELECT * FROM t2 WHERE col0 = $1;

PREPARE p9(INT, DOUBLE, STRING) AS SELECT * FROM t2 WHERE col0 = $3 AND col1 > $4 AND col2 = $5;

PREPARE p9(TIMESTAMP) AS SELECT * FROM t2 WHERE col1 > $1;
