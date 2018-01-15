-- Tests different scenarios of except operation
create temporary view t1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", NULL)
  as t1(k, v);

create temporary view t2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5),
  ("one", NULL),
  (NULL, 5)
  as t2(k, v);


-- Except operation that will be replaced by left anti join
SELECT * FROM t1 EXCEPT SELECT * FROM t2;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 EXCEPT SELECT * FROM t1 where v <> 1 and v <> 2;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 where v <> 1 and v <> 22 EXCEPT SELECT * FROM t1 where v <> 2 and v >= 3;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT t1.* FROM t1, t2 where t1.k = t2.k
EXCEPT
SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != 'one';


-- Except operation that will be replaced by left anti join
SELECT * FROM t2 where v >= 1 and v <> 22 EXCEPT SELECT * FROM t1;


-- Except operation that will be replaced by left anti join
SELECT (SELECT min(k) FROM t2 WHERE t2.k = t1.k) min_t2 FROM t1
MINUS
SELECT (SELECT min(k) FROM t2) abs_min_t2 FROM t1 WHERE  t1.k = 'one';


-- Except operation that will be replaced by left anti join
SELECT t1.k
FROM   t1
WHERE  t1.v <= (SELECT   max(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k)
MINUS
SELECT t1.k
FROM   t1
WHERE  t1.v >= (SELECT   min(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k);

-- Check except/minus all operations
SELECT * FROM t1 EXCEPT ALL SELECT * FROM t2;

SELECT * FROM t1 EXCEPT ALL SELECT * FROM t1 where v <> 1 and v <> 2;

SELECT * FROM t1 where v <> 1 and v <> 22
EXCEPT ALL
SELECT * FROM t1 where v <> 2 and v >= 3;

SELECT t1.* FROM t1, t2 where t1.k = t2.k
EXCEPT ALL
SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != 'one';

SELECT * FROM t2 where v >= 1 and v <> 22 EXCEPT ALL SELECT * FROM t1;

SELECT (SELECT min(k) FROM t2 WHERE t2.k = t1.k) min_t2 FROM t1
MINUS ALL
SELECT (SELECT min(k) FROM t2) abs_min_t2 FROM t1 WHERE  t1.k = 'one';

SELECT t1.k
FROM   t1
WHERE  t1.v <= (SELECT   max(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k)
MINUS ALL
SELECT t1.k
FROM   t1
WHERE  t1.v >= (SELECT   min(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k);

-- Check unsupported types in intersect all operations
create temporary view m1 as select * from values
  (1, map(1, "a", 2, "b")),
  (2, map(3, "c", 4, "d"))
  as m1(k, v);

create temporary view m2 as select * from values
  (2, map(3, "c", 4, "d"))
  as m2(k, v);

SELECT * FROM m1 EXCEPT ALL SELECT * FROM m2;
