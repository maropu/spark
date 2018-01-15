-- Tests different scenarios of intersect operation
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

-- Check intersect all operations
SELECT * FROM t1 INTERSECT ALL SELECT * FROM t2;

SELECT * FROM t1 INTERSECT ALL SELECT * FROM t1 where v <> 1 and v <> 2;

SELECT * FROM t1 where v <> 1 and v <> 22
INTERSECT ALL
SELECT * FROM t1 where v <> 2 and v >= 3;

SELECT t1.* FROM t1, t2 where t1.k = t2.k
INTERSECT ALL
SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != 'one';

SELECT * FROM t2 where v >= 1 and v <> 22 INTERSECT ALL SELECT * FROM t1;

SELECT (SELECT min(k) FROM t2 WHERE t2.k = t1.k) min_t2 FROM t1
INTERSECT ALL
SELECT (SELECT min(k) FROM t2) abs_min_t2 FROM t1 WHERE  t1.k = 'one';

SELECT t1.k
FROM   t1
WHERE  t1.v <= (SELECT   max(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k)
INTERSECT ALL
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

SELECT * FROM m1 INTERSECT ALL SELECT * FROM m2;
