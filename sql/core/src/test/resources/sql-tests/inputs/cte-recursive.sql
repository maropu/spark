create temporary view t as select * from values 0, 1, 2 as t(id);
create temporary view t2 as select * from values 0, 1 as t(id);

-- fails due to recursion isn't allowed with RECURSIVE keyword
WITH r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 10
)
SELECT * FROM r;

-- very basic recursion
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 10
)
SELECT * FROM r;

-- unlimited recursion fails at spark.sql.cte.recursion.level.limits level
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT * FROM r;

-- terminate recursion with LIMIT
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT * FROM r LIMIT 10;

-- terminate projected recursion with LIMIT
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT level, level FROM r LIMIT 10;

-- fails because using LIMIT to terminate recursion only works where Limit can be pushed through
-- recursion
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT level, level FROM r ORDER BY level LIMIT 10;

-- using string column in recursion
WITH RECURSIVE r(c) AS (
  SELECT 'a'
  UNION ALL
  SELECT c || ' b' FROM r WHERE LENGTH(c) < 10
)
SELECT * FROM r;

-- recursion works regardless the order of anchor and recursive terms
WITH RECURSIVE r(level) AS (
  SELECT level + 1 FROM r WHERE level < 10
  UNION ALL
  VALUES (0)
)
SELECT * FROM r;

-- multiple anchor terms are supported
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  VALUES (0, 'B')
  UNION ALL
  SELECT level + 1, data || 'C' FROM r WHERE level < 3
)
SELECT * FROM r;

-- multiple recursive terms are supported
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT level + 1, data || 'B' FROM r WHERE level < 2
  UNION ALL
  SELECT level + 1, data || 'C' FROM r WHERE level < 3
)
SELECT * FROM r;

-- multiple anchor and recursive terms are supported
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  VALUES (0, 'B')
  UNION ALL
  SELECT level + 1, data || 'C' FROM r WHERE level < 2
  UNION ALL
  SELECT level + 1, data || 'D' FROM r WHERE level < 3
)
SELECT * FROM r;

-- recursion without an anchor term fails
WITH RECURSIVE r(level) AS (
  SELECT level + 1 FROM r WHERE level < 3
)
SELECT * FROM r;

-- UNION combinator supported to eliminate duplicates and stop recursion
WITH RECURSIVE r(level) AS (
  VALUES (0), (0)
  UNION
  SELECT (level + 1) % 10 FROM r
)
SELECT * FROM r;

-- fails because a recursive query should contain UNION ALL or UNION combinator
WITH RECURSIVE r(level) AS (
  VALUES (0)
  INTERSECT
  SELECT level + 1 FROM r WHERE level < 10
)
SELECT * FROM r;

-- recursive reference is not allowed in a subquery
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r WHERE (SELECT SUM(level) FROM r) < 10
)
SELECT * FROM r;

-- recursive reference can't be used multiple times in a recursive term
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT r1.level + 1, r1.data
  FROM r AS r1
  JOIN r AS r2 ON r2.data = r1.data
  WHERE r1.level < 10
)
SELECT * FROM r;

-- recursive reference is not allowed on right side of a left outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT level + 1, r.data
  FROM (
    SELECT 'B' AS data
  ) AS o
  LEFT JOIN r ON r.data = o.data
)
SELECT * FROM r;

-- recursive reference is not allowed on left side of a right outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  RIGHT JOIN (
    SELECT 'B' AS data
  ) AS o ON o.data = r.data
)
SELECT * FROM r;

-- aggregate is supported in the anchor term
WITH RECURSIVE r(level, data) AS (
  SELECT MAX(level) AS level, SUM(data) AS data FROM VALUES (0, 1), (0, 2)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r ORDER BY level;

-- recursive reference is not allowed in an aggregate in a recursive term
WITH RECURSIVE r(group, data) AS (
  VALUES (0, 1L)
  UNION ALL
  SELECT 1, SUM(data) FROM r WHERE data < 10 GROUP BY group
)
SELECT * FROM r;

-- recursive reference is not allowed in an aggregate (made from project) in a recursive term
WITH RECURSIVE r(level) AS (
  VALUES (1L)
  UNION ALL
  SELECT SUM(level) FROM r WHERE level < 10
)
SELECT * FROM r;

-- aggregate is supported on a recursive table
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT COUNT(*) FROM r;

-- recursive reference is not allowed to use in combination with distinct
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 'A')
  UNION ALL
  SELECT DISTINCT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r;

-- multiple with works
WITH RECURSIVE y AS (
  VALUES (1) AS t(id)
),
x AS (
  SELECT * FROM y
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
)
SELECT * FROM x;

-- multiple with works 2
WITH RECURSIVE x AS (
  VALUES (1) AS t(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
),
y AS (
  VALUES (1) AS t(id)
  UNION ALL
  SELECT id + 1 FROM y WHERE id < 10
)
SELECT * FROM y LEFT JOIN x ON x.id = y.id;

-- multiple with works 3
WITH RECURSIVE x AS (
  VALUES (1) AS t(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
),
y AS (
  VALUES (1) AS t(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 10
)
SELECT * FROM y LEFT JOIN x ON x.id = y.id;

-- multiple with works 4
WITH RECURSIVE x AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 3
),
y AS (
  SELECT * FROM x
  UNION ALL
  SELECT * FROM x
),
z AS (
  SELECT * FROM x
  UNION ALL
  SELECT id + 1 FROM z WHERE id < 10
)
SELECT * FROM z;

-- multiple with works 5
WITH RECURSIVE x AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 3
),
y AS (
  SELECT * FROM x
  UNION ALL
  SELECT * FROM x
),
z AS (
  SELECT * FROM y
  UNION ALL
  SELECT id + 1 FROM z WHERE id < 10
)
SELECT * FROM z;

-- recursion nested into WITH
WITH t AS (
  WITH RECURSIVE s AS (
    VALUES (1) AS t(i)
    UNION ALL
    SELECT i + 1 FROM s
  )
  SELECT i AS j FROM s LIMIT 10
)
SELECT * FROM t;

-- WITH nested into recursion
WITH RECURSIVE outermost AS (
  WITH innermost AS (
    SELECT * FROM outermost
  )
  SELECT level + 1 FROM innermost WHERE level < 5
  UNION ALL
  SELECT 0 AS level
)
SELECT * FROM outermost;

-- recursion nested into recursion
WITH RECURSIVE t AS (
  WITH RECURSIVE s AS (
    VALUES (1) AS t(i)
    UNION ALL
    SELECT i + 1 FROM s WHERE i < 10
  )
  SELECT i AS j FROM s
  UNION ALL
  SELECT j + 1 FROM t WHERE j < 10
)
SELECT * FROM t;

-- recursion nested into recursion 2
WITH RECURSIVE t AS (
  WITH RECURSIVE s AS (
    SELECT j, 1 AS i FROM t
    UNION ALL
    SELECT j, i + 1 FROM s WHERE i < 3
  )
  VALUES (1) as t(j)
  UNION ALL
  SELECT j + 1 FROM s WHERE j < 3
)
SELECT * FROM t;

-- routes represented here is as follows:
--
-- New York<--->Boston
-- |            ∧
-- ∨            |
-- Washington---+
-- |
-- ∨
-- Raleigh
CREATE TEMPORARY VIEW routes(origin, destination) AS VALUES
  ('New York', 'Washington'),
  ('New York', 'Boston'),
  ('Boston', 'New York'),
  ('Washington', 'Boston'),
  ('Washington', 'Raleigh');

-- handling cycles that could cause infinite recursion
WITH RECURSIVE destinations_from_new_york AS (
  SELECT 'New York' AS destination, ARRAY('New York') AS path, 0 AS length
  UNION ALL
  SELECT r.destination, CONCAT(d.path, ARRAY(r.destination)), d.length + 1
  FROM routes AS r
  JOIN destinations_from_new_york AS d ON d.destination = r.origin AND NOT ARRAY_CONTAINS(d.path, r.destination)
)
SELECT * FROM destinations_from_new_york;

-- Fibonacci numbers
WITH RECURSIVE fibonacci AS (
  VALUES (0, 1) AS t(a, b)
  UNION ALL
  SELECT b, a + b FROM fibonacci WHERE a < 10
)
SELECT a FROM fibonacci ORDER BY a;

-- Clean up
DROP VIEW IF EXISTS t;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS routes;

