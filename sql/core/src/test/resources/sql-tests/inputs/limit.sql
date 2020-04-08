
-- limit on various data types
SELECT * FROM global_temp.testdata LIMIT 2;
SELECT * FROM global_temp.arraydata LIMIT 2;
SELECT * FROM global_temp.mapdata LIMIT 2;

-- foldable non-literal in limit
SELECT * FROM global_temp.testdata LIMIT 2 + 1;

SELECT * FROM global_temp.testdata LIMIT CAST(1 AS int);

-- limit must be non-negative
SELECT * FROM global_temp.testdata LIMIT -1;
SELECT * FROM global_temp.testdata TABLESAMPLE (-1 ROWS);


SELECT * FROM global_temp.testdata LIMIT CAST(1 AS INT);
-- evaluated limit must not be null
SELECT * FROM global_temp.testdata LIMIT CAST(NULL AS INT);

-- limit must be foldable
SELECT * FROM global_temp.testdata LIMIT key > 3;

-- limit must be integer
SELECT * FROM global_temp.testdata LIMIT true;
SELECT * FROM global_temp.testdata LIMIT 'a';

-- limit within a subquery
SELECT * FROM (SELECT * FROM range(10) LIMIT 5) WHERE id > 3;

-- limit ALL
SELECT * FROM global_temp.testdata WHERE key < 3 LIMIT ALL;
