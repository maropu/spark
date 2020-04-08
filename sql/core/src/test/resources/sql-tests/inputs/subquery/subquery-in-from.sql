-- Aliased subqueries in FROM clause
SELECT * FROM (SELECT * FROM global_temp.testdata) AS t WHERE key = 1;

FROM (SELECT * FROM global_temp.testdata WHERE key = 1) AS t SELECT *;

-- Optional `AS` keyword
SELECT * FROM (SELECT * FROM global_temp.testdata) t WHERE key = 1;

FROM (SELECT * FROM global_temp.testdata WHERE key = 1) t SELECT *;

-- Disallow unaliased subqueries in FROM clause
SELECT * FROM (SELECT * FROM global_temp.testdata) WHERE key = 1;

FROM (SELECT * FROM global_temp.testdata WHERE key = 1) SELECT *;
