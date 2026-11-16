-- Polars' SQL has no ROLLUP: its three groupings as a union.
SELECT * FROM (
  SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty, max(l_extendedprice) AS top FROM {lineitem} GROUP BY l_returnflag, l_linestatus
  UNION ALL
  SELECT l_returnflag, CAST(NULL AS VARCHAR) AS l_linestatus, count(*) AS n, sum(l_quantity) AS qty, max(l_extendedprice) AS top FROM {lineitem} GROUP BY l_returnflag
  UNION ALL
  SELECT CAST(NULL AS VARCHAR) AS l_returnflag, CAST(NULL AS VARCHAR) AS l_linestatus, count(*) AS n, sum(l_quantity) AS qty, max(l_extendedprice) AS top FROM {lineitem}
) t
ORDER BY l_returnflag NULLS LAST, l_linestatus NULLS LAST;
