-- ClickHouse fills a rolled-up key with its type's default unless group_by_use_nulls is set.
SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty, max(l_extendedprice) AS top
FROM {lineitem}
GROUP BY ROLLUP(l_returnflag, l_linestatus)
ORDER BY l_returnflag NULLS LAST, l_linestatus NULLS LAST
SETTINGS group_by_use_nulls = 1;
