-- ROLLUP: Q1's keys with their subtotals and grand total, three groupings in one pass.
SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty, max(l_extendedprice) AS top
FROM {lineitem}
GROUP BY ROLLUP(l_returnflag, l_linestatus)
ORDER BY l_returnflag NULLS LAST, l_linestatus NULLS LAST;
