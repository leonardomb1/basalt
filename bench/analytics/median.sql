-- An exact median per group: every value held, then selected.
SELECT l_returnflag, l_linestatus, median(l_extendedprice) AS med
FROM {lineitem}
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
