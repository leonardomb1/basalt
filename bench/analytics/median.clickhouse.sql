-- ClickHouse's median is approximate and medianExact returns one middle value;
-- quantileExactInclusive(0.5) interpolates between the two, as the others do.
SELECT l_returnflag, l_linestatus, quantileExactInclusive(0.5)(toFloat64(l_extendedprice)) AS med
FROM {lineitem}
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
