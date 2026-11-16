-- No SQL-standard PIVOT here: the same columns by conditional sums.
SELECT o_orderpriority,
       sum(CASE WHEN o_orderstatus = 'F' THEN o_totalprice END) AS F,
       sum(CASE WHEN o_orderstatus = 'O' THEN o_totalprice END) AS O,
       sum(CASE WHEN o_orderstatus = 'P' THEN o_totalprice END) AS P
FROM {orders}
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
