-- Grouping on string keys.
SELECT l_shipinstruct, l_shipmode, count(*) AS n, sum(l_quantity) AS qty
FROM {lineitem}
GROUP BY l_shipinstruct, l_shipmode
ORDER BY l_shipinstruct, l_shipmode;
