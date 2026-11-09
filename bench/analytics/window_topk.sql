-- A window top-k per group: each customer's three largest orders.
SELECT count(*) AS n, sum(o_totalprice) AS total
FROM (SELECT o_custkey, o_totalprice, row_number() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS rn
      FROM {orders}) t
WHERE rn <= 3;
