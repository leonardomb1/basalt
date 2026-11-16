-- A running total and the previous value per customer: SUM over a ROWS frame and LAG,
-- both ordered within each customer's 1.5M orders' partitions.
SELECT count(*) AS n, sum(running) AS total_running, sum(prev) AS total_prev
FROM (SELECT sum(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
             lag(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey) AS prev
      FROM {orders}) t;
