-- ClickHouse names LAG lagInFrame, which reads the frame, so the frame is the
-- partition up to the row before; a row with none gets NULL as the others' LAG does.
SELECT count(*) AS n, sum(running) AS total_running, sum(prev) AS total_prev
FROM (SELECT sum(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
             lagInFrame(toNullable(o_totalprice)) OVER (PARTITION BY o_custkey ORDER BY o_orderdate, o_orderkey
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev
      FROM {orders}) t;
