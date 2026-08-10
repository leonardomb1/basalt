-- DuckDB reference for bench/tpch/q01.sql. Must stay semantically identical to the
-- basalt script, including the CSV sink, so the comparison is whole-pipeline and not
-- basalt-writes-a-file vs duckdb-prints-a-table. @DATA@/@OUT@ are substituted by the
-- runner (src/bench_e2e.zig).
COPY (
  SELECT l_returnflag,
         l_linestatus,
         sum(l_quantity)                                       AS sum_qty,
         sum(l_extendedprice)                                  AS sum_base_price,
         sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         avg(l_quantity)                                       AS avg_qty,
         avg(l_extendedprice)                                  AS avg_price,
         avg(l_discount)                                       AS avg_disc,
         count(*)                                              AS count_order
  FROM read_parquet('@DATA@/lineitem.parquet')
  WHERE l_shipdate <= '1998-09-02'
  GROUP BY l_returnflag, l_linestatus
  ORDER BY l_returnflag, l_linestatus
) TO '@OUT@' (FORMAT csv, HEADER);
