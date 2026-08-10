-- Fixture generation for the end-to-end harness: TPC-H SF1 as parquet (the compute
-- suite) plus csv (the movement suite). Run once:
--
--     duckdb tpch.db < bench/gen.sql
--
-- from the directory you want the fixtures in, then point the harness at it:
--
--     export BASALT_BENCH_DATA=$PWD
--
-- DuckDB is a fixture generator and the reference implementation here, not a
-- dependency of basalt. ~1.3 GB total; nothing below is committed.
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf = 1);

COPY lineitem TO 'lineitem.parquet' (FORMAT parquet);
COPY orders   TO 'orders.parquet'   (FORMAT parquet);
COPY customer TO 'customer.parquet' (FORMAT parquet);
COPY part     TO 'part.parquet'     (FORMAT parquet);
COPY supplier TO 'supplier.parquet' (FORMAT parquet);
COPY partsupp TO 'partsupp.parquet' (FORMAT parquet);
COPY nation   TO 'nation.parquet'   (FORMAT parquet);
COPY region   TO 'region.parquet'   (FORMAT parquet);

-- The movement suite reads CSV, so the same rows are needed in that form.
COPY lineitem TO 'lineitem.csv' (FORMAT csv, HEADER);
COPY orders   TO 'orders.csv'   (FORMAT csv, HEADER);
