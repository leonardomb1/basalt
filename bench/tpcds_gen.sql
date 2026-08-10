-- TPC-DS SF1 fixtures for the `tpcds-*` suite. Run once, in the directory you want
-- the fixtures in, then export each table to parquet:
--
--     duckdb tpcds.db < bench/tpcds_gen.sql
--     mkdir -p tpcds && for t in $(duckdb tpcds.db -noheader -list \
--       -c "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"); do
--         duckdb tpcds.db -c "COPY $t TO 'tpcds/$t.parquet' (FORMAT parquet)"
--     done
--
-- 24 tables, ~690 MB as parquet. Nothing here is committed.
INSTALL tpcds;
LOAD tpcds;
CALL dsdgen(sf = 1);
