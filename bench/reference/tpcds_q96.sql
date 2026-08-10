-- DuckDB reference for bench/tpcds/q96.sql — the spec form, verbatim joins.
COPY (
SELECT count(*) AS n
FROM read_parquet('@DATA@/tpcds/store_sales.parquet'),
     read_parquet('@DATA@/tpcds/household_demographics.parquet'),
     read_parquet('@DATA@/tpcds/time_dim.parquet'),
     read_parquet('@DATA@/tpcds/store.parquet')
WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
  AND t_hour = 20 AND t_minute >= 30 AND hd_dep_count = 7 AND s_store_name = 'ese'
ORDER BY n
LIMIT 100
) TO '@OUT@' (FORMAT csv, HEADER);
