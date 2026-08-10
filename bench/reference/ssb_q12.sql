-- DuckDB reference for bench/ssb/q12.sql.
COPY (
SELECT sum(lo_extendedprice * lo_discount) AS revenue
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_yearmonthnum = 199401) d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35
) TO '@OUT@' (FORMAT csv, HEADER);
