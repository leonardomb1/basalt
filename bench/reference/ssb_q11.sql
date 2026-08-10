-- DuckDB reference for bench/ssb/q11.sql.
COPY (
SELECT sum(lo_extendedprice * lo_discount) AS revenue
FROM read_parquet('@DATA@/ssb/lineorder.parquet')
JOIN (SELECT d_datekey FROM read_parquet('@DATA@/ssb/date.parquet') WHERE d_year = 1993) d ON lo_orderdate = d_datekey
WHERE lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25
) TO '@OUT@' (FORMAT csv, HEADER);
