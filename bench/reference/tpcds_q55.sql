-- DuckDB reference for bench/tpcds/q55.sql — the spec form, verbatim joins.
COPY (
SELECT i_brand_id brand_id, i_brand brand, sum(ss_ext_sales_price) ext_price
FROM read_parquet('@DATA@/tpcds/date_dim.parquet'),
     read_parquet('@DATA@/tpcds/store_sales.parquet'),
     read_parquet('@DATA@/tpcds/item.parquet')
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 28 AND d_moy = 11 AND d_year = 1999
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
) TO '@OUT@' (FORMAT csv, HEADER);
