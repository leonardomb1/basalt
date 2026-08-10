-- TPC-DS q42 — category revenue for one month
--
-- Ported from the spec's comma-join form to basalt's explicit joins with the
-- dimensions as CTEs. Predicates and grouping are unchanged.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH dt AS (SELECT d_date_sk, d_year FROM IDENTIFIER($data || '/tpcds/date_dim.parquet') WHERE d_moy = 11 AND d_year = 2000),
     it AS (SELECT i_item_sk, i_category_id, i_category FROM IDENTIFIER($data || '/tpcds/item.parquet') WHERE i_manager_id = 1)
SELECT dt.d_year, it.i_category_id, it.i_category, SUM(ss_ext_sales_price) AS revenue
FROM IDENTIFIER($data || '/tpcds/store_sales.parquet')
JOIN dt ON ss_sold_date_sk = d_date_sk
JOIN it ON ss_item_sk = i_item_sk
GROUP BY dt.d_year, it.i_category_id, it.i_category
ORDER BY revenue DESC, dt.d_year, it.i_category_id, it.i_category
LIMIT 100;
