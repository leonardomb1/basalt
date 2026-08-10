-- TPC-DS q55 — brand revenue, one manager and month
--
-- Ported from the spec's comma-join form to basalt's explicit joins with the
-- dimensions as CTEs. Predicates and grouping are unchanged.
PARAM data STRING;
PARAM out  STRING;

LOAD INTO IDENTIFIER($out || '.csv') AS
WITH dt AS (SELECT d_date_sk FROM IDENTIFIER($data || '/tpcds/date_dim.parquet') WHERE d_moy = 11 AND d_year = 1999),
     it AS (SELECT i_item_sk, i_brand_id, i_brand FROM IDENTIFIER($data || '/tpcds/item.parquet') WHERE i_manager_id = 28)
SELECT it.i_brand_id AS brand_id, it.i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price
FROM IDENTIFIER($data || '/tpcds/store_sales.parquet')
JOIN dt ON ss_sold_date_sk = d_date_sk
JOIN it ON ss_item_sk = i_item_sk
GROUP BY it.i_brand, it.i_brand_id
ORDER BY ext_price DESC, brand_id
LIMIT 100;
