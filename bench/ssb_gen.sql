-- SSB (Star Schema Benchmark) derived from TPC-H SF1, following the standard
-- derivation: lineitem+orders denormalised into the `lineorder` fact, TPC-H
-- dimensions widened with region/nation/city, and a generated date dimension.
-- Not ssb-dbgen output, so absolute numbers are not comparable to published SSB
-- results — but both engines read the identical files, which is the point.
INSTALL tpch; LOAD tpch;
CALL dbgen(sf=1);

CREATE OR REPLACE TABLE ddate AS
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER)          AS d_datekey,
       strftime(d, '%Y-%m-%d')                         AS d_date,
       CAST(strftime(d, '%Y') AS INTEGER)              AS d_year,
       CAST(strftime(d, '%Y%m') AS INTEGER)            AS d_yearmonthnum,
       strftime(d, '%b%Y')                             AS d_yearmonth,
       CAST(strftime(d, '%W') AS INTEGER)              AS d_weeknuminyear,
       CAST(strftime(d, '%m') AS INTEGER)              AS d_monthnuminyear
FROM (SELECT UNNEST(generate_series(DATE '1992-01-01', DATE '1999-12-31', INTERVAL 1 DAY)) AS d);

-- 5 regions x 25 nations; SSB gives each nation 10 cities named "<NATION9>N".
CREATE OR REPLACE TABLE cust AS
SELECT c_custkey, c_name, c_address, c_phone, c_mktsegment,
       r.r_name                                        AS c_region,
       n.n_name                                        AS c_nation,
       rpad(n.n_name, 9, ' ') || CAST(c_custkey % 10 AS VARCHAR) AS c_city
FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey
                JOIN region r ON n.n_regionkey = r.r_regionkey;

CREATE OR REPLACE TABLE supp AS
SELECT s_suppkey, s_name, s_address, s_phone,
       r.r_name                                        AS s_region,
       n.n_name                                        AS s_nation,
       rpad(n.n_name, 9, ' ') || CAST(s_suppkey % 10 AS VARCHAR) AS s_city
FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey
                JOIN region r ON n.n_regionkey = r.r_regionkey;

-- SSB spells brands 'MFGR#<mfgr><brand>' and categories 'MFGR#<mfgr><d>'.
CREATE OR REPLACE TABLE prt AS
SELECT p_partkey, p_name, p_size, p_container, p_type,
       'MFGR#' || regexp_extract(p_mfgr, '([0-9])$', 1)                        AS p_mfgr,
       'MFGR#' || regexp_extract(p_mfgr, '([0-9])$', 1)
                || substr(regexp_extract(p_brand, '([0-9]+)$', 1), 2, 1)        AS p_category,
       'MFGR#' || regexp_extract(p_brand, '([0-9]+)$', 1)
                || CAST(p_partkey % 10 AS VARCHAR)                             AS p_brand1
FROM part;

-- The fact: one row per lineitem, carrying its order's attributes.
CREATE OR REPLACE TABLE lineorder AS
SELECT l.l_orderkey                                    AS lo_orderkey,
       l.l_linenumber                                  AS lo_linenumber,
       o.o_custkey                                     AS lo_custkey,
       l.l_partkey                                     AS lo_partkey,
       l.l_suppkey                                     AS lo_suppkey,
       CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS lo_orderdate,
       o.o_orderpriority                               AS lo_orderpriority,
       o.o_shippriority                                AS lo_shippriority,
       CAST(l.l_quantity AS INTEGER)                   AS lo_quantity,
       l.l_extendedprice                               AS lo_extendedprice,
       o.o_totalprice                                  AS lo_ordtotalprice,
       CAST(l.l_discount * 100 AS INTEGER)             AS lo_discount,
       l.l_extendedprice * (1 - l.l_discount)          AS lo_revenue,
       ps.ps_supplycost * CAST(l.l_quantity AS INTEGER) AS lo_supplycost,
       CAST(l.l_tax * 100 AS INTEGER)                  AS lo_tax,
       l.l_shipmode                                    AS lo_shipmode
FROM lineitem l
JOIN orders o   ON l.l_orderkey = o.o_orderkey
JOIN partsupp ps ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey;

COPY lineorder TO 'lineorder.parquet' (FORMAT parquet);
COPY cust      TO 'customer.parquet'  (FORMAT parquet);
COPY supp      TO 'supplier.parquet'  (FORMAT parquet);
COPY prt       TO 'part.parquet'      (FORMAT parquet);
COPY ddate     TO 'date.parquet'      (FORMAT parquet);
SELECT 'lineorder' t, count(*) n FROM lineorder
UNION ALL SELECT 'customer', count(*) FROM cust
UNION ALL SELECT 'supplier', count(*) FROM supp
UNION ALL SELECT 'part', count(*) FROM prt
UNION ALL SELECT 'date', count(*) FROM ddate;
