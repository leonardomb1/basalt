-- The SQL Server -> StarRocks shapes timed by bench/dbmove.sh (see its header):
--   full     the company set ORD010/ORD020/ORD030 as one table, key-range split
--   incr     the same set, only rows whose UPDATED_AT is in the last 30 minutes
--   singles  a catalog of small tables, each with its own discovery, replaced
PARAM shape STRING DEFAULT 'full';

CREATE CONNECTION src TYPE sqlserver OPTIONS (host = '127.0.0.1', port = 31433, user = 'sa',
  password = 'It_Passw0rd1', database = 'bench', tls = 'insecure');
CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = '127.0.0.1', fe_port = 39030,
  be_url = 'http://127.0.0.1:38040', database = 'bronze', user = 'root', password = '');

CASE $shape
WHEN 'full' THEN
  LOAD INTO sr.bronze_ord USING stream_load
    UPSERT ON (ROW_ID, ORD_COMPANY)
    SPLIT BY (ROW_ID) JOBS 4 AS
  SELECT *, now() AS extraction_timestamp
  FROM EACH TABLE OF (src.QUERY($$SELECT name, SUBSTRING(name, LEN(name) - 2, 2) FROM sys.tables WHERE name LIKE 'ORD0[0-9]0'$$))
    AS (table_name, ORD_COMPANY) ANCHOR SCHEMA src.dbo.ORD010;
WHEN 'incr' THEN
  LOAD INTO sr.bronze_ord USING stream_load
    UPSERT ON (ROW_ID, ORD_COMPANY) AS
  SELECT *, now() AS extraction_timestamp
  FROM EACH TABLE OF (src.QUERY($$SELECT name, SUBSTRING(name, LEN(name) - 2, 2) FROM sys.tables WHERE name LIKE 'ORD0[0-9]0'$$))
    AS (table_name, ORD_COMPANY)
    PUSHDOWN($$UPDATED_AT >= DATEADD(MINUTE, -30, GETDATE())$$)
    ANCHOR SCHEMA src.dbo.ORD010;
WHEN 'singles' THEN
  FOR EACH ROW OF (src.QUERY($$SELECT name FROM sys.tables WHERE name LIKE 'SGL%' ORDER BY name$$)) AS (name)
    PARALLEL ON ERROR CONTINUE
    FOR EACH ROW OF (src.QUERY($$SELECT OBJECT_ID('dbo.${name}'), COL_LENGTH('dbo.${name}', 'UPDATED_AT')$$)) AS (oid, upd_len)
      FOR EACH ROW OF (sr.QUERY($$SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'bronze_${lower(name)}'$$)) AS (in_sr)
        LOAD INTO sr.IDENTIFIER('bronze_' || lower($name)) USING stream_load REPLACE AS
        SELECT *, now() AS extraction_timestamp FROM src.dbo.IDENTIFIER($name);
      END FOR;
    END FOR;
  END FOR;
END CASE;
