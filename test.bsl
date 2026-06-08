@batch

connection pg = postgres
  host = "10.221.2.202" port = 5432
  user = env("SQL_USER") password = secret("SQL_PASS")
  database = "ssma"

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("SR_USER") password = secret("SR_PASS")
  database = "test"

read pg query "select * from persons where \"updatedAt\" > NOW() - INTERVAL '90 minutes'"
  | write sr stream_load persons upsert on id
