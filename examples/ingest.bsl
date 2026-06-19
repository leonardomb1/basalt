@http(path = "/ingest")

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("SR_USER") password = secret("SR_PASS")
  database = "test"

read request
  | select kind        = event,
           value       = cast(value as int),
           ingested_at = now()
  | filter kind != "debug"
  | write sr stream_load events_log append
