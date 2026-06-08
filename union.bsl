@http(path = "/extract")

param job json from body

connection erp = sqlserver
  host = "142.0.65.89" port = 37000
  user = env("SQL_USER") password = secret("SQL_PASS")
  database = "CF9JAO_148172_PR_PD"

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("SR_USER") password = secret("SR_PASS")
  database = "test"

for name, source in job.queue @[mode = parallel, on_error = continue]
  union erp json "${source}" @[tag = emp, canon = first, tag_substr = "4,2"]
    | select R_E_C_N_O_, "${name}_EMPRESA" = emp, * except(R_E_C_N_O_, emp), updated_at = now()
    | write sr stream_load "proth_${name:lower}" upsert on R_E_C_N_O_, "${name}_EMPRESA"
