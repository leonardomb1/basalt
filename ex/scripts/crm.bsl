@http(path = "/crm")

# Microsoft Dynamics 365 / Dataverse (TDS endpoint) -> StarRocks bronze.
# Azure AD auth from your AAD username+password (federated/managed auto-detected,
# NO app registration). Resource defaults to the org URL.
#
# IMPORTANT: never SELECT * on Dataverse — it materializes every (incl. computed)
# column and times out. List the columns you need per entity in `cols`.
#
# POST body: [{ "name":"<entity>", "cols":"<col,col,...>", "where":"<T-SQL or ''>" }]
#   curl -X POST localhost:8080/crm -d '[
#     {"name":"opportunity",
#      "cols":"opportunityid, name, statecode, modifiedon",
#      "where":"[modifiedon] > DATEADD(MINUTE, -90, GETDATE())"}
#   ]'
# `where` references a projected column. Entity PK is <entity>id (derived below).

param tables json

connection crm = sqlserver
  host = "kaeferbr.crm2.dynamics.com" port = 1433
  database = "ripbr"
  auth = "aad"
  user = env("USER_CRM") password = secret("USER_CRM_PASSWORD")
  tls = "require"

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("USER_SR") password = secret("USER_SR_PASSWORD")
  database = "bronze"

for name, cols, where in tables @[mode = parallel, on_error = continue]
  read crm query "SELECT ${cols} FROM ${name}" @[where = "${where}", buffer]
    | select *, extraction_timestamp = now()
    | write sr stream_load "crm_${name}" upsert on "${name}id"
