# Connections

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs',
  tls      = 'require'
);
```

The connection types are `postgres`, `mysql`, `sqlserver`, `starrocks`,
`doris`, `http`, `sftp`, `smb` and `ftp`; any other `TYPE` is a plan-time error.

- `postgres`, `mysql`, `sqlserver`: `host port database user password tls`
  (`off`, `require` or `insecure`). `sqlserver` adds `auth` (`sql`, the default;
  `aad` with `client_id` `resource` `token`; `ntlm`; `kerberos`) and, for
  Windows authentication, `domain realm kdc spn` (below).
- `starrocks` and `doris`: `fe_host`/`host`, `fe_port`/`port`, `user`
  `password`, a required `database`, and for loading `be_url` (also `load_url`)
  `buckets replication_num auto_create label_prefix`.
- `http`, `sftp`, `smb` and `ftp` have pages of their own: [HTTP APIs](../connectors/http.md),
  [SFTP](../connectors/sftp.md), [SMB file shares](../connectors/smb.md) and
  [FTP](../connectors/ftp.md); the
  databases' particulars are under [SQL databases](../connectors/databases.md).

## Credentials

**Credentials by convention:** connection `erp` resolves `ERP_USER` / `ERP_PASS`
from the environment at connect time — the common case costs zero characters.
Explicit `user = ...` / `password = ...` options override the convention. An
`http` connection reads them only for `auth = 'basic'` (and `oauth2` without
`client_id`/`client_secret`), so a public API needs none. Azure Blob paths
(`az://...`, see [Sources](sources.md)) resolve `AZURE_STORAGE_KEY`, and `AZURE_BLOB_ENDPOINT` points
them at an emulator. S3 paths (`s3://...`) resolve `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`, `AWS_REGION`), and
`AWS_ENDPOINT_URL` points them at an emulator such as MinIO. Secrets are never
literals in the script; always environment indirection.

A second `CREATE CONNECTION` of a name replaces the first from that statement
on; `CREATE OR REPLACE CONNECTION` says the same thing explicitly.
