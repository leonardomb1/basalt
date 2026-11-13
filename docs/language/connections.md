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
  (`off`, `require` or `insecure`). `require` verifies the server's certificate
  against the system's roots and, when `BASALT_CA_BUNDLE` names a PEM file, its
  certificates too, as an internal CA needs; a file that cannot be read fails the
  connection, naming it. `sqlserver` adds `auth` (`sql`, the default;
  `aad` with `client_id` `resource` `token`; `ntlm`; `kerberos`) and, for
  Windows authentication, `domain realm kdc spn` (below).
- `starrocks` and `doris`: `fe_host`/`host`, `fe_port`/`port`, `user`
  `password`, a required `database`, and for loading `be_url` (also `load_url`)
  `buckets replication_num auto_create label_prefix load_size compression`.
- Every database, `sftp` and `smb` connection also takes `connect_timeout`
  (seconds, default 60) and `query_timeout` (seconds, default none); see
  [Timeouts and dead peers](../connectors/databases.md#timeouts-and-dead-peers).
- An option the type does not read is a plan-time error, as is a database
  connection with no `host`.
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

An option's value is a literal, `env('NAME')` / `secret('NAME')`, or the script's
own: a `$name` that names a PARAM or LET takes its value, and a `${...}` in a text
option (or in the connection's `PAGINATE` / `WITH`) is filled in from them, once,
before any read — as in a read's own `WITH`. So a value computed from a secret
reaches the connection:

```sql
LET token = sha256(env('API_PASS') || strftime(now(), '%d/%m/%Y'));
CREATE CONNECTION api TYPE http OPTIONS (base_url = 'https://api.example.com',
  auth = 'header', header_name = 'X-Token', header_value = $token);
```

`basalt check` leaves such a value to the run, as it does an `env()`, but refuses
a `$name` that no PARAM or LET declares.

A second `CREATE CONNECTION` of a name replaces the first from that statement
on; `CREATE OR REPLACE CONNECTION` says the same thing explicitly.
