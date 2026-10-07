# SFTP

An `sftp` connection names a file server, and paths reach it as
`sftp://name/path` — absolute, or `sftp://name/~/path` under the login's home:

```sql
CREATE CONNECTION bank TYPE sftp OPTIONS (
  host = 'sftp.banco.com.br', user = 'empresa',
  key_file = '/etc/basalt/bank_ed25519'          -- or a password (BANK_PASS)
);
LOAD INTO IDENTIFIER('retornos/' || $dia || '.parquet') AS
SELECT * FROM IDENTIFIER('sftp://bank/retorno/' || $dia || '.csv');
LOAD INTO 'sftp://bank/remessa/pagamentos.csv' AS SELECT * FROM pagamentos;
```

Options are `host port user password key_file key_passphrase known_hosts
host_key`; `user`/`password` follow the `NAME_USER`/`NAME_PASS` convention, and
the password is optional when a `key_file` logs in. The key is an OpenSSH
Ed25519 key, passphrase-protected or not; an RSA key file is refused with that
advice. Password and keyboard-interactive logins both work. Without a
connection, `sftp://user@host/path` logs in with `~/.ssh/id_ed25519` or
`SFTP_PASSWORD`. A connection name is process-wide: under `basalt serve` every
script sees the latest `CREATE CONNECTION` of it, so give different servers
different names.

The server's host key is checked before anything is sent, against
`known_hosts` (`~/.ssh/known_hosts` by default, hashed entries and `[host]:port`
included) or a pinned `host_key = 'SHA256:…'`. An unknown key is refused with
its fingerprint and the option that pins it, a changed or `@revoked` one is
refused outright — never trusted on first use, as a transfer to the wrong host
is a leak. Ciphers are chacha20-poly1305, AES-GCM and AES-CTR with HMAC-SHA2,
with OpenSSH's strict key exchange.

Reads are by offset, so a Parquet file or an Excel workbook on the server is
read without fetching what the query does not need, and a parallel read gets a
session per lane; sessions are pooled per server and user. A write goes to
`name.part` and is renamed over `name` once complete, so a partner polling the
folder never picks up half a file; a failed load removes the `.part`. `APPEND`
is not supported, and a missing folder is an error rather than created on
someone else's server.
