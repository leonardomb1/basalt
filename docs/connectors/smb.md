# SMB file shares

An `smb` connection names a Windows file share or a Samba server, and paths
reach it as `smb://name/share/path` — or `smb://name/path` when the connection
fixes the share:

```sql
CREATE CONNECTION fs TYPE smb OPTIONS (
  host = 'fileserver.corp.local', domain = 'CORP', user = 'svc_basalt',
  share = 'Financeiro'                           -- optional; password from FS_PASS
);
SELECT * FROM 'smb://fs/fechamento/2026-10.xlsx';
LOAD INTO 'smb://fs/exportacao/vendas.parquet' AS SELECT * FROM vendas;
```

Options are `host port user password domain share realm kdc spn auth`;
`user`/`password` follow the `NAME_USER`/`NAME_PASS` convention, `domain` is the
account's (empty for one local to the server), and `port` is 445. Without a
connection, `smb://[domain;]user@host/share/path` logs in as that user with
`SMB_PASSWORD` (`SMB_USER` and `SMB_DOMAIN` filling what the URL leaves out).
Paths use `/`; names are matched as the server matches them, without regard to
case on Windows.

The login is NTLMv2, or Kerberos when the connection names a `realm` — the
domain's DNS name in capitals (`CORP.LOCAL`), not its NetBIOS name. Kerberos is
what an Active Directory account on Azure Files needs (NTLM there serves only
the storage account key), and what a domain that disables NTLM leaves:

```sql
CREATE CONNECTION az TYPE smb OPTIONS (
  host = 'mystorage.privatelink.file.core.windows.net', share = 'dados',
  user = 'svc_basalt', realm = 'CORP.LOCAL'      -- password from AZ_PASS
);
```

basalt asks the KDC for a ticket with the password itself — it holds no system
ticket and reads no keytab — with pre-authentication and AES keys only (RC4 is
refused). The KDC is `kdc = 'host[:port]'` when given, else the one DNS lists
for `_kerberos._tcp.<realm>`, else the realm's own name; the service is
`cifs/<host>`, or `spn` when the server is known by another name (reach a
private endpoint by IP, say, but name it by its DNS name). Tickets are reused
until they expire. The user may be written `me@CORP.LOCAL` or `CORP\me`; it logs
in as `me`, and with `auth = 'kerberos'` the part after `@` serves as the realm
when no `realm` is given. `auth = 'ntlm'` keeps NTLM despite a realm;
`SMB_REALM` and `SMB_KDC` fill in for a plain `smb://` URL. The machine's clock
must be within five minutes of the KDC's.

Every request is signed and every signed reply checked — SMB 2.1 to 3.1.1, the
latter with pre-authentication integrity — which Windows 11 24H2 and Server
2025 require by default and older servers accept. A guest login is refused
rather than taken as success. A share or session that requires encryption gets
it: AES-128-GCM on SMB 3.1.1, AES-128-CCM on 3.0.

Reads are by offset, as on SFTP: a Parquet file or an Excel workbook is read
without fetching what the query does not need, several reads in flight within
the credits the server grants, and a parallel read gets a session per lane;
sessions are pooled per server and login. A write goes to `name.part` and is
renamed over `name` once complete; a failed load deletes the `.part` and leaves
`name` as it was. `APPEND` is not supported, and a missing folder is an error.
