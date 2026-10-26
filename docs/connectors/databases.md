# SQL databases

Postgres, MySQL, SQL Server, StarRocks and Doris are read with a SQL query and
written by bulk load or insert; their options are listed under
[Connections](../language/connections.md). What is particular to each follows.

## PostgreSQL and MySQL

Every session runs in UTC, the zone basalt's timestamps are in: PostgreSQL's
with `TimeZone=UTC`, MySQL's with `SET time_zone = '+00:00'` (the offset form,
which needs no time-zone tables on the server). A PostgreSQL `timestamptz` and a
MySQL `TIMESTAMP` therefore read as the same UTC instant whatever the server's
or database's default zone, a timestamp loaded into a MySQL `TIMESTAMP` column
is stored as that UTC instant, and a pushed-down timestamp literal compares in
UTC. A `DATETIME`, a PostgreSQL `timestamp` and StarRocks' and Doris' columns
carry no zone and read as written.

The session zone also sets the server's own clock in basalt's sessions. On
MySQL, StarRocks and Doris, `NOW()`, `CURRENT_TIMESTAMP` and `FROM_UNIXTIME` in
a query basalt sends give UTC, and so does a `DATETIME` column's `DEFAULT` or
`ON UPDATE CURRENT_TIMESTAMP` filled by a row basalt writes; on PostgreSQL a
`timestamp` column's `DEFAULT now()` is the UTC wall-clock time. Other sessions
on the same server keep their own zone, so a row they write may differ by the
offset.

A PostgreSQL `bytea` reads as `BYTES`, decoded from whichever `bytea_output`
format the server uses, and a PostgreSQL `time` as `TIME`. A basalt `TIME` is a
time of day below `24:00:00`, so PostgreSQL's `24:00:00` fails the read, naming
the column, rather than read as midnight; a `timetz` reads as text. A MySQL
`TIME` reads as text: it may hold an elapsed time (`-838:59:59` to `838:59:59`)
that no time of day can, and `CAST(t AS TIME)` reads one that is a time of day.

A MySQL `BIGINT UNSIGNED` reads as `DECIMAL(20,0)`, as a Parquet `UINT64` and an
Arrow `UInt64` do, so every value up to 18446744073709551615 reads exactly. A
`BIGINT UNSIGNED` primary key still splits a read under `-j`, over its whole
unsigned range.

## StarRocks

A `starrocks` connection is both ends: `LOAD INTO sr.t` writes by stream load
(`be_url`), and `FROM sr.db.t` / `sr.QUERY($$...$$)` reads through the FE's
MySQL protocol (`fe_host`/`host`, `fe_port`/`port`, default 9030) with the same
`user`/`password` — no second `mysql` connection pointed at the FE is needed.
Reads get everything a SQL source gets: `WHERE` and whole-aggregate pushdown in
the StarRocks dialect, key-range splits under `-j`, and `FOR EACH ROW OF
(sr.QUERY(...))` discovery. `tls = 'require'` (or `'insecure'` for a
self-signed certificate) encrypts the FE connection, its DDL and catalog queries
included; it is needed where the FE authenticates by LDAP, as basalt sends a
password in clear text (`mysql_clear_password`) only over TLS, here and on a
`mysql` connection alike.

Before a load, the target's database and table are created only if
`information_schema` does not list them (`auto_create = false` skips this
altogether). A role allowed to load into an existing table therefore needs no
CREATE privilege; one that does lack a privilege it needs gets StarRocks' own
message in the error (`starrocks refused 'CREATE TABLE …': Access denied; you
need …`), not only in the log.

A load streams its rows into one stream-load transaction until the body reaches
`load_size` (default `'128MB'`; a size like `'512MB'` or a byte count) or has
been open five minutes, then commits it and starts the next, so rows become
visible a transaction at a time and a failed run leaves the open one
uncommitted. Each transaction is also written to a spool file in the run's
spill directory (`--spill-dir`, counted against `--spill-cap`; on a RAM-backed
`/tmp` that is memory, up to one `load_size` per lane), so a connection lost
along the way — the server restarting, say — resends it under the same label
instead of failing the load. A load the server refuses is not resent: the run
fails with the server's message. Loads run in strict mode, so a value too long
for its column, or one that does not convert to the column's type, fails the
load instead of landing as NULL.

`compression = 'gzip'` (the default is `'none'`) sends each transaction's body
as one gzip stream, compressed on the fly at a fast level — about 2.5x smaller
on typical rows, at well over 100 MB/s per lane — for a link slower than the
lane. `load_size` still counts the body before compression, so a transaction
holds the same rows either way; the spool keeps the compressed bytes, and a
resend replays them unchanged.

## Doris

A `doris` connection is the same in every respect — Doris is the project
StarRocks forked from, read through its FE and loaded by stream load — except
for the tables it creates. Doris takes no FLOAT, DOUBLE or STRING column as a
key, so an `APPEND` or `REPLACE` target is a duplicate table with no sort key
(`DISTRIBUTED BY RANDOM`), where StarRocks keys on the first column; an `UPSERT
ON (k)` target is a `UNIQUE KEY` table with merge-on-write, its text keys
`VARCHAR(65533)`, and `PARTIAL COLS` loads as Doris' `partial_columns`.
Timestamps are `DATETIME(6)`, to keep microseconds.

## SQL Server

**Named SQL Server instances:** write `host = 'sql01.corp.local\SALES'`. When a `host`
carries a `\INSTANCE` and no explicit `port` is given, basalt resolves the
instance's TCP port via the SQL Server Browser (UDP 1434) before connecting.
Give an explicit `port` to skip the lookup — the robust choice where UDP 1434
is firewalled but the TDS port is open. (`*.dynamics.com` / Azure SQL are
default-instance cloud endpoints, so this never applies there.)

**Windows authentication (`auth = 'ntlm'` or `'kerberos'`):** authenticates to
an on-prem SQL Server with a domain account. Give the domain either inline —
`user = 'CORP\myuser'` — or as its own option, `domain = 'CORP'`; when both
appear the `domain` option wins and the `CORP\` prefix is stripped off the user
name. Either way it is a login **with an explicit password**, *not* single
sign-on from the host's logged-in identity: basalt runs on Linux, holds no
system ticket, reads no keytab, and always needs `password`.

`auth = 'kerberos'` is for a server or domain that refuses NTLM. It needs the
realm — `realm = 'CORP.LOCAL'`, the domain's DNS name, or a user written
`myuser@CORP.LOCAL` — and asks the KDC for a ticket to `MSSQLSvc/<host>:<port>`
with the resolved port (a named instance's included), so `host` is the server's
DNS name as registered in AD; when it is not (an IP, an alias), `spn` names the
service: `spn = 'MSSQLSvc/sql01.corp.local:1433'`. The KDC is `kdc =
'host[:port]'`, else the one DNS lists for `_kerberos._tcp.<realm>`, else the
realm's own name; AES keys only, RC4 refused, tickets reused until they expire.
The server's reply must prove it holds the service's key, or the login is
refused. The channel is always TLS (`tls = 'off'` is taken as `'require'`).
Extended Protection set to *Required* on the server is not supported.

Encryption is mandatory for `auth = 'ntlm'`: `tls = 'off'` is a plan-time
error, refused before any socket opens, because an unencrypted NTLM exchange
hands the challenge/response to any passive observer for offline cracking.
`tls = 'require'` verifies the server certificate and is the right setting;
`tls = 'insecure'` encrypts without verifying and is accepted, since on-prem
instances usually present a self-signed certificate — but an unverified channel
still leaves an active man-in-the-middle able to relay the handshake. Point
`BASALT_CA_BUNDLE` at the PEM of your internal CA and use `tls = 'require'` to
close that gap. Note that NTLMv2 never puts the password on the wire — only a
challenge-response derived from it — whereas a SQL login sends it under
LOGIN7's trivially reversible scrambling, so on an unverified channel NTLM is
the stronger of the two, not the weaker.
