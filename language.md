# The Basalt SQL Language (`.sql`)

Basalt scripts describe a **columnar data pipeline**: read from a source,
transform with a query, write to a sink. A script is plan-time static — parsed,
type-checked, and planned once, then executed as a streaming pull pipeline.

This is the reference for the SQL dialect, derived from the parser
(`src/lang/sql_parser.zig`); it reflects what the engine actually accepts.
Basalt SQL is the only dialect: the BSL (`.bsl`) parser was removed in v0.2.0 —
`examples/golden/` holds the frozen plans that gated the removal.

1. [Program structure](#1-program-structure)
2. [Parameters](#2-parameters)
3. [Connections](#3-connections)
4. [Sink — `LOAD INTO`](#4-sink--load-into)
5. [Queries](#5-queries)
6. [`UNION`, `INTERSECT`, `EXCEPT` and `UNION ALL BY NAME`](#6-union-intersect-except-and-union-all-by-name)
7. [`FOR EACH ROW OF` and the `CASE` statement](#7-for-each-row-of-and-the-case-statement)
8. [HTTP mode](#8-http-mode)
9. [Expressions](#9-expressions)
10. [Running & exit codes](#10-running--exit-codes)
11. [Designed but not yet implemented](#11-designed-but-not-yet-implemented)

---

## 1. Program structure

A script is a sequence of `;`-terminated statements:

```sql
@include 'lib.sql';               -- top of file only; C-style, relative to this file
CREATE ENDPOINT '/x' DOC '...';   -- only for HTTP mode; absent = batch
PARAM ...;                        -- request/CLI inputs
LET name = <expr>;                -- sealed plan-time constant (§2)
THROW 'msg' WHEN <condition>;     -- fail the plan on the script's own invariant
CREATE CONNECTION ...;            -- named data endpoints
CREATE FUNCTION f(a) AS <expr>;   -- scalar functions (inlined at plan time)
CREATE FUNCTION p(a) AS ... END;  -- statement functions, invoked with CALL (§9)
CREATE FUNCTION t(a) RETURNS TABLE AS SELECT ...;  -- table functions, read with FROM t('x') (§9)
LOAD INTO ... AS <query>;         -- output pipeline(s)
<query>;                          -- terminal SELECT = print to stdout
CALL p('x');                      -- run a statement function
FOR EACH ROW OF (...) ... END FOR;
CASE ... END CASE;                -- plan-time dispatch
PRINT <expr>;                     -- progress line on stderr, via the run log
```

`@include` splices another script's declarations ahead of this one at plan
time: each included file is parsed separately (errors report the included
file's own path and line), includes may nest (depth 16, cycles rejected), and
paths resolve relative to the including file. A file is spliced in **once**, at
its first include, like C's `#pragma once`: when `outliers.sql` and
`dispersion.sql` both include `stats.sql` and a script includes both, `stats.sql`
is in the program one time — its `CREATE FUNCTION`s are not defined twice — and
each library still sees what it declares. That holds for every statement in it,
so an included file's `LOAD INTO` runs once however many paths reach it.

`THROW <message> [WHEN <condition>];` asserts what the engine cannot infer.
Both operands are ordinary expressions over `$params` and `$lets` (§9), so they
are decided at plan time: an absent or true condition aborts the script before a
row is read, with `message` as the error text verbatim; a false condition is a
no-op. `basalt check` rejects a script whose guard fires, so a bad invocation is
caught without connecting to anything. A fired guard is permanent, never
transient — exit `1`, never `75` (§10), so a scheduler will not retry it.

```sql
THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
THROW 'since must be an ISO date' WHEN $since <> '' AND length($since) < 10;
THROW 'unreachable branch';       -- unconditional, e.g. in a CASE arm
```

- **Batch is the silent default.** A script with no `CREATE ENDPOINT` runs once
  to completion (exit codes in §10).
- Keywords are case-insensitive; identifiers keep their case.
- Comments: `--` to end of line, `/* ... */` blocks.
- Strings are `'...'` (double `''` for a literal quote), and only `'...'`.
- **Quoted names** are `"..."` (ANSI): `"Exchange rate"` is the column of that
  name, not the text. It is the only way to name a column containing a space or
  spelling a keyword — `SELECT "select", "Valor Total" FROM ...` — and a quoted
  name is never read as a keyword. Double `""` for a literal quote inside one.
  A misspelled quoted name fails as `unknown field`, at plan time.
- **Raw SQL literals** use Postgres dollar-quoting: `$$...$$`, or
  `$tag$...$tag$` when the body contains `$$`. No escaping inside; `${...}`
  interpolation of loop vars still applies within them (§7).
- **Dynamic names** (per-row table/sink names, keys) use `$var` +
  `IDENTIFIER()` + `||`, not raw string interpolation — see §7.

**`PRINT <expr>;`** emits one progress line where it stands — the way a long
`FOR EACH` or `CALL` says what it is doing. The argument is an ordinary
expression (literals, `||`, `$params`, `$lets`, and inside a `FOR EACH` or
statement-function body the loop variables, bound per row); non-strings render
as they would in a sink. It writes to **stderr through the run log at `info`**,
never stdout — stdout is the data contract (`--format json` NDJSON rows or the
summary object), and a progress line there would corrupt it. So `PRINT`
inherits the log settings: `--log-format json` carries the text as the `msg`
field of an NDJSON line, and the default level is `warn`, so a `PRINT` only
appears under `--log-level info` (or `debug`); `-q` silences it. `PRINT` is not
an output pipeline — a script still needs a `LOAD INTO` or a terminal query.

## 2. Parameters

```sql
PARAM dias   INT DEFAULT 7;              -- batch: -p dias=3 | http: query string
PARAM desde  TIMESTAMP;                  -- no default = required
PARAM job    JSON FROM BODY;             -- whole JSON body as a document
PARAM tenant STRING FROM HEADER('X-Tenant');
```

- Reference with `$`: `$dias`, `$desde`. JSON documents navigate by dotted
  path — `$job.tables`, `$job.source.host` — resolved to literals at plan time.
- Safe navigation: `$job.filtro?.uf` — a missing intermediate resolves the
  whole path to `null` instead of erroring.
- Types: `BOOL INT FLOAT STRING BYTES DATE TIME TIMESTAMP DECIMAL(p,s) JSON`
  (common synonyms accepted: `INTEGER BIGINT DOUBLE TEXT VARCHAR(n) DATETIME
  NUMERIC ...`). A value bound from text — `-p`, a kernel's `params`, a
  default — is read as a `CAST` reads it and keeps the declared type:
  `-p d=2026-02-01` is a `DATE` (`date_add('day', 1, $d)` works), a date alone
  bound to a `TIMESTAMP` is its midnight, `TIME` takes `HH:MM[:SS[.ffffff]]`,
  and a `DECIMAL(10,2)` rounds `12.345` to `12.35`. Text that is not one fails
  the run, and `check`, with the value named (`PARAM d: 'nope' is not a date`).
- Source defaults: scalars bind from the query string, `JSON` from the body.

**`LET name = <expr>;`** is PARAM's sealed sibling: a script-scoped constant
folded once at plan time (in declaration order; it may reference `$params` and
earlier `$lets`) and referenced as `$name`. It can never be bound externally —
`-p name=...` is an error, HTTP binding ignores it, and it is not part of an
endpoint's parameter surface. A LET and a PARAM may not share a name. `LET
run_ts = now();` gives one consistent timestamp across every pipeline of a run.

`LET name = (SELECT ...);` binds the single cell of a query instead, run when
the statement is reached (so it may read anything the script can) — see
*Subqueries in a predicate* (§5). An expression LET cannot reference a query
LET: expression LETs fold before any query runs.

## 3. Connections

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs',
  tls      = 'require'
);
```

Connector types and their options are unchanged from BSL: `sqlserver`
(`host port database user password tls auth domain tenant client_id resource`),
`mysql`, `postgres`, `starrocks` (`fe_host fe_port be_url database buckets
replication_num auto_create label_prefix ...`), `http`.

A `starrocks` connection is both ends: `LOAD INTO sr.t` writes by stream load
(`be_url`), and `FROM sr.db.t` / `sr.QUERY($$...$$)` reads through the FE's
MySQL protocol (`fe_host`/`host`, `fe_port`/`port`, default 9030) with the same
`user`/`password` — no second `mysql` connection pointed at the FE is needed.
Reads get everything a SQL source gets: `WHERE` and whole-aggregate pushdown in
the StarRocks dialect, key-range splits under `-j`, and `FOR EACH ROW OF
(sr.QUERY(...))` discovery.

Before a load, the target's database and table are created only if
`information_schema` does not list them (`auto_create = false` skips this
altogether). A role allowed to load into an existing table therefore needs no
CREATE privilege; one that does lack a privilege it needs gets StarRocks' own
message in the error (`starrocks refused 'CREATE TABLE …': Access denied; you
need …`), not only in the log.

**Named SQL Server instances:** write `host = '10.110.2.5\WMS'`. When a `host`
carries a `\INSTANCE` and no explicit `port` is given, basalt resolves the
instance's TCP port via the SQL Server Browser (UDP 1434) before connecting.
Give an explicit `port` to skip the lookup — the robust choice where UDP 1434
is firewalled but the TDS port is open. (`*.dynamics.com` / Azure SQL are
default-instance cloud endpoints, so this never applies there.)

**Windows authentication (`auth = 'ntlm'`):** authenticates to an on-prem SQL
Server with a domain account. Give the domain either inline —
`user = 'CORP\myuser'` — or as its own option, `domain = 'CORP'`; when both
appear the `domain` option wins and the `CORP\` prefix is stripped off the user
name. This is **NTLMv2 with an explicit password**. It is *not* Kerberos and
*not* single sign-on from the host's logged-in identity: basalt runs on Linux,
holds no ticket, and always needs `password`. A server that mandates Kerberos
will refuse it.

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

**Credentials by convention:** connection `erp` resolves `ERP_USER` /
`ERP_PASS` from the environment at connect time — the common case costs zero
characters. Explicit `user = ...` / `password = ...` options override the
convention. An `http` connection reads them only for `auth = 'basic'` (and
`oauth2` without `client_id`/`client_secret`), so a public API needs none. Azure Blob paths (`az://...`, §5) resolve `AZURE_STORAGE_KEY`, and
`AZURE_BLOB_ENDPOINT` points them at an emulator. S3 paths (`s3://...`, §5)
resolve `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ optional
`AWS_SESSION_TOKEN`, `AWS_REGION`), and `AWS_ENDPOINT_URL` points them at an
emulator such as MinIO. Secrets are never literals in the script; always
environment indirection.

`CREATE OR REPLACE CONNECTION` re-declares an existing name.

**HTTP connections** hold what every read of an API shares. After `OPTIONS`,
an `http` connection takes the REST source clauses (§5) — `PAGINATE`, `RETRY`,
`WITH (...)` — as defaults each read starts from; a read's own clauses win:

```sql
CREATE CONNECTION gh TYPE http OPTIONS (base_url = 'https://api.github.com',
    auth = 'bearer', token = env('GH_TOKEN'))
  RETRY 3 ON (429, 503)
  WITH (header = 'Accept: application/vnd.github+json');
```

`auth` is `bearer` (`token`), `basic` (`user`/`password`, which default to the
`NAME_USER`/`NAME_PASS` convention), `header` (`header_name`/`header_value`,
for API keys), `login_json` or `oauth2` (`token_url`, `client_id`,
`client_secret`, `scope`). With no `auth`, no credentials are read at all.

`CREATE RESOURCE conn.name AS GET(...) | POST(...) [PAGINATE ...] [RETRY ...]
[WITH (...)];` names one endpoint, so it reads like a table:

```sql
CREATE RESOURCE gh.repos AS GET('/orgs/ziglang/repos', type = 'public')
  PAGINATE BY page (param = 'page', size = 100);

SELECT name, stargazers_count FROM gh.repos WHERE NOT archived;
```

`SHOW TABLES FROM gh` lists a connection's resources (`resource`, `method`,
`path`) and `DESCRIBE gh.repos` fetches it to print its columns. Reading
`gh.name` that no `CREATE RESOURCE` declared is a plan-time error that says so.

## 4. Sink — `LOAD INTO`

```sql
LOAD INTO sr.silver.pedidos            -- conn[.schema].table, or a quoted path
  USING stream_load                    -- physical adapter (connector verb)
  UPSERT ON (empresa, num_pedido)      -- disposition (below)
  SPLIT BY (num_pedido) JOBS 4         -- key-range parallel load
  WITH (label_prefix = 'noturno')      -- residual connector knobs
AS
<query>;
```

- File target by quoted path — the extension picks the writer:
  `LOAD INTO '/out/x.csv'`, `LOAD INTO '/out/x.parquet'`,
  `LOAD INTO '/out/x.arrow'` (Arrow IPC file; also `.feather`, `.ipc`, and
  `.arrows` for the stream format, local paths only), or an object-store
  path `LOAD INTO 'az://account/container/bronze/x.parquet'` or
  `LOAD INTO 's3://bucket/bronze/x.parquet'`.
- A per-row dynamic target uses `IDENTIFIER(<string-expr>)` over loop vars
  (§7): `LOAD INTO sr.IDENTIFIER('crm_' || lower($name)) ...`.
- Dispositions on a **table target**: `APPEND` (default, omissible) · `REPLACE`
  (overwrite) · `UPSERT ON (k1, k2)` · `UPSERT ON (id) PARTIAL COLS (a, b)` ·
  bare `UPSERT` (infer the PK from the source table's metadata at plan time —
  needs a table read on a SQL source that exposes it). An empty/unresolved
  upsert key is an error, never a silent no-op.
- Dispositions on a **file target** — the omissible default is *not* `APPEND`:
  a bare `LOAD INTO 'x.csv'`, like `REPLACE`, creates or truncates the file, so
  a rerun replaces it. Explicit `APPEND` accumulates for CSV only: the file is
  opened without truncating and the header row is written only when it was
  absent or empty. Explicit `APPEND` is a plan-time error for `.parquet` and
  Arrow IPC (the footer indexes every row group or batch and is written last,
  so appending means rewriting the file) and for `az://` / `s3://` (an object is replaced on
  write, never extended) — use `REPLACE`, a per-run path, or `INTO BUFFER` (§8).
- `SPLIT BY (col)` parallelizes the load by key ranges; `JOBS n` fixes the
  lane count (otherwise the CLI `-j` applies).

**stdout is not syntax**: a terminal `SELECT ...;` statement prints the result
as an aligned table — `basalt run -c "SELECT * FROM 'x.csv'"` works as a
mini-DuckDB.

## 5. Queries

```sql
WITH pedidos AS (                          -- CTE = named binding
  SELECT filial, num, valor, obra
  FROM erp.dbo.SC5010
    PUSHDOWN($$D_E_L_E_T_ <> '*'$$)        -- raw predicate, verbatim to the source
  WHERE valor > 0                          -- translated predicate
), obras AS (                              -- a join's right side is a CTE
  SELECT codigo_obra, nome_obra FROM erp.dbo.obras
)
SELECT p.filial, p.num, o.nome_obra
FROM pedidos p
LEFT JOIN obras o ON p.obra = o.codigo_obra
ORDER BY p.num DESC
LIMIT 100 OFFSET 20;
```

### Sources (`FROM ...`)

| source | syntax |
|--------|--------|
| SQL table | `FROM erp.dbo.SC5010` |
| SQL table (per-row name) | `FROM erp.dbo.IDENTIFIER($name)` (§7) — still a table read |
| raw query | `FROM erp.QUERY($$SELECT ...$$)` (no dialect translation) |
| file — CSV, Parquet or Arrow IPC | `FROM 'path.csv'` / `FROM 'path.parquet'` / `FROM 'path.arrow'` — the extension picks the reader; local or HTTPS URL (Arrow IPC: local only). Any other extension is a plan-time error unless `WITH (format = 'csv' \| 'parquet' \| 'arrow')` names one |
| compressed file | `FROM 'path.csv.gz'` / `.csv.zst` — the inner name picks the reader |
| file inside a zip | `FROM 'archive.zip :: inner.csv'`, or just `FROM 'archive.zip'` when it holds one file |
| object storage | `FROM 'az://account/container/path.parquet'` or `FROM 's3://bucket/key.parquet'`; a trailing `/` reads every object under the prefix as one table |
| REST (connection) | `FROM crm.GET('/v1/customers', status = 'open')` — path on the conn's base URL; each `name = value` is a URL-encoded query param, and path and values are expressions (`'/v1/customers/' \|\| $id`). `crm.POST('/search', body = $$...$$)` sends a body. `crm.'/v1/customers'` is the older spelling of a bare GET |
| REST resource | `FROM crm.customers` — an endpoint named with `CREATE RESOURCE` (§3) |
| REST (raw URL) | `FROM HTTP('https://host/api/x')` — the URL exactly as written, the way `QUERY()` is raw SQL |
| request body | `FROM BODY (col TYPE [NOT NULL], ...)` (§8) |
| durable buffer | `FROM BUFFER 'name'` (§8) |
| discovered union | `FROM EACH TABLE OF (...)` (§6) |
| generated integers | `FROM RANGE(10)` / `FROM RANGE(2, 5)` — `lo..hi-1` as a `range` column; bounds are int literals or params |
| no source | `SELECT 1 AS x, now() AS t;` — a `SELECT` with no `FROM` yields one row of computed values |
| CTE | `FROM <name>` |
| table function | `FROM paid_orders($since) p` — a `CREATE FUNCTION ... RETURNS TABLE` (§9), also as a `JOIN`'s right side |

A raw `QUERY($$…$$)` is sent as it is and may hold several statements — a
`DELETE` and an `INSERT` before its `SELECT`, a statement after it. The batch runs
to its end: every statement executes, and an error in any of them fails the
read with the server's message. It may return one result set; a second is an
error (`the query returned more than one result set`) rather than dropped or,
if it has the same columns, appended as if it were more rows. A read that stops
early — a `LIMIT`, a row cap — cancels whatever of the batch had not run yet.
MySQL and StarRocks refuse a multi-statement `QUERY` outright.

A SQL Server session is opened as SSMS and the ODBC, JDBC and .NET drivers open
one: `ANSI_WARNINGS`, `ANSI_NULLS`, `ANSI_PADDING`, `ANSI_NULL_DFLT_ON`,
`QUOTED_IDENTIFIER`, `CONCAT_NULL_YIELDS_NULL` and `ARITHABORT` on. So a value too
long for its column is refused (`String or binary data would be truncated`)
rather than cut, a division by zero is an error rather than NULL, a `varchar`
keeps its trailing spaces, a column created without `NULL` / `NOT NULL` allows
nulls, and a table with a filtered index or an indexed view can be written.

A CSV column's type is sniffed from the first 1024 rows: int ⊂ float ⊂ string,
and a column whose every non-empty, unquoted cell is an ISO `YYYY-MM-DD` reads as
a `DATE` — so `WHERE day >= '2026-01-01'`, `date_add`, `date_diff` and `EXTRACT`
work on it directly, and a parquet sink stores it as a date. A string function
on it (`substr(day, 1, 7)`, `day LIKE '2026%'`) still sees the ISO text. A cell
past the sample that does not parse as the inferred type is an error, never a
silent coercion. Timestamps are not sniffed; `CAST(ts AS TIMESTAMP)` as before.

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched. A
remote `.parquet` (`https://...`, `az://...`, `s3://...`) is read the same way, by HTTP
range request, so a projected query transfers only the chunks it decodes; a
server that ignores `Range` falls back to one whole-object fetch. Parquet writes
store `DECIMAL` in the narrowest physical type its precision allows — INT32 to 9
digits, INT64 to 18, FIXED_LEN_BYTE_ARRAY up to 38; past 38 digits (the engine's
own ceiling) a value is refused rather than silently truncated.

Arrow IPC — `.arrow`, `.feather`, `.ipc` (the file format, Feather v2) or
`.arrows` (the stream format) — is how a dataframe reaches basalt fastest: the
file is memory-mapped and its columns copied out, so polars' `write_ipc` or
pyarrow's `write_feather` hands a frame over with no encode or decode step.
Both formats read, uncompressed or with LZ4-frame or ZSTD buffers, and only
the columns a query uses are converted. Every integer width reads as `int` (a
`UInt64` above 2^63−1 is an error, never a wraparound), floats as `float`,
all three string layouts (`utf8`, `large_utf8`, polars' default `utf8_view`)
as `string` and the binary ones as `bytes`, `decimal128` as `decimal`, and
dates, times, timestamps and durations in any unit as basalt's own
(nanoseconds floor to the microsecond; a zoned timestamp is its UTC
wall-clock). Dictionary-encoded columns — a polars `Categorical` — read as
their values. Lists, structs and maps read as JSON text, so `json_get` and
`CROSS JOIN UNNEST(JSON_EACH(col))` reach inside them. Unions and run-end or list-view
encodings are refused with the column named. Only the first stream of a
`.arrows` holding several is read. An Arrow read is one lane; `-j` does not
split it yet.

A parquet column's type comes from its `LogicalType` annotation when the writer
set one, else from the legacy `ConvertedType`. That matters for files from
polars, DuckDB, Spark or pyarrow: they omit the legacy annotation on a naive
timestamp and on every nanosecond one, so without the logical type those
columns would read as bare `int`. `TIMESTAMP` and `TIME` in milliseconds,
microseconds or nanoseconds all read as basalt's microsecond `timestamp`/`time`;
nanoseconds floor to the microsecond. A UTC-adjusted timestamp reads as its UTC
wall-clock time: basalt has no zoned timestamp, so `isAdjustedToUTC` is not
carried. `DATE`, `DECIMAL`, `STRING`/`ENUM`/`JSON` and `INTEGER` map as their
converted twins do; a `UUID` stays `bytes`.

A struct's fields read as flat dotted columns (`addr.city`). Everything
repeated — a `LIST` of scalars or of structs, a `MAP`, and any nesting of them —
reads as one `string` column named for it, holding each row's value as JSON:
lists as arrays (`[1,2]`, `[]` when empty), structs as objects, maps as objects
keyed by each key's text (`{"k1":3}`), `NULL` for a null column value and
`null` for a null element or field, dates and timestamps as quoted text.
`CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag` gives a row per element and
`json_get` reaches into one. No column of the file is left out, so
`LOAD INTO 'copy.parquet' AS SELECT * FROM 'x.parquet'` carries every one — the
nested ones as JSON text, since that is how basalt holds them. A filter on a
nested column is never used to skip row groups: the file's statistics describe
its leaves, not the JSON.

Source clauses, in any order after the source:

- **`PUSHDOWN(<expr>)`** — a raw predicate sent verbatim into the generated
  source query's `WHERE` (the successor of BSL `@[where]`). The argument is a
  string expression: a `$$...$$` literal (`PUSHDOWN($$D_E_L_E_T_ <> '*'$$)`),
  a loop-var value (`PUSHDOWN($where)`), or one built with `||`. ANDed with
  whatever the translated `WHERE` pushes down. Empty ⇒ no clause. Syntax errors
  surface at the source at runtime (permanent, exit 1).
- **Projection pushdown** — a table read asks the source only for the columns
  the pipeline provably needs (`SELECT a, b FROM erp.t` is sent as
  `SELECT [a], [b] FROM t`), so a narrow read of a 300-column table no longer
  moves 300 columns. `SELECT * EXCEPT (...)` — after a table read or a union of
  table reads — first asks the source for the table's shape and no rows, then
  names every column but the excepted ones, so a column nobody wants never
  leaves the server (that 79 GB XML column stays where it is). A plain
  `SELECT *` still fetches everything; a split-parallel read adds its key column
  to the list.
- **Implicit pushdown** — the contiguous `WHERE` (filter) prefix directly after
  a SQL table/query read is translated into that source query's `WHERE`
  automatically. A join no longer blocks it: a filter naming only columns the probe
  side already had is moved below the join first, and a filter on a join *key* also
  gains a twin on the other side's key — so `FROM fact JOIN dim ON fact.k = dim.k
  WHERE dim.k = '…'` prunes the fact table at the source. Both moves are refused
  where they would change the answer: never below a `RIGHT`/`FULL` join (there the
  probe side is the one that gets null-extended), never when a referenced column
  could have come from the right side, and the key twin only under an inner join.
  `EXPLAIN` shows where the filter ended up. Translatable: comparisons,
  `AND`/`OR`/`NOT`, `IS [NOT] NULL`/`EMPTY`, `IN`, `LIKE`, `CASE`/`IF`, `CAST` (but
  not one converting text to a number — the sources disagree about what
  `CAST('abc' AS INT)` means, NULL in StarRocks and MySQL against an error in
  Postgres and here, so descending it would make the answer depend on whether it
  descended; put the expression in a raw `QUERY(...)` to ask for the source's own
  coercion), and the portable string
  functions (`lower upper trim substr replace concat coalesce starts_with
  ends_with contains`). A `$param`, LET or loop variable descends
  as its value — `WHERE D2_EMISSAO >= $since` sends `>= '20240105'`, with a
  query LET's value decided first. Untranslatable pieces (arithmetic,
  `now()`/`today()`, user funcs) stay in the engine — the filter is always
  kept, so results never change, only how much crosses the wire. `EXPLAIN`
  prints the descended predicate on a `pushdown:` line. A CTE, derived table or
  table function the query *starts from* counts as that read: its own `WHERE`
  descends as if written inline (`(via binding …)` in `EXPLAIN`), except a binding
  holding a window function, which stays apart so a `WHERE rn = 1` over it can run
  as a top-N. A filter written after a `SELECT` list — the query's own `WHERE`
  over a CTE, derived table or table function — moves in front of it when every
  column it names is one that list passes through or renames, rewritten in the
  source's names: `SELECT num FROM paid_orders($d) WHERE num > 5`, with the body
  selecting `C5_NUM AS num`, sends `"C5_NUM" > 5`. A conjunct naming a computed
  column (`x * 2 AS y`) stays above the list, the others still move. Rows a
  moved filter removes are no longer evaluated by the list, so a computed column
  that would have failed on one of them no longer fails the query. A CTE,
  derived table or table function read as a `JOIN`'s right side is readied the
  same way — its own WHERE descends into its read and only the columns it uses
  are asked for — so `JOIN itens($filial) i` reads that branch's rows, not the
  table. A filter written *after* an inner join that names only the right side's
  columns, each by its alias (`WHERE i.valor > 0`), joins that read too — and
  descends with it. After a `LEFT`, `RIGHT` or `FULL` join it stays above the
  join, since there it also decides the unmatched rows; so does a condition
  naming both sides, or a bare column name, which could be either side's.
- **Text comparisons follow the column's collation**, so the source never keeps
  fewer rows than basalt would. basalt compares text byte by byte; a source
  compares by collation — case-insensitive by default on SQL Server and MySQL,
  and SQL Server ignores trailing spaces (`'ab   ' = 'ab'`) under every
  collation, binary ones too. Sent as written, `code >= 'B'` lost `'a…'` rows on
  a case-insensitive server, and `D2_DOC > '000123'` lost the padded
  `'000123   '` on a Protheus `char(n)`. So, asking the source's catalog once
  per run and table, and only when a text comparison is in play:
  - `=`, `IN`, `LIKE` and the prefix/suffix/contains tests descend under any
    collation — folding and padding only let the source match more, and the
    engine re-applies the filter.
  - `<`, `<=`, `>`, `>=` on text descend only where the column compares bytes (a
    `_BIN2` collation on SQL Server, or `_BIN` on a `char`/`varchar`; `_bin`
    on MySQL; on Postgres `C`, `POSIX`, the builtin provider, or any libc
    collation on a musl build — as the server reports it, never an ICU one;
    StarRocks always) and the literal is printable ASCII; where the collation
    also pads, `>` is sent as `>= 'x' OR col LIKE 'x%'` and `<` as `<=`,
    which keeps every row basalt keeps. Elsewhere they stay in the engine.
  - `<>`, `NOT (… = …)` and `IS NOT EMPTY` on text negate an equality the
    collation widens, so they descend only where it compares bytes — on
    SQL Server with an exact form, `NOT (col = 'x' AND DATALENGTH(col) = 1)`.
  - `length()` and `strpos()` never descend (MySQL's `LENGTH` counts bytes, SQL
    Server's `LEN` drops trailing spaces), nor a `LIKE` pattern holding `_` (a
    byte in a SQL Server `varchar` under a UTF-8 collation); SQL Server's `[` is
    escaped, and on SQL Server a literal outside printable ASCII stays in the
    engine.

  `EXPLAIN` does not connect, so it says `text comparisons decided by the
  collation at run time` where the catalog will decide.
- **Whole-aggregate pushdown** — `read <sql> | filters | GROUP BY` descends as
  one grouped query when every filter translates, the group keys are bare
  columns, and the aggregates are `COUNT[(DISTINCT)] SUM MIN MAX` with types
  the engine can pin via explicit casts (`AVG`, summed floats/decimals, and
  collation-dependent string extremes deliberately stay engine-side — the
  result must be bit-identical, not merely close). `HAVING`/sort/limit still
  run in the engine on the tiny grouped result.
  Nothing re-applies the `WHERE` over a grouped result, so each filter must
  keep *exactly* basalt's rows: a text comparison descends only where the
  column's collation compares bytes — on a Protheus binary collation as
  `D2_FILIAL = '01' AND DATALENGTH(D2_FILIAL) = 2`, since SQL Server would also
  count `'01 '`. The top-N descent below holds its filters to the same rule.
  When an aggregate over a SQL source does *not* descend, every matching row
  is streamed to the engine to be grouped — the run log says so in a `warn`
  line that names the rule that refused it (`the WHERE predicate does not
  translate whole to mysql SQL`, ``group key `x` is renamed by the
  aggregate``, …).
- **`LIMIT` and top-N pushdown** — `read <sql> | filters | [SELECT] | [ORDER BY]
  | LIMIT n [OFFSET m]` asks the source for `n + m` rows: `LIMIT` on
  postgres/mysql/StarRocks, `TOP` on sqlserver. Without `ORDER BY` any `n + m`
  rows are an answer, so it always descends. With one, the source orders them
  first — nulls last in both directions, as the engine does (`NULLS LAST`;
  a leading `k IS NULL` key on mysql/StarRocks; a `CASE` key on sqlserver) —
  and the engine still sorts, offsets and cuts what arrives, so the final order
  is its own. It descends only when the answer cannot change:
  - every `WHERE` before the limit translates (§7's rules) — the source counts
    rows after its own filter, so one left here would thin the capped set;
  - each `ORDER BY` key is a source column, as-is or renamed by the `SELECT`
    (`SELECT id AS k … ORDER BY k`), not a computed one;
  - each key is a number, date, time or timestamp. A string key stays
    engine-side: its order is the source's collation — case-folded by default
    on mysql and sqlserver, space-padded for `char(n)` — where the engine
    compares bytes, the same reason string `MIN`/`MAX` do not descend.

  Rows tied on every key at the cut are interchangeable either way: which of
  them the engine keeps already depends on the order rows arrive in, which a
  SQL source never promises. A `DISTINCT`, a join or an aggregate before the
  limit is not this shape (the aggregate has its own descent above). The
  capped read runs as one statement, so it is not split into key ranges under
  `-j`. A top-N that does not descend streams every matching row here to be
  sorted, and the run log says so in a `warn` line naming the rule. `EXPLAIN`
  shows it on the scan's `pushdown:` line — `order by id desc limit 1000 (if
  the keys are numeric or temporal)`, since analysis does not connect to learn
  the key's type — and `physical: serial (top-N pushed, sorts at most 1000
  rows)` in place of `materializes`.
- **`PAGINATE BY page|offset|cursor (param = 'page', size = 100,
  total = 'count', field = 'next', start = 2, max = 50)`** — REST pagination.
  Friendly keys map to the engine hints (`param`→`page_param`/`cursor_param`,
  `size`→`page_size`, `total`→`total_field`, `field`→`cursor_field`,
  `start`→`start_page`, `max`→`max_pages`); unknown keys pass through.
- **`RETRY n [ON (429, 503)]`** — retries + retryable statuses.
- **`WITH (delimiter = ';', encoding = 'latin1')`** — the CSV dialect. The
  delimiter is one character, or the word `tab`; the encodings are `utf8`
  (default), `latin1` / `iso-8859-1`, and `cp1252` / `windows-1252`. Non-UTF-8
  input is decoded to UTF-8 as it is read, so everything downstream — comparisons,
  `length()`, a parquet sink — sees proper text. A local file read with a dialect
  still fans out over byte-range chunks under `-j`, each lane decoding its chunk
  in that dialect. Multi-byte encodings are not supported: they would break that
  chunking. A file whose bytes are not what you claimed does not fail, it just yields
  mojibake, so prefer the publisher's stated encoding over guessing. The delimiter
  is also accepted on a `LOAD INTO` file target; `encoding` is not — a CSV sink
  always writes UTF-8, and being told otherwise is an error rather than ignored.
### Compressed files and archives

A compression suffix is read through: `FROM 'orders.csv.gz'` and `FROM 'orders.csv.zst'`
decompress as they stream, and it is the *inner* name that picks the reader. Both
work over HTTP too. `.xz` is not supported (std's decoder has the wrong shape for
this reader), nor is bzip2.

A file inside a zip is addressed with `::`, the separator ClickHouse uses for the
same idea:

```sql
SELECT COUNT(*) FROM 'inf_diario_fi_202607.zip :: inf_diario_fi_202607.csv'
  WITH (delimiter = ';');

SELECT * FROM 'cnpj.zip';        -- an archive holding one file needs no `::`
```

An archive holding **more than one** file and no `::` is a plan-time error naming
the candidates, rather than a silent pick of the first — the wrong file read
successfully is worse than no read at all. Stored and deflated members are
supported, which is every zip in practice.

Members stream: nothing is expanded to memory or to a temp file, so a 59 MB CSV in
a 12 MB zip costs the same memory as reading it loose.

Two consequences worth knowing:

- **Neither is parallel.** There is no mapping from a byte offset in a compressed
  stream to a row, so `-j` cannot cut one up — the same reason gzip is not
  splittable under Hadoop. `EXPLAIN` reports `physical: serial` for both, and a
  loose CSV is the faster shape if you have the disk for it.
- **Parquet cannot be read through either.** It needs to seek — footer first, then
  the chunks the query wants — and neither a codec nor an archive member offers
  that. Both are refused at plan time rather than quietly expanded somewhere.

Reading an archive over HTTP is not supported yet: a zip's index sits at the end of
the file, so it needs a ranged fetch before anything else can happen.

- **`WITH (format = 'csv' | 'parquet')`** — read or write a path as this format
  whatever its extension says. Needed for a file named `.dat` or `.txt`, and for
  a URL that serves CSV from an extensionless path. Without it, an extension
  basalt does not know is refused at plan time rather than parsed as CSV: a
  `.zip` used to be read as text and answer `COUNT(*)` with the number of
  newlines that happened to occur in its compressed bytes.
- **`WITH (k = v, flag, ...)`** — residual source options: `items` (dotted
  path to the row array when the response nests it, e.g. `items = 'data.rows'`
  — a bare array needs nothing), `buffer` (drain the source fully before
  opening the sink), `prefetch`, `timeout_ms`, `header = 'Name: value'`,
  `auth` forms, `method`/`body` for POST sources, etc.

`WHERE` on a REST source runs in basalt after the fetch; on a SQL table it is
pushdown. Same word, different plan — `EXPLAIN` shows which.

A complete REST read, for orientation:

```sql
CREATE CONNECTION crates TYPE http OPTIONS (base_url = 'https://crates.io/api/v1')
  RETRY 2 ON (429, 503);

SELECT id AS crate, downloads, json_get(links, 'owners') AS owners
FROM crates.GET('/crates', sort = 'downloads', per_page = 100)
  PAGINATE BY page (param = 'page', size = 100, total = 'meta.total', max = 5)
  WITH (items = 'crates')
WHERE downloads > 0;
```

The rows are the array at `items` (or the response itself); a single object —
a detail endpoint's answer — is one row. Columns are typed from the first
object: numbers, booleans and strings as themselves, nested objects and arrays
as JSON text, which `json_get` and `JSON_EACH` (§9, and `UNNEST` below) take
apart.

### Operators

| clause | plan stage |
|--------|-----------|
| `WHERE <expr>` | filter |
| `SELECT a, expr AS x` | projection |
| `SELECT * EXCLUDE (a, b)` / `EXCEPT` | all-but projection |
| `SELECT * RENAME (a AS b)` | rename projection |
| `COUNT(*) / SUM / AVG / MIN / MAX ... GROUP BY k` | aggregate (every other item must be a group key, aliased or not, or a plan-time constant). A numeric aggregate refuses a non-numeric argument at plan time, and casts text per row — so a CSV column read as text still sums, and text that is not a number fails the run |
| `ROUND(AVG(x), 2)`, `SUM(a)/COUNT(*)` | an aggregate inside an expression: the calls are computed by the aggregate, the arithmetic around them by a projection after it |
| `COUNT(DISTINCT x)` | aggregate — combines freely with other aggregates; ignores nulls |
| `MEDIAN(x)` | aggregate — a float; the mean of the two middle values on an even count; ignores nulls. Holds every value of the group until the end, so it is the one aggregate that is not O(1) per group. Engine-side only (never pushed down), and not a window function |
| `count_if(cond)` | aggregate — the rows where `cond` is true, as an `INT`; `0` for no rows, never null |
| `bool_and(cond)` / `bool_or(cond)` | aggregate — whether `cond` held for every row / for any row; nulls ignored, null when the group has no non-null value |
| `bit_and(x)` / `bit_or(x)` / `bit_xor(x)` | aggregate — the bitwise fold of an `INT` column; nulls ignored, null when there is nothing to fold |
| `var_samp(x)` / `var_pop(x)`, `stddev_samp(x)` / `stddev_pop(x)` | aggregate — the sample and population variance and standard deviation, as floats; nulls ignored. `variance` and `stddev` are the **sample** ones, as in Postgres, DuckDB, Trino and SQL Server (MySQL and StarRocks read them as population). A sample statistic of fewer than two values is null; a population one of a single value is `0` |
| `HAVING <expr>` | filter after the aggregate; aggregate calls in it refer to the columns it produced, including ones the `SELECT` list never asked for |
| `ORDER BY a DESC, b` | sort |
| `LIMIT n [OFFSET m]` | limit |
| `SELECT * EXCEPT (a, b)` / `EXCLUDE` | every column but those; a name not present is ignored, so one list serves tables that differ. Right after a union (`EACH TABLE OF`, `UNION ALL BY NAME`) the names are dropped *before* the branches are reconciled, so a column one table carries with an incompatible type can be excepted instead of failing the load. A name may be `IDENTIFIER(<expr>)` — a `$param` or loop variable rendered at run time, `'a, b'` excluding both and `''` nothing |
| `SELECT DISTINCT` / `DISTINCT ON (a, b)` | distinct — `ON` keys are input columns: they need not be in the SELECT list, and may be ones it renames (`DISTINCT ON (grp) grp AS k`). `DISTINCT ON` keeps the first row per key in `ORDER BY` order when there is one (`ORDER BY k, ts DESC` keeps the latest), else the first in input order |
| `CROSS JOIN UNNEST(SPLIT(tags, ',')) AS tag` | explode (also `UNNEST(col)`) |
| `CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag` | explode a JSON array: one row per element — strings unquoted, objects and arrays as JSON text, a JSON `null` as null. A null or `null` cell gives no rows; an object or scalar is an error |
| `[INNER\|LEFT\|RIGHT\|FULL\|CROSS\|SEMI\|ANTI] JOIN <cte> x ON a = b [AND c = d ...]` | join (right side must be a CTE) |

Row order without `ORDER BY` is not defined in SQL, and `GROUP BY` returns groups
in hash-partition order. A pipeline that only filters, projects or joins keeps the
source's order at any `-j` — to the terminal, a CSV, a parquet or an Arrow file —
so the same run writes the same rows in the same order; lanes still read in
parallel, and their output is put back in order before it is written. A parquet
file's row groups follow those lanes' units (each lane encodes its own), so its
layout, not its rows, can differ from a `-j 1` run. Loading into a database table
does not order its rows (a table has none). `DISTINCT` keeps the first row per
key in input order at any `-j`. Add `ORDER BY` whenever the order is part of
the answer.

Joins are hash equi-joins: the CTE (right) side is materialized and indexed
once, the left side streams through. Keys are plain columns (compute
expressions in the CTE or a select first), `AND`-combined for composite keys;
pairs may be written in either order, and a null key never matches. `CROSS
JOIN <cte>` takes no `ON`. Right-side columns that collide with a left name
come back suffixed `_r`, and `_r2`, `_r3`, … if that name is taken too — that is
the name `SELECT *` shows. A qualified reference needs no suffix: with `FROM t a
JOIN r b`, `b.amt` is the right side's `amt` everywhere in the query (`SELECT`,
`WHERE`, `GROUP BY`, `ORDER BY`, a later join's `ON`), and `SELECT b.amt` calls
its output `amt` unless `a.amt` is already in the list. A pipeline shaped `read | filters | join | filters |
write` probes in parallel under `-j` — over local CSV/Parquet morsels, and
over key-range splits for a splittable SQL source. Since 0.5.8 a chain of joins
followed by `GROUP BY` fans out the same way (`read | filters | join+ | filters |
aggregate | sort/limit | write`). Right and full joins stay serial in every case:
they have to emit the build rows nothing matched, and each lane would emit those
from its own copy of the match tracking. The build side is fully resident; past 4 GiB the
run fails fast instead of eating the host — raise the ceiling per join with
`WITH (max_build = '16GB')` on the join clause, filter the CTE, or flip the
join.

### Window functions

`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG(col[, n])` / `LEAD(col[, n])`, and
`SUM(col)` / `COUNT(*|col)` / `MIN(col)` / `MAX(col)` / `AVG(col)` over `PARTITION BY` /
`ORDER BY`:

```sql
SELECT conta, data, valor,
       ROW_NUMBER() OVER (PARTITION BY conta ORDER BY data DESC) AS recencia
FROM 'movimentos.csv';
```

- `ORDER BY` inside `OVER (...)` is required for the ranking and offset functions — it
  is what the numbering is by. An aggregate does not need it: **without `ORDER BY` its
  frame is the whole partition** (share-of-total), and **with it the frame is everything
  up to and including the current row's peers** (a running total). Those are the two
  frames standard SQL defaults to, so no frame syntax is needed to reach either.
- Ties share a running total: two rows with equal `ORDER BY` values both read the total
  *including both*, which is what `RANGE` framing specifies.
- `PARTITION BY` is optional; without it the whole input is one partition.
- A window function must be the **whole** select item: `ROW_NUMBER() OVER (...) + 1` is
  not accepted, because a window is a stage rather than an expression.
- Its column is **appended** to the projection. Columns the window itself names — the
  partition keys, the order keys and the function's argument — do **not** have to be
  projected: they are carried through hidden and dropped afterwards, so
  `SELECT LAG(v) OVER (PARTITION BY k ORDER BY t) AS prev FROM 't.csv'` returns `prev`
  alone.
- Several window functions in one `SELECT` may share one `OVER (...)` —
  `MIN(v) OVER (w), MAX(v) OVER (w)` is fine, and each may frame it its own way (a
  moving sum beside a running total). A different `PARTITION BY` or `ORDER BY` is
  refused; write the second as a separate query or wrap the first in a derived table.
  A window function's argument is a plain column: compute an expression in a CTE first.
- `MIN`/`MAX` answer a value from the column and keep its type; `AVG` is always a float;
  all of them are nullable, since a peer group of nothing but nulls has no answer.
- The names are not reserved: a column called `rank` still reads as a column.
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` and
  `ROWS BETWEEN <n> PRECEDING AND CURRENT ROW` set an explicit frame, which counts
  **rows** rather than peers. That is the difference worth knowing:

  ```sql
  -- ties do NOT share: 10, 30, 50 over v = 10, 20, 20
  SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  -- ties DO share (the default): 10, 50, 50
  SUM(v) OVER (ORDER BY v)
  ```

  A bounded frame is a moving window — `AVG(v) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING
  AND CURRENT ROW)` is a three-row moving average, clipped at the partition's start. A
  `FOLLOWING` end bound is not accepted; the frame always ends at the current row.
- A frame applies to an aggregate. `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG` and `LEAD`
  do not take one, and writing one is an error rather than being ignored.
- `LAG`/`LEAD` read a plain column and an optional literal offset (default 1). Looking
  past the edge of the row's own partition yields **null** — there is no third
  `default` argument — so the column is always nullable.

Because a window function cannot sit inside an expression, compose one by wrapping it in
a derived table. Change detection reads:

```sql
SELECT conta, valor - anterior AS delta
FROM (SELECT conta, valor,
             LAG(valor) OVER (PARTITION BY conta ORDER BY data) AS anterior
      FROM 'movimentos.csv') m
WHERE anterior IS NOT NULL;
```

A window is a **breaker**: a row's number is not known until its whole partition has
arrived, so memory is bounded by the input, as it already is for `ORDER BY`, `DISTINCT`
and `GROUP BY`. `DISTINCT ON (...)` remains the cheaper way to keep one row per key —
without an `ORDER BY` it streams, where `ROW_NUMBER() ... = 1` would not.

### Subqueries in a predicate

`IN (SELECT ...)` and the scalar subquery are supported as sugar over machinery
that already existed — each is rewritten at parse time into the join or constant
it always denoted, so nothing here adds a new execution path:

```sql
-- Planned as a SEMI JOIN against the (anonymous) subquery binding.
SELECT * FROM 'facts.csv' WHERE x IN (SELECT k FROM 't.csv');

-- NOT IN plans as an ANTI JOIN with SQL's NULL rule — see below.
SELECT * FROM 'facts.csv' WHERE x NOT IN (SELECT k FROM 't.csv');

-- A scalar subquery runs ONCE, before the outer query, and its single cell is
-- spliced in as a constant — so the comparison pushes down to the source like
-- any literal. This is the incremental-extraction idiom:
SELECT * FROM conn.orders WHERE ts > (SELECT max(ts) FROM 'lake/orders.parquet');

-- The same thing, named — useful when several statements share the value:
LET hi = (SELECT max(ts) FROM 'lake/orders.parquet');
SELECT * FROM conn.orders WHERE ts > $hi;
```

The subquery of `IN` must produce exactly one named column, and the test must sit
as a **top-level AND condition of `WHERE`** — under an `OR`, a `NOT` or a `CASE`
it cannot be lifted into a join without changing what the predicate means, so
those shapes are parse errors (spell them as an explicit join). The left side
must be a plain column. The literal-list form `IN (1, 2, 3)` is unchanged.

A scalar query — inline or as `LET x = (SELECT ...);` — must produce exactly one
column; one row is the value, zero rows read as `NULL` (standard SQL), and more
than one row is an error. The inline form is evaluated per *statement*, not per
row: it may not reference columns of the outer query.

**`NOT IN` is standard three-valued `NOT IN`.** A single NULL from the subquery
makes `x NOT IN (...)` unknown for every row, so no row survives; a NULL `x`
survives only an empty subquery. To keep the rows that match no *non-null* key —
what an extraction script usually means — say so in the subquery:
`WHERE k IS NOT NULL`.

`EXISTS` has no spelling: the uncorrelated form is degenerate (all rows or none)
and the useful form is correlated. A **correlated** subquery — one referencing a
column of the outer query — has no equivalent here and is not planned: it needs
either decorrelation in an optimiser or per-row execution of the inner query, and
the second is fatal for a streaming engine. Rewrite it as a join — the equality
correlation `EXISTS` usually carries is exactly `IN` on that key.

Table aliases (`FROM t a`, `JOIN c b`) are stripped at parse time — the engine
sees bare column names.

Aggregation end to end:

```sql
SELECT region,
       COUNT(*)                AS orders,
       COUNT(DISTINCT customer) AS customers,
       SUM(amount)             AS revenue
FROM 'orders.parquet'
WHERE placed_at >= '2026-01-01'
GROUP BY region
HAVING COUNT(*) > 100
ORDER BY revenue DESC
LIMIT 10;
```

A **plan-time constant** may sit in the list alongside the aggregates, since it is
one value for the whole query — a literal, arithmetic or `||` over literals, a
call like `now()`, or a `$param` / `$let`. That is how an aggregate result carries
a run id or a tenant tag:

```sql
PARAM tag STRING DEFAULT 'acme';
LET run_ts = now();
SELECT $tag AS tenant, $run_ts AS loaded_at, region, COUNT(*) AS orders
FROM 'orders.parquet' GROUP BY region;
```

A plain *column* still may not: it has no single value per group, so it must be
wrapped in an aggregate or named in `GROUP BY`.

**A float `SUM` is reproducible for a given `-j`, not across values of it.** The
lanes each total their own slice and the slices are added in a fixed order, so
rerunning the same command writes the same bytes; but `-j` decides how the input
is cut, and float addition is not associative, so `-j 4` and `-j 8` can differ in
the last bits (as can either from `-j 1`). `SUM` over an `INT` or a `DECIMAL` is
exact and identical everywhere — `SUM(CAST(amount AS DECIMAL(18,2)))` is the way
to total money you intend to compare or checksum. `COUNT`, `MIN` and `MAX` are
exact too, as are `count_if`, `bool_and`/`bool_or` and the `bit_*` aggregates.
The variances and deviations combine their lanes the same fixed way, so they
share `SUM`'s float caveat: `ROUND` them before comparing runs at different `-j`.

### Naming, `GROUP BY` and `ORDER BY`

A computed `SELECT` item does not need `AS`. Without one it is named after the
text of its expression, lowercased and stripped of spaces — `COUNT(*)` becomes
`count(*)`, `ClientIP - 1` becomes `clientip-1`. An explicit alias always wins.

The same naming is what lets `GROUP BY` and `ORDER BY` repeat an expression
instead of its alias: both sides render the expression the same way, so they
bind to the one column the projection already produced.

```sql
SELECT AdvEngineID, COUNT(*) FROM hits
GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;      -- binds to count(*)

SELECT DATE_TRUNC('minute', EventTime) AS m, COUNT(*) AS c FROM hits
GROUP BY DATE_TRUNC('minute', EventTime);          -- binds to m
```

- **A subquery in `FROM` is a derived table** — `FROM (SELECT ...) x` — and works in a
  join's right side too: `JOIN (SELECT ...) y ON ...`. It lowers to exactly what
  `WITH x AS (...)` produces, so it costs nothing extra to run; the alias is optional.
- **Column order is the `SELECT` list's**, not the aggregate's. `SELECT k, SUM(x), k2`
  writes `k, sum, k2`; the engine folds grouping keys before aggregates internally and
  reorders the projection back on the way out. (Before 0.5.8 that query wrote
  `k, k2, sum` — the values were right, the columns had moved.)
- `GROUP BY <n>` is positional — it names the *n*-th `SELECT` item.
- `GROUP BY <expr>` accepts a computed key (`GROUP BY ClientIP - 1`).
- `ORDER BY` may name a column the `SELECT` list does not project. It is
  carried through the projection as a hidden column and dropped after the
  `LIMIT`, so sorting by an unselected column costs nothing in the output.

## 6. `UNION`, `INTERSECT`, `EXCEPT` and `UNION ALL BY NAME`

`UNION ALL` is SQL's: branches line up **by position**, under the first branch's
column names, and must have the same number of columns. Types widen per column
(an int meeting a float is a float); a pair with no common type is an error.
`UNION` without `ALL` also removes duplicate rows — from everything to its left, so
`a UNION b UNION ALL c` deduplicates `a ∪ b` and then appends `c`.

```sql
SELECT id, amount FROM 'eu.csv'
UNION ALL
SELECT id, total FROM 'us.csv'      -- `total` lands under `amount`
ORDER BY id;
```

`INTERSECT` keeps the rows found in both branches, `EXCEPT` those of the first
found nowhere in the second; both line columns up by position and remove
duplicates. Two NULLs count as the same value there, unlike in a join's `ON`.
`INTERSECT` binds tighter than `UNION` and `EXCEPT` (`a UNION b INTERSECT c` is
`a UNION (b INTERSECT c)`). `INTERSECT ALL` and `EXCEPT ALL` are not supported.

`BY NAME` lines branches up by column name instead, and is what reconciling N
similar tables needs. It applies to `UNION` only, and a chain is one or the other.

A `BY NAME` branch may be **any query** — a file, a filter, a projection, an
aggregate — not only `SELECT ['tag' AS c,] t.* FROM <conn>.<table>`. That shape is the reconciliation
case the feature was built for (N similar tables aligned by name) and it still gets the
`tag` column and table discovery; a general branch is built like any other pipeline and
reconciled the same way.

Alignment is **by column name**: NULL-fill missing, drop extra, cast type
differences — DuckDB's `UNION ALL BY NAME`. `UNION BY NAME` also deduplicates.

```sql
-- explicit branches: the tag is just a literal column
SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
UNION ALL BY NAME
SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
ANCHOR SCHEMA erp.dbo.CT2010;          -- schema authority (optional)
```

```sql
-- discovered: one branch per row of a raw 2-column query (table, tag)
SELECT *
FROM EACH TABLE OF (erp.QUERY($$SELECT name, SUBSTRING(name,4,2) FROM sys.tables WHERE name LIKE 'CT2%'$$))
  AS (table_name, CT2_EMPRESA)         -- 2nd name = output tag column
  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)      -- raw predicate on EVERY branch
  ANCHOR SCHEMA erp.dbo.CT2010;
```

The discovery source may also be a full basalt `SELECT`, executed in-engine at
plan time (its translatable `WHERE` prefix still descends to the source as
usual). The connection the *discovered tables* live in is inferred from the
query's leading read when it names a connection; `IN <conn>` overrides it:

```sql
SELECT *
FROM EACH TABLE OF (SELECT name, substr(name, 4, 2) FROM erp.sys.tables WHERE name LIKE 'CT2%')
  AS (table_name, CT2_EMPRESA)
  ANCHOR SCHEMA erp.dbo.CT2010;
```

JSON form (array of `{table, tag}` objects, e.g. from a request body):
`FROM EACH TABLE OF ($job.tables) IN erp AS (table_name, tag)` — element keys
remappable via `WITH (table_field = ..., tag_field = ..., tag_substr = '4,2')`.

## 7. `FOR EACH ROW OF` and the `CASE` statement

Plan-time fan-out — one pipeline (or dispatch) per row of a discovery source.
A catalog of tables, each read and loaded under a per-row name:

```sql
FOR EACH ROW OF ($tables) AS (name, where)
  PARALLEL ON ERROR CONTINUE           -- or SEQUENTIAL / ON ERROR STOP
  LOAD INTO sr.IDENTIFIER('fluig_' || lower($name))
    USING stream_load UPSERT AS        -- bare UPSERT: PK inferred from source
  SELECT *, now() AS extraction_timestamp
  FROM fluig.dbo.IDENTIFIER($name)     -- a per-row TABLE read
  PUSHDOWN($where);                    -- raw predicate value ("" ⇒ no WHERE)
END FOR;
```

- Sources: a raw discovery query (`conn.QUERY($$...$$)`, first N columns → N
  loop vars positionally), an in-engine `SELECT` query (any basalt query, run
  once at plan time; first N columns → N loop vars positionally), or a JSON
  param path (`$tables`, `$job.tables`, …; object fields bound to the loop
  vars by name, a missing field ⇒ `""`).
- The body holds queries (`LOAD INTO` / `SELECT`, each with its own `WITH`),
  nested `FOR EACH ROW OF`, the `CASE` statement, `CALL`, `PRINT`, `EXPLAIN`
  and `THROW`. Declarations — `PARAM`, `LET`, `CREATE CONNECTION`, `CREATE
  FUNCTION` — belong at the top level; `check` and `run` refuse one in a body
  with the same message.
- Loops nest. The inner loop's discovery source is rendered with the outer
  row (`FOR EACH ROW OF (erp.QUERY($$SELECT ... WHERE t = '${name}'$$))`), and
  the inner body sees both rows' variables, the innermost winning a shared
  name. Each `PARALLEL` loop fans out over its own rows.
- A `WITH` inside a body is rendered per row like the query that reads it,
  and is scoped to the body: after the loop the name means whatever it meant
  before. At the top level a `WITH` is visible from its statement onwards, so
  two statements may reuse a CTE name and each reads its own.
- Loop variables may be typed: `AS (name, port:INT)`.
- A loop variable is also an ordinary expression **value**: `SELECT $name AS
  empresa`, `WHERE $port > 1000` (typed vars compare as their declared type).
  Only `$name` is the loop variable: a bare `name` in the body is still the
  source column of that name, as it is beside a PARAM.
- The `CASE` **statement** (`... THEN <statements> ... END CASE`) dispatches
  whole pipelines per row — subject form (`CASE $env WHEN 'prod', 'staging'
  THEN ... END CASE`) and the guard form. `END CASE` distinguishes it from the
  CASE **expression** (§9). Use it when the branches are *different pipelines*
  (different sources/sinks); for choosing a *value*, put the conditional in the
  expression (`IDENTIFIER(if($pk = '', $name || 'id', $pk))`).

### Dynamic names — `$var`, `IDENTIFIER()`, `||`

Loop variables (and params) are referenced with `$` — `$name`, `$where` —
resolved by name per row. A *name* is computed from them by an ordinary string
expression, and **`IDENTIFIER(<string-expr>)`** turns that string into a table
or object reference (the precedent is Snowflake / Databricks `IDENTIFIER`).
`||` is string concat; `lower()`, `if()`, `concat()` compose as usual.

| you want | write |
|---|---|
| a per-row source table | `FROM conn.schema.IDENTIFIER($name)` |
| a per-row file path | `FROM IDENTIFIER('dir/' \|\| $name \|\| '.csv')` (the extension must be literal) |
| a computed sink name | `LOAD INTO conn.IDENTIFIER('pre_' \|\| lower($name))` |
| a per-row sink file | `LOAD INTO IDENTIFIER('dir/' \|\| $name \|\| '.csv')` (extension literal, as above — it picks the writer) |
| a raw predicate value | `PUSHDOWN($where)` |
| a conditional key | `UPSERT ON (IDENTIFIER(if($pk = '', $name \|\| 'id', $pk)))` |
| a per-row column *name* | `SELECT IDENTIFIER($col), COUNT(*) ... GROUP BY IDENTIFIER($col) ORDER BY IDENTIFIER($col)` |
| a per-row column value | `SELECT $name AS empresa` — a plain expression, no quoting |

In an expression or a name position (`SELECT` list, `WHERE`, `GROUP BY`,
`ORDER BY`, `DISTINCT ON`) `IDENTIFIER(<string-expr>)` is a **column** whose
name is computed per row — or once, from a `PARAM`, outside any loop. The
column is only known when the row renders it, so `check` validates the stages
before the first dynamic name and leaves the rest to `run`, which reports an
unknown column per row (``for-each row c=nope: unknown field `nope```).

`IDENTIFIER($name)` resolves to a **table** read, so bare `UPSERT` still infers
the PK from source metadata — a raw `QUERY(...)` read cannot. This is why the
catalog holds only `{name, where}`, never a PK.

### Raw `${...}` interpolation (raw SQL bodies only)

Inside a raw `QUERY($$...$$)` or `PUSHDOWN($$...$$)` literal, `${var}` /
`${ <expr> }` still splices loop values into the SQL text (C#-style: nested
string literals in the hole need no escaping) — `QUERY($$SELECT ${cols} FROM
${name}$$)`. Prefer `$var` + `IDENTIFIER()` everywhere a *name* is meant;
reach for `${...}` only when you are literally building a raw SQL string.

## 8. HTTP mode

```sql
CREATE ENDPOINT '/eventos' DOC 'Recebe telemetria';

LOAD INTO sr.bronze.eventos USING stream_load AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, tipo, now() AS recebido_em
FROM BODY (
  device_id STRING NOT NULL,
  ts        STRING,
  tipo      STRING,
  payload   JSON
)
WHERE tipo IN ('leitura', 'alarme');
```

- `basalt serve <dir>` hosts every endpoint script, routed by the declared
  path; `DOC` feeds the startup banner.
- **`FROM BODY (schema)`** declares the request contract. The body (JSON array
  or single object) is validated row by row: a missing/null `NOT NULL` column
  or an unreadable value rejects the request with a message naming the row —
  served as **422**. Extra keys are dropped. `JSON` columns ride as text.
- **`FROM HEADER('X-Tenant')`** on a `PARAM` binds it from that request header
  (case-insensitive); bare `FROM HEADER` matches the param's own name.
- Status contract: success → `200` + summary JSON; per-item failures → `207`;
  permanent error → `422`; transient → `503` + `Retry-After`.

### Durable buffer (WAL)

`ACCEPT ... INTO BUFFER` turns the endpoint into a queue: **200 means
"accepted durably"** (fsynced), and the load happens asynchronously.

```sql
CREATE ENDPOINT '/eventos'
  DOC 'Recebe telemetria; ack após persistir em disco'
  ACCEPT BODY (
    device_id STRING NOT NULL,
    ts        STRING,
    payload   JSON
  )
  INTO BUFFER 'eventos'
    AT '/var/lib/basalt/wal'
    SEGMENT 16 MB
    RETAIN UNTIL LOADED;          -- or: RETAIN 24 HOURS (allows reprocessing)

LOAD INTO sr.bronze.eventos USING stream_load AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, payload, now() AS recebido_em
FROM BUFFER 'eventos'
  FLUSH EVERY 5 SECONDS OR 50000 ROWS;
```

- Requests are validated against the `ACCEPT BODY` schema (422 naming the
  row), appended to append-only JSONL segments, and acked after one fsync
  (group commit: N rows, one sync).
- A flusher thread drains completed segments through the pipeline, one run
  per segment. The StarRocks label is derived from the segment name
  (`eventos-000042`), so a crash between "loaded" and "marked" replays the
  same label and the sink dedups — effectively exactly-once, no 2PC.
- Backpressure: buffer disk usage over the limit (1 GiB default) ⇒ `503 +
  Retry-After` — the client is the queue.
- **Batch replay**: `FROM BUFFER 'eventos' AT '<dir>'` in a plain batch script
  reads every retained segment — the queue is just another source.
- Honest cost: `serve` becomes stateful (the WAL directory needs a persistent
  volume) and durability is the node's disk, not replicated.

## 9. Expressions

SQL-ish, Pratt-parsed. Precedence (high→low): unary `- NOT ~` → `* / %` →
`+ - ||` → `<< >>` → `&` → `^` → `|` → comparisons
`= == != <> < <= > >= LIKE IN IS` → `??` → `AND` → `OR`.

**Scope rule:** `$name` is script/environment scope — a PARAM, a LET, or (in a
`FOR EACH ROW OF` / statement-function body) a loop variable, resolved at plan
time. Bare names are row/local scope — columns, `LET … IN` bindings, aliases —
and a PARAM, LET or loop variable never stands in for one: with `PARAM region`,
`WHERE region = 'West'` filters on the column and `WHERE region = $region` on
the parameter. Among `$` names the innermost binding wins: loop var >
LET/PARAM. A `$name` that nothing binds is a plan-time error naming it — never
the column it happens to spell — and `check` reports it even over a table whose
columns it has not seen.

- `$name` — see the scope rule above. `$job.a?.b` navigates a JSON param.
- Bitwise (INT only, engine-side — never pushed down): `& | ^ << >>`, unary
  `~`. `^` is xor. `>>` is arithmetic; shift counts `< 0` or `>= 64` yield 0
  (`-1` for `>>` of a negative). Companions: `bit_count() to_hex() from_hex()`.
- `a || b` — string concat (ANSI), sugar for `concat(a, b)`.
- `IDENTIFIER(<string-expr>)` — treat a computed string as a table/object name
  (§7); valid in `FROM`/`LOAD INTO`/upsert-key positions, not general
  expressions.
- `CASE` expression, both forms:
  `CASE status WHEN 'paid', 'ok' THEN 'done' ELSE 'open' END` ·
  `CASE WHEN amount >= 1000 THEN 'gold' WHEN amount >= 100 THEN 'silver' ELSE 'std' END`
- `IF(c, a, b)` kept as sugar.
- `x IS [NOT] NULL` · `x IS [NOT] EMPTY` (true when null **or** `''`; string
  operands only — handy for loop values).
- `a ?? b` — null-coalesce (sugar for `COALESCE`).
- `CAST(x AS INT)` / `CAST(x AS DECIMAL(18,2))` / `CAST(x AS DATE)` /
  `CAST(x AS TIMESTAMP)` — implicit widening is int→float/decimal only. Text
  parses as `YYYY-MM-DD[ HH:MM:SS]`. Text to a number is **strict**: an optional
  sign, digits and at most one `.`, and anything else fails rather than being
  guessed at. That matters wherever money is written the Brazilian or European way
  — `'1000,00'` is not a number basalt will read, and it used to come back as
  100000.00. Strip the separator first: `CAST(replace(v, ',', '.') AS
  DECIMAL(18,2))`, or reach for `TRY_CAST` to turn unreadable values into nulls.
- **A decimal that loses digits rounds half away from zero**, as PostgreSQL and
  SQL Server round: `CAST('12.345' AS DECIMAL(10,2))` is `12.35` and `-12.345`
  is `-12.35`. The rule is the same for a cast, for a value written to a file
  column of smaller scale (Parquet, Arrow), and for an aggregate's result — one
  answer whichever path a value takes. A database sink is sent the value whole
  and its column does its own rounding. A float converts through its
  15 significant digits, as PostgreSQL converts `float8` to `numeric`: the
  double nearest `2.675` is `2.67499…`, and it still becomes `2.68`. A float too
  large for 38 digits fails the cast.
- **DECIMAL arithmetic is exact.** `+`, `-` and `%` over two decimals (or a
  decimal and an int) answer a decimal at the wider operand's scale, `*` one at
  the summed scale — `CAST(1.1 AS DECIMAL(18,2)) + CAST(0.3 AS DECIMAL(18,2))` is
  `1.40`, not `1.4000000000000001`, so a `SUM` cast to `DECIMAL` stays exact
  when this month's total is subtracted from last month's. `/` has no finite
  scale and stays float, as does anything with a float operand (a literal like
  `0.3` is a float). `round(x, n)` on a decimal is exact too, and with a literal
  `n` answers `DECIMAL(p, n)`; `-x` stays a decimal, and `CAST(x AS INT)` rounds
  half away from zero. `%` takes the dividend's sign for every numeric kind:
  `-5.5 % 2` is `-1.5`.
- A `DATE`/`TIMESTAMP` column compares directly against an ISO string literal
  (`WHERE d >= '2013-07-01'`). The literal is coerced to the column's type,
  never the reverse, and it is validated at plan time — so `'2013-13-01'` and
  `'01/07/2013'` are errors from `check`, not silent text comparisons.
- `"Valor Total"` — a quoted column name (§1), valid anywhere a bare name is:
  `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, an alias (`AS "Total Geral"`), and
  after a qualifier (`t."Valor Total"`).
- `x LIKE 'a%'`, `x IN (1, 2, 3)` (expands to an OR-chain),
  `x [NOT] BETWEEN a AND b` (inclusive; expands to `x >= a AND x <= b`, so it
  pushes down like any other pair of comparisons).
- `LET x = <val> IN <body>` — local binding, inlined at plan time.
- Scalar functions (case-insensitive): `now() today() lower() upper() length()
  strlen() trim() substr() replace() concat() coalesce() starts_with()
  ends_with() contains() like() date_trunc() extract() regexp_replace()` ·
  math `abs() floor() ceil() round(x[,n]) mod() power() sqrt() sign()` (round
  is half-away-from-zero, deliberately engine-side) · nulls `nullif()
  greatest() least()` (null args ignored, Postgres-style) · strings `lpad()
  rpad() left() right() split_part() strpos() repeat() reverse()` — these,
  `length()`, `substr()`, `upper()`/`lower()` and `LIKE`'s `_` count
  characters, as Postgres and DuckDB do; `strlen()` counts bytes. A byte that is
  not UTF-8 counts as one character, so mis-encoded text never fails a load.
  `upper`/`lower` map Latin, Greek and Cyrillic, one character to one (`ß`
  stays) · dates
  `date_add(unit, n, ts) date_diff(unit, a, b) make_date() epoch()
  to_timestamp() strftime(ts, fmt)` (`%Y %m %d %H %M %S %y %%`; month/year
  arithmetic clamps the day-of-month) · json `json_get(doc, path)`.
- `JSON_GET(doc, path)` — one value out of a JSON document, as text: `path` is
  `a.b`, `a[0].b` or `a.0.b` (a leading `$.` is allowed). Strings come back
  unquoted, objects and arrays as JSON text; a missing key, an index past the
  end or a JSON `null` is null. A cell that is not JSON is an error, not a
  null. `CAST(json_get(doc, 'n') AS INT)` for a typed value.
- `JSON_FILTER(arr, x -> cond)`, `JSON_TRANSFORM(arr, x -> value)`,
  `JSON_ANY(arr, x -> cond)`, `JSON_ALL(arr, x -> cond)` — work on a JSON array
  in place, without `UNNEST` and re-aggregating: the lambda's body is evaluated
  once per element with its parameter bound to it. `json_filter` keeps the
  elements whose condition is true, as written; `json_transform` returns the
  array of the body's values; `json_any` / `json_all` whether the condition is
  true for some / every element (an empty array: false / true). A nested
  Parquet list reads as exactly such an array.

  ```sql
  SELECT id,
         json_filter(tags, t -> t LIKE 'vip%')                    AS vip_tags,
         json_transform(items, i -> json_get(i, 'sku'))           AS skus,
         json_any(items, i -> CAST(json_get(i, 'qty') AS INT) = 0) AS has_empty_line
  FROM 'orders.parquet'
  WHERE json_any(tags, t -> t = 'new');
  ```

  An element is the parameter as itself — a number is a number (`x > 5`), a
  string a string, `true`/`false` a BOOL — and an object or array is its JSON
  text, which `json_get` and these functions take apart (lambdas nest). The body
  may name the row's columns too (`t -> t = region`); the parameter shadows a
  column of its name. In `json_transform`'s result, text that is a JSON object
  or array (what `json_get` returns for one) goes in as that object or array,
  any other text as a string. An element the body cannot compare — a number
  against text, in an array that mixes kinds — counts as null (the condition is
  not true; `json_transform` puts `null`); a failing `CAST` still fails the run.
  A null cell is null; a cell that is JSON but not an array is an error. A
  lambda is an argument of these four functions only, and never descends to a
  SQL source — the row is filtered here, while the rest of its WHERE still
  descends.
- `TRY_CAST(x AS T)` — CAST that yields null instead of failing on a bad
  value; the workhorse for dirty inputs. Never pushed down.
- `CAST(x AS TIME)` takes `'HH:MM:SS[.ffffff]'` or `'HH:MM'` text, or a
  timestamp (its time of day).
- `DATE_TRUNC('minute', ts)` and `EXTRACT(minute FROM ts)` — units `year`,
  `month`, `week`, `day`, `hour`, `minute`, `second`, the same for `DATE_ADD`
  and `DATE_DIFF`. `week` is the ISO week: it starts on Monday
  (`DATE_TRUNC('week', …)` is that Monday at 00:00), `EXTRACT(week …)` numbers
  it 1–53 with week 1 holding the year's first Thursday, and `DATE_DIFF`
  counts the Mondays crossed. `check` rejects an unknown unit even over a SQL
  table whose columns it has not seen. `EXTRACT` also accepts the
  ordinary two-argument call form. `STRLEN` is an alias for `LENGTH`.
- `REGEXP_REPLACE(s, pattern, replacement)` — replaces the first match;
  `\1`…`\9` in the replacement expand to captured groups (`\0` is the whole
  match). A literal pattern is compiled at plan time, so a malformed one fails
  `check`. See §11 for the supported syntax.
- `CREATE [OR REPLACE] FUNCTION nome(a [TYPE] [DEFAULT <expr>], ...)` — two
  body forms. `AS <expr>;` is a scalar function, inlined at plan time;
  recursion and arity mismatches are compile errors, declared types are
  checked against literal arguments at the call site, defaults fill omitted
  trailing arguments. A body starting with `LOAD`/`FOR`/`CALL`/`SELECT`/`WITH`/
  `PRINT`/`THROW`, or with the statement `CASE` (the one closed by `END CASE`),
  is a **statement function** terminated by `END;` and invoked with
  `CALL nome(args);` — its params bind like loop variables (`$name`,
  `IDENTIFIER($name)`, `PUSHDOWN($f)`, `${name}` in strings), rendered per
  call through the same machinery as a `FOR EACH ROW OF` body. CALL nesting is
  depth-guarded (16); a statement function is not atomic — a mid-body failure
  leaves earlier loads committed, exactly as if the statements were inline.
  Plain re-declaration of a name is an error; `OR REPLACE` is the sanctioned
  overwrite.
- `CREATE [OR REPLACE] FUNCTION nome(a [TYPE] [DEFAULT <expr>], ...) RETURNS
  TABLE AS <query>;` is a **table function**: a query with parameters, read
  wherever a CTE could be — `FROM nome(args) [alias]`, or the right side of a
  `JOIN`. Each call is the body with every `$a` replaced by its argument, lowered
  to a derived table at plan time, so it behaves exactly as if the query were
  written inline; two calls in one query (even of one function) are independent,
  `WITH` clauses inside the body included. Without an alias, the function's name
  qualifies its columns (`paid.id`). Arguments are plan-time constants —
  literals, `$params`, loop variables, and expressions over them — never a
  column. Arity, defaults and literal-argument types are checked as for a scalar
  function, the body is checked where it is declared, and an `@include`d table
  function is called like a local one. A body may call table functions declared
  before it; a call that reaches its own function (possible only through `OR
  REPLACE`) stops at 16 levels. A discovery query — `FOR EACH ROW OF (...)`,
  `EACH TABLE OF (SELECT ...)` — may call one too, as it may read a derived table
  or open with `WITH`: their bindings run ahead of the loop.

  ```sql
  CREATE FUNCTION paid_orders(since DATE, branch STRING DEFAULT '01') RETURNS TABLE AS
    SELECT C5_NUM AS num, C5_CLIENTE AS cliente, C5_EMISSAO AS emissao
    FROM erp.dbo.SC5010 PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
    WHERE C5_FILIAL = $branch AND C5_EMISSAO >= $since;

  SELECT num, cliente FROM paid_orders($desde) WHERE cliente <> '000001';
  ```

### `DESCRIBE` and `SHOW TABLES`

`DESCRIBE <source>;` prints one row per column — `column`, `type`, `nullable`
— for a file, a `conn.schema.table`, a `conn.QUERY($$...$$)` or a whole query
(`DESCRIBE SELECT ...`, CTEs included). The types are the engine's, i.e. what a
sink would receive; a table or query is asked for no rows, so it is cheap on a
large one. `SHOW TABLES FROM <conn>[.<schema>] [LIKE 'pattern'];` lists a SQL
source's tables and views from its `information_schema` (`table_schema`,
`table_name`, `table_type`), system schemas left out; on an `http` connection
it lists the declared resources (§3). Both are ordinary result
statements: `basalt run --format json -c "DESCRIBE erp.dbo.SC5010;"` gives a
program the schema, and the REPL's `\d` and `\dt` are the same statements.

### `EXPLAIN`

`EXPLAIN <statement>` prints the plan instead of running it.
`EXPLAIN ANALYZE <statement>` runs it and prints the operator tree with the
time and row count each stage actually cost — time is *exclusive*, so a
stage's figure excludes its inputs.

Both print the same tree: root first, the scan deepest, which is the nesting a
pull pipeline has. `EXPLAIN` annotates each operator with its detail and output
schema, `EXPLAIN ANALYZE` with what it measured.

```console
$ basalt run -c "EXPLAIN SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
plan
  write  stdout  (default)
    aggregate  1 agg(s), 1 group(s)
      schema: g:string?  c:int
      scan  parquet  x.parquet
        schema: g:string?  v:int?
  physical: morsel-parallel candidate (per-lane partials, combined)

$ basalt run -c "EXPLAIN ANALYZE SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
plan (actuals, exclusive time)
  aggregate      13.1ms          601 rows        2 batches
    scan          6.2ms       500000 rows        6 batches
```

The `physical:` line names the fan-out the plan is eligible for: `split-parallel`
for a SQL read divided into key ranges, `morsel-parallel` for a local CSV divided
into byte-range chunks or a parquet divided into row groups, `serial` for
everything else (a CSV over HTTP is fetched whole, so it parses serially). Both
stay "candidate" because what settles it is inside the source — a table's key and
size, a CSV that quotes a newline, a parquet with one row group — and neither
`EXPLAIN` nor `check` reads that far.

A source whose schema only the source itself can describe — a database table, a
remote object — reads `schema: unresolved`, said once at the scan rather than
repeated down the tree. Neither form connects.

A SQL read prints the WHERE it will send on a `pushdown:` line under its scan
(§5). A join prints its right side's read on a `right` line under it, with that
read's own `pushdown:` — so a CTE or table function joined in shows that it
filters at the source:

```console
$ basalt run -c "... EXPLAIN SELECT o.id, c.nm FROM pg.orders o JOIN active(1) c ON o.cid = c.cid;"
plan
  write  stdout  (default)
    select  id, nm
      join  inner __tvf1_active
        right  scan  postgres  table customers (via binding __tvf1_active)
          pushdown: ("active" = 1)
        scan  postgres  table orders
          schema: unresolved
  physical: split-parallel candidate
```

`EXPLAIN` is an ordinary statement: it goes anywhere a terminal `SELECT` or a
`LOAD INTO` goes, and explains that one query against whatever the statements
above it declared — connections, CTEs, `PARAM`s, `LET`s, functions. Everything
before and after it runs normally, at full parallelism, and a script may hold
several. A script that *opens* with `EXPLAIN` still explains the whole script,
offline: no params are bound and nothing runs, which is the form to reach for
when the point is to inspect a job rather than run one.

A plan printed by an `EXPLAIN` **statement** goes to **stderr**, like the
`EXPLAIN ANALYZE` tree and `PRINT`: the statements around it may be writing
rows to stdout, and stdout is the data contract (§10). The whole-script
`EXPLAIN` prefix prints to stdout instead — there the plan is the invocation's
only output, so `basalt run -c "EXPLAIN ..." > plan.txt` still captures it.

```console
$ basalt run -c "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'db', database = 'erp');
                 EXPLAIN SELECT SUM(v) AS v FROM pg.public.t;"
```

`EXPLAIN ANALYZE` moves no data. The pipeline runs — that is where the numbers
come from — but every sink is discarded: a terminal `SELECT` prints its plan
instead of its rows, and a `LOAD` writes nothing, creates no table and commits
no blob. A database can wrap an explained `INSERT` in `BEGIN ... ROLLBACK`; a
load into a remote lake has no undo, so it is never performed.

It also runs serially, whatever `-j` says, so the tree is one operator tree
rather than a different report per parallel path. Read the timings as a serial
profile: the row counts and the shape are exact, the durations are not what the
same query costs at full parallelism.

`EXPLAIN COSTS` is rejected at parse time — there is no cost model to report.

## 10. Running & exit codes

```
basalt run   <script>|-|-c "<inline>" [-p key=value ...] [-j threads] [--format table|json|csv|tsv|arrow] [--max-rows N]
basalt serve <dir> [--port N] [--watch]
basalt check <script>|-|-c "<inline>" [--format json] [--known t1,t2]
basalt complete <script>|-|-c "<inline>" --pos N [--connect]
basalt kernel [--format table|json|csv|tsv|arrow] [-j threads] [--max-rows N]
```

Options may come before or after the script path, and `-` (the script on
stdin) may sit anywhere among them: `basalt run --format json job.sql` and
`basalt run job.sql --format json` are the same run.

`--format json` makes stdout machine-readable: a terminal `SELECT` emits one
JSON object per row (NDJSON, streamed — decimals as strings, temporals as ISO
text, bytes as base64), and a `LOAD` run emits one summary object instead.
`--format csv` and `--format tsv` emit a header line and the rows — quoted exactly as
a `.csv` sink quotes them, a null as an empty field — and nothing else: no rule,
no `(N rows)` footer, so `| wc -l`, `| cut` and `| column -t` see only data. The
default `table` is for reading and does close with `(N rows)`, on stdout, the way
`psql` does; reach for `csv`, `tsv` or `json` when a program is the reader.
`--format arrow` emits the `SELECT` as an Arrow IPC stream — one record batch
per engine batch, typed (`DECIMAL` as decimal128, dates as date32, times and
timestamps in microseconds), readable with `pyarrow.ipc.open_stream`,
`polars.read_ipc_stream` or Arrow JS `tableFromIPC`. An empty result is still a
valid stream: the schema followed by the end-of-stream marker.

A script with several results writes one stream per result, back to back, and
each describes itself. Its schema carries `custom_metadata`:
`basalt.statement` (the result's ordinal in the run, from `0`), `basalt.kind`
(`select`, `show`, `describe` or `explain`), and `basalt.line` / `basalt.col`
where the statement starts. Just before its end-of-stream marker comes a
zero-row record batch whose message metadata holds `basalt.rows`,
`basalt.elapsed_ms` and `basalt.truncated` — plain Arrow, so a reader that ignores metadata sees the
same table. pyarrow reads the streams in turn from one file object
(`ipc.open_stream(f)` until `f` is exhausted; the trailer through
`read_next_batch_with_custom_metadata`). A statement `EXPLAIN` under arrow is a
result of its own — one string column, `plan`, a row per line — rather than
text on stderr, and so is a whole-program `EXPLAIN`.
Logs are stderr-only, plain text, level `warn` by default (`--log-level`,
`--log-format json`, `-q`).

`--max-rows N` keeps the first `N` rows of each result printed to stdout and
stops the query there: once a result has more rows than it keeps, every source
reports end of input at its next read, so `SELECT * FROM 'huge.parquet'`
returns its first rows after decoding one row group, not the file. A blocking
operator is unaffected — an aggregate, a sort or a join's build side has read
its whole input before the first row reaches stdout, so what is kept is exactly
the first `N` rows of the full answer. A cut result logs `result cut at
--max-rows N` (level `warn`), carries `basalt.truncated = true` in its Arrow
trailer, and in a kernel status. A `LOAD` is never capped.

Errors point at what is wrong, not at the statement it sits in: an unknown
column or function is reported at the name itself, on its own line of a
multi-line query. Under `--log-format json` an error is one NDJSON line in the
log's shape, with the range an editor underlines — `end_line`/`end_col` are just
past the offending text, and absent when the error is about a whole stage — and
whether a retry could help (`class`, the exit-`75` distinction below):

```
{"ts":1790592941244,"level":"error","event":"script_error","msg":"unknown field `nope`",
 "file":"orders.sql","line":2,"col":14,"end_line":2,"end_col":18,"class":"permanent"}
```

`event` is `parse_error`, `script_error` or `aborted`; `file` is the script, or
the `@include`d file the fault is in.

For an editor: `basalt check --format json` prints its diagnostics as a JSON
array on stdout — `[]` when the script checks out, else one object per error in
the shape above without `ts`/`event` — and still exits `1` on an error.
`check` does not stop at the first problem: every statement is checked on its
own and each that fails is listed, in script order. A statement that does not
parse is skipped to its `;` and parsing resumes after it — except inside a
`FOR`, `CASE` or `CREATE FUNCTION` body, whose own `;`s make the statement's end
unknowable, so the problems after a broken block go unreported until it is
fixed. `--known enrich,daily` names tables the script reads but does not
declare — a notebook's other cells — so `FROM enrich` is checked as a table
whose columns are unknown rather than failing as an unknown source.
`basalt complete --pos N` prints what Tab would offer at byte offset `N` (the
end, without `--pos`; `--utf16` counts `N` and the answer in UTF-16 units): `{"start":S,"end":N,"items":[{"text":…,"kind":…,"detail":…}]}`, the
items replacing `script[S..N]`, with `kind` one of `keyword`, `function`,
`param`, `cte`, `connection`, `table`, `column`, `path`. `S` is where the word
under the cursor begins even when nothing matches. `detail`, present only when
there is one, is a built-in function's signature (`date_add(unit, n, ts)`) or
a column's type — the engine's for a file, the database's `data_type` for a
table. A word comes back once: a column named `name` is not offered again as
the keyword `name`, and a column shadows a function of its name. It completes against
the script's own declarations, so a script half typed still has its names;
local files named in it give their columns; connections are asked for their
tables and columns only under `--connect`, since that is a round trip to each.

While a `LOAD` runs, a terminal gets a live line on stderr — what is moving
where, rows so far, the rate and the clock, with `[3/12]` in front inside a
`FOR EACH`:

```
⠹ [3/12] erp.dbo.SC5010 → sr.bronze.sc5010  1,204,112 rows  48.3k rows/s  0:24
```

A run that writes closes with one sentence, and one that has several `LOAD`s
(more than one statement, or a `FOR EACH` / `CASE` / `CALL`) reports each as it
finishes — ` +` loaded, ` x` failed, with the reason:

```
 + sr.bronze.sc5010                      1,204,112 rows    24.1s
 x sr.bronze.sb1010  connection reset by peer
Loaded 11 of 12 targets, 9,482,004 rows in 3m 12s (49.4k rows/s, 12 lanes)
1 failed
```

A single `LOAD` prints only the sentence — `Loaded 20,000,000 rows into
sr.bronze.orders in 8.2s (2.4M rows/s, 12 lanes)`, or `Read 4,000,000 rows,
loaded 7 into agg.csv in 1.9s` when the query reduces. The text carries no run
id; under `--log-format json` the same facts are `load_complete` /
`load_failed` events (level `info`) and a `run_complete` line, each with
`run_id`, and `--format json` prints the summary object with `loads` and
`loads_failed`. Parse those, never the sentence. A run of `SELECT`s writes no
sentence — the printed rows are its feedback — but under `--log-format json` it
too closes with a `run_complete` line on stderr (never on stdout, which holds
the rows). Every `run_complete` carries a `pushdown` object when a source was
spared anything: parquet `row_groups` considered and `row_groups_skipped` on
statistics, `columns_read` of `columns_total` (parquet or Arrow), and
`sql_filtered_reads`, the SQL reads that carried a pushed filter.

The progress line is drawn only when stderr is a TTY, never under `-q` or `--log-format json`,
not for a terminal `SELECT` (whose rows go to the same screen), and not for the
first 400 ms, so short runs stay silent. Under `--log-format json` the line
becomes an event instead, once a second after those 400 ms, terminal or not —
what a UI reading the log shows in its place (`--no-progress` and `-q` still
turn it off):

```
{"ts":…,"level":"info","run_id":…,"event":"progress","target":"erp.dbo.SC5010 → sr.bronze.sc5010","rows":1204112,"rows_per_sec":48300,"elapsed_ms":24930}
```

`loop_done`/`loop_total` join it inside a `FOR EACH`. A log or `PRINT` line erases it rather
than colliding with it, and the run summary replaces it at the end. Piped or
redirected, stderr carries no control characters at all. `--no-progress` turns
it off.

`basalt run` prints a terminal `SELECT` whole — every row and every column, in a
plain left-aligned table — whether stdout is a terminal, a pipe or a file. What
`-c` returns never depends on who is watching.

The REPL is where a person looks at data, so there the table is fitted to the
terminal: a row of types under the names, numbers right-aligned, `NULL` spelled
out, cells cut at 40 columns, and `…` for what does not fit — the middle columns,
in the manner of pandas, and the middle rows past 40. The footer always gives the
true size:

```
 id  customer_name       amount  …  country  last_col
int  string               float  …  string   string
---  ------------------  ------  …  -------  --------
  0  customer number 0        0  …  BR       tail-0
  …  …                        …  …  …        …
 99  customer number 99   148.5  …  BR       tail-99
(100 rows × 10 columns, 5 shown — \view scrolls it)
```

A REPL result keeps its first 10,000 rows (and the last 20) rather than all of
it. Set `NO_COLOR` to drop the dim and bold styling.

In the REPL, `\view` (or `\v`) opens the last result full-screen: arrows scroll
by column and by row with the names and types pinned, Ctrl-arrows and PgUp/PgDn
move a screenful, Home/End jump to the first and last columns, `g`/`G` to the
first and last rows, `q` leaves.

`basalt repl` executes on a top-level `;` and carries `CREATE CONNECTION` /
`CREATE FUNCTION` / `PARAM` declarations across entries (re-declaring a name
replaces it). Meta commands: `\connections` list the session's declarations ·
`\reset` drop them · `\clear` (or `clear`, `cls`, `^L`) clear the screen · `\format table|json|csv|tsv` switch result output · `\view` scroll
the last result · `\help` ·
`\q`.

The entry is a small text editor rather than a single line, with an editor's
habits. Enter runs the entry when it ends in a top-level `;` and the cursor is at
its end; anywhere else it opens a line — and after `(` it steps in and puts the
`)` on a line of its own. Ctrl+J runs the entry as it
stands, `;` or not (Ctrl+Enter too, where the terminal delivers it). Brackets and quotes close themselves, typing the closer
steps over it, and over a selection they wrap it; the matching bracket is
underlined. Alt+Up/Down move the line or selected lines, Shift+Alt+Up/Down
duplicate them, Tab and Shift+Tab indent and dedent a selection, Ctrl+/ comments
lines out with `--` and back in, Esc drops the selection. The entry is coloured
as you type (keywords, strings, numbers, comments, `$params`; `NO_COLOR` turns
it off), a multi-line entry gets line numbers in its gutter, and a parse error
is shown with a caret under the column it names.

`\connect [type]` makes a connection by asking the few questions its connector
needs — host with the type's usual port, database, user, password (blank means
the `env(NAME_USER)` / `env(NAME_PASS)` convention; `env:VAR` names another
variable) — shows the `CREATE CONNECTION` it built, registers it, and offers to
reach it and to save it to the startup file. `^R` searches the history incrementally (type to narrow, `^R` for an older
match, Enter keeps it). A session starts by running `~/.config/basalt/repl.sql`
(or `$XDG_CONFIG_HOME/basalt/repl.sql`) when it exists — the place for the
connections you always want, with `env()` for the secrets — and `\save` writes
the session's declarations there (or to a named file); `\i <file>` runs any
file so its declarations join the session; `\connections` shows them as a
table with host, database and whether the session has reached them (`\c test`
reaches each now); `\edit` opens the last entry in `$EDITOR` and runs what
comes back.

Tab completes the word under the cursor: keywords (in the case you are typing),
the session's connections, functions and `$params`, the entry's CTEs, a path
inside an unclosed quote, `conn.` followed by that connection's tables, the
columns of every table and file the entry names, and the built-in functions —
scalar, aggregate and window. Tables and columns are asked
of the source once per session, on first use, through the same
`information_schema` queries `SHOW TABLES` and `DESCRIBE` run. A lone match is
taken; several fill in what they share and come up as a row of choices that
Tab cycles through. Arrows travel the whole entry — Ctrl+arrows by word, Home/End the line,
Ctrl+Home/End the entry — and Up or Down past its edge recall history, where an
entry comes back whole however many lines it had (`~/.basalt_history`). Shift
with any of those selects; typing, Backspace and Delete replace the selection.
`^A` selects all, `^C` copies a selection (and drops the entry when there is
none), `^X` cuts, `^V` pastes, `^Z`/`^Y` undo and redo. A paste is inserted as
text, never run line by line, and a copy reaches the system clipboard where the
terminal supports OSC 52. Two things a terminal cannot deliver: the mouse, and
Ctrl+Enter, which arrives as plain Enter.

### `basalt kernel` — a session for a notebook or editor

`basalt kernel` keeps one session alive for a program to drive, the way `repl`
keeps one for a person. Each script it runs sees every `CREATE CONNECTION`,
`CREATE FUNCTION`, `CREATE RESOURCE`, `PARAM` and `LET` an earlier script
declared: they are kept as text and replayed ahead of the next script, so a
script behaves exactly as it would in one file below them all, and re-declaring
a name replaces it. A `WITH` belongs to its query and is not kept — two scripts
may reuse a CTE name. A `LET` keeps the value it was given in the script that
declares it (even one that only declares): `LET t = now()` is one instant for
every later script, and `LET n = (SELECT count(*) ...)` is not queried again,
however its source changes. Declare it again to recompute it. The REPL keeps
`LET`s the same way.

Requests are NDJSON on stdin, one object per line:

```
{"op":"run","id":"c1","script":"SELECT ...;","params":{"days":7},"format":"arrow"}
{"op":"cancel","id":"c1"}     stop the running script; the session survives
{"op":"complete","id":"c2","script":"SELECT erp.","pos":11}
{"op":"check","id":"c3","script":"SELECT ...;","tables":["enrich"]}
{"op":"reset"}                forget every declaration
{"op":"close"}                exit 0 (as does EOF)
```

`complete` answers what `basalt complete` would, against the session's
declarations as well as the script's own; its status carries
`complete: {start, end, items}`. Connections are asked for their tables and
columns once per session and remembered, as the REPL does (`"connect": false`
keeps it offline); `reset` forgets that too. `pos` is a byte offset, or — with
`"utf16": true` — a UTF-16 offset, the unit a JavaScript editor counts in, and
the answer's `start`/`end` then count the same way.

`check` lists every problem in a script without running it, against the
session — its connections, params, LETs and functions — as `basalt check
--format json` does; its status carries `"diagnostics": [...]`, `[]` when the
script checks out, each placed in the script as sent (`file` is `session` when
the fault is in a declaration an earlier script made). `tables` names what
the script may read that no script declared, such as other cells' results:
a name alone (`"enrich"`) is checked as a table with unknown columns;
`{"name": "enrich", "columns": [{"name": "id", "type": "int"}, …]}` types
everything read from it (any type a `PARAM` takes: `int`, `decimal(10,2)`,
`varchar(20)`, `timestamp`, …, each column nullable). A script's own `WITH`
of the same name wins. Nothing runs and nothing connects, and the session is
left as it was: a `check`ed script's declarations are checked, not kept.
`params` binds the script's `PARAM`s for that script only, as `-p` does for a
run; `format` overrides `--format` (default `arrow`) and `max_rows` overrides
`--max-rows` for that script. A `cancel`
acts at once, even mid-script; one naming a different `id` than the running
script's is ignored, and one without an `id` stops whatever runs. SIGINT
cancels the running script too — however many times it is sent, it never ends
the process.

Replies are frames on stdout, each a JSON header line. A `data` header is
followed by exactly `len` raw bytes: what the script wrote to stdout — its
results in `format`, in order, possibly over several frames. A `result` frame
follows the last byte of each result, so the data between two `result` frames
is exactly one result in any format — NDJSON rows of two SELECTs never run
together, and an Arrow reader that cannot read streams in turn gets one stream
at a time. Exactly one `status` closes each request, listing the results again,
and nothing for that request follows it:

```
{"type":"data","id":"c1","len":1184}
<1184 bytes>
{"type":"result","id":"c1","statement":0,"kind":"select","line":2,"col":1,"rows":12,"elapsed_ms":40,"truncated":false}
{"type":"status","id":"c1","ok":true,"cancelled":false,"truncated":false,"elapsed_ms":42,"declared":[{"kind":"param","name":"days"}],"results":[...]}
{"type":"status","id":"c2","ok":false,"cancelled":false,"truncated":false,"elapsed_ms":3,"declared":[],
 "error":{"msg":"unknown field `nope`","file":"script","line":3,"col":8,"transient":false}}
```

A statement that runs past 400 ms also sends a `progress` frame a second —
`{"type":"progress","id":…,"target":"…","rows":…,"rows_per_sec":…,"elapsed_ms":…}`
— for a `SELECT` as well as a `LOAD`, since a notebook shows either filling.
Inside a `FOR EACH` it adds `loop_done` and `loop_total`: rows of the
outermost loop finished, and rows in all — the terminal's `[3/12]`.

Each `LOAD` sends a `load` frame as it finishes, written or failed, in the
order they finish — what the CLI's ` + target` and ` x target` lines say:

```
{"type":"load","id":"c3","load":0,"target":"/tmp/a.parquet","line":1,"col":1,"rows_read":30000000,
 "rows_written":30000000,"elapsed_ms":3270,"lanes":12,"ok":true}
{"type":"load","id":"c3","load":1,"target":"sr.bronze.x","line":2,"col":1,"rows_read":0,"rows_written":0,
 "elapsed_ms":4,"lanes":1,"ok":false,"reason":"sqlserver connect failed: ServerClosedConnection","transient":true}
```

`load` numbers the loads in the order they finished; `line`/`col` are where
the `LOAD` stands, as for a result. `target` is the path or `conn.table` as the
script spelled it, `IDENTIFIER()` and `${...}` rendered. `rows_read` is the
load's own, loads running side by side in a parallel `FOR EACH` included.
A load that fails — before writing a row, too — still reports, with
`reason` and `transient`. Inside a `FOR EACH`, each row's load carries
`loop_row` and `loop_rows` (its row, 1-based, of its own loop) and the
outermost loop's `loop_done`/`loop_total`; a row that fails under `ON ERROR
CONTINUE` reports and the loop goes on.

The status of a `run` repeats them, so a reader that skips frames still has
them — `"loads":[…]` (the frames' fields, without `type`/`id`) — with the
run's totals: `loads_ok`, `loads_failed`, `rows_read`, `rows_loaded` and
`lanes`, the facts of the CLI's closing sentence (`Loaded 11 of 12 targets,
9,482,004 rows in 3m 12s (49.3k rows/s, 12 lanes)`). A failed or cancelled
run carries them too, for the loads that ran before it stopped.
A result's `line`/`col` count in the script as sent, as its Arrow metadata
does; `truncated` says a row cap cut it, and the status's `truncated` says any
result was. `declared` lists what the script added to the session — a script's
declarations join the session once it parses, whether or not it then runs
cleanly. An error's `line`/`col` count in the script as sent; `file` is
`script`, an `@include`d file's path, or `session` when the fault lies in a
declaration an earlier script made. `end_line`/`end_col`, when present, end
the offending name, as under `--log-format json`. `transient` is the exit-`75`
class below.
Results written before a failing statement are still delivered. Logs and
`PRINT` stay on stderr (`--log-level`, `--log-format`); the item lines and
the per-run summary are left out — the `load` frames and the status carry
them.

| code | meaning |
|------|---------|
| `0`  | success |
| `1`  | permanent failure (bad script, data/schema error) — maps to HTTP 422 |
| `75` | transient (`EX_TEMPFAIL`) — safe to retry — maps to HTTP 503 |
| `130`| aborted (SIGINT) |

Transient means the network or the peer failed, not the script: a refused,
reset or timed-out connection, a failed name lookup, an HTTP 429/5xx, and a
database that closed the connection or failed on its socket — during login
(a server turning connections away under a burst of parallel logins) or
mid-query (`ServerClosedConnection`, `ConnectionIoFailed`). Past an HTTP
source's in-place backoff and a SQL sink's one reconnect mid-write, basalt does
not retry: the exit code hands the decision to whatever scheduled the run. A `FOR EACH` whose failed items are all transient exits `75` too. The same
end-of-file or write failure on a local file stays permanent.

## 11. Designed but not yet implemented

Accepted design not yet in the engine:

- **Whole-CTE / cross-source pushdown** (§5's full Trino model). Filters now move
  below a join and across an equijoin key (§5), which was the case that mattered
  most — before it, a join meant *no* predicate descended at all and a query over
  an 80M-row table read the whole thing. What is still missing: a multi-stage CTE
  that is entirely one connection is not collapsed into a single descended query,
  aggregates and joins are never pushed into the source the way Trino's
  `applyAggregation`/`applyJoin` do, and there is no runtime/dynamic filter — the
  build side's key values are not sent back to the probe scan, so a selective
  predicate on a *non-key* dimension column still reads the whole fact table.
  And a filter on a join's right side by a bare name (`WHERE valor > 0` rather
  than `i.valor`) stays above the join, as does one after an outer join (§5).

Deliberately partial:

- **The regex engine is a subset** (`exec/regex.zig`), sized for the patterns
  SQL actually carries rather than for a dependency. It supports `^ $ . |`,
  character classes with ranges and negation, capturing and non-capturing
  groups, the escapes `\d \w \s` (and their negations), the quantifiers
  `* + ?` and counted `{n} {n,} {n,m}` — each greedy, or lazy with a `?`
  suffix (`*?`, `{2,4}?`). Lookaround and backreferences inside the pattern
  are **rejected at compile time** rather than read as literal characters, so
  an unsupported pattern is an error and never a silently wrong match.
