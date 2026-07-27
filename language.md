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
6. [`UNION ALL BY NAME`](#6-union-all-by-name--reconciliation-by-name)
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
CREATE CONNECTION ...;            -- named data endpoints
CREATE FUNCTION f(a) AS <expr>;   -- scalar functions (inlined at plan time)
CREATE FUNCTION p(a) AS ... END;  -- statement functions, invoked with CALL (§9)
LOAD INTO ... AS <query>;         -- output pipeline(s)
<query>;                          -- terminal SELECT = print to stdout
CALL p('x');                      -- run a statement function
FOR EACH ROW OF (...) ... END FOR;
CASE ... END CASE;                -- plan-time dispatch
```

`@include` splices another script's declarations ahead of this one at plan
time: each included file is parsed separately (errors report the included
file's own path and line), includes may nest (depth 16, cycles rejected), and
paths resolve relative to the including file.

- **Batch is the silent default.** A script with no `CREATE ENDPOINT` runs once
  to completion (exit codes in §10).
- Keywords are case-insensitive; identifiers keep their case.
- Comments: `--` to end of line, `/* ... */` blocks.
- Strings are `'...'` (double `''` for a literal quote). `"..."` is also
  accepted with BSL backslash escapes.
- **Raw SQL literals** use Postgres dollar-quoting: `$$...$$`, or
  `$tag$...$tag$` when the body contains `$$`. No escaping inside; `${...}`
  interpolation of loop vars still applies within them (§7).
- **Dynamic names** (per-row table/sink names, keys) use `$var` +
  `IDENTIFIER()` + `||`, not raw string interpolation — see §7.

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
  NUMERIC ...`).
- Source defaults: scalars bind from the query string, `JSON` from the body.

**`LET name = <expr>;`** is PARAM's sealed sibling: a script-scoped constant
folded once at plan time (in declaration order; it may reference `$params` and
earlier `$lets`) and referenced as `$name`. It can never be bound externally —
`-p name=...` is an error, HTTP binding ignores it, and it is not part of an
endpoint's parameter surface. A LET and a PARAM may not share a name. `LET
run_ts = now();` gives one consistent timestamp across every pipeline of a run.

## 3. Connections

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  database = 'totvs',
  tls      = 'require'
);
```

Connector types and their options are unchanged from BSL: `sqlserver`
(`host port database user password tls auth tenant client_id resource`),
`mysql`, `postgres`, `starrocks` (`fe_host fe_port be_url database buckets
replication_num auto_create label_prefix ...`), `http`.

**Named SQL Server instances:** write `host = '10.110.2.5\WMS'`. When a `host`
carries a `\INSTANCE` and no explicit `port` is given, basalt resolves the
instance's TCP port via the SQL Server Browser (UDP 1434) before connecting.
Give an explicit `port` to skip the lookup — the robust choice where UDP 1434
is firewalled but the TDS port is open. (`*.dynamics.com` / Azure SQL are
default-instance cloud endpoints, so this never applies there.)

**Credentials by convention:** connection `erp` resolves `ERP_USER` /
`ERP_PASS` from the environment at connect time — the common case costs zero
characters. Explicit `user = ...` / `password = ...` options override the
convention. Azure Blob paths (`az://...`, §5) resolve `AZURE_STORAGE_KEY`, and
`AZURE_BLOB_ENDPOINT` points them at an emulator. Secrets are never literals
in the script; always environment indirection.

`CREATE OR REPLACE CONNECTION` re-declares an existing name.

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
  `LOAD INTO '/out/x.csv'`, `LOAD INTO '/out/x.parquet'`, or an object-store
  path `LOAD INTO 'az://account/container/bronze/x.parquet'`.
- A per-row dynamic target uses `IDENTIFIER(<string-expr>)` over loop vars
  (§7): `LOAD INTO sr.IDENTIFIER('crm_' || lower($name)) ...`.
- Dispositions: `APPEND` (default, omissible) · `REPLACE` (overwrite) ·
  `UPSERT ON (k1, k2)` · `UPSERT ON (id) PARTIAL COLS (a, b)` · bare `UPSERT`
  (infer the PK from the source table's metadata at plan time — needs a table
  read on a SQL source that exposes it). An empty/unresolved upsert key is an
  error, never a silent no-op.
- `SPLIT BY (col)` parallelizes the load by key ranges; `JOBS n` fixes the
  lane count (otherwise the CLI `-j` applies).

**stdout is not syntax**: a terminal `SELECT ...;` statement prints the result
as an aligned table — `basalt run -c "SELECT * FROM 'x.csv'"` works as a
mini-DuckDB.

## 5. Queries

```sql
WITH pedidos AS (                          -- CTE = named binding
  SELECT filial, num, valor
  FROM erp.dbo.SC5010
    PUSHDOWN($$D_E_L_E_T_ <> '*'$$)        -- raw predicate, verbatim to the source
  WHERE valor > 0                          -- translated predicate
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
| file — CSV or Parquet | `FROM 'path.csv'` / `FROM 'path.parquet'` — the extension picks the reader; local or HTTPS URL |
| object storage | `FROM 'az://account/container/path.parquet'`; a trailing `/` reads every blob under the prefix as one table |
| REST (connection) | `FROM crm.'/v1/customers'` (path on the conn's base URL) |
| REST (bare URL) | `FROM HTTP('https://host/api/x')` |
| request body | `FROM BODY (col TYPE [NOT NULL], ...)` (§8) |
| durable buffer | `FROM BUFFER 'name'` (§8) |
| discovered union | `FROM EACH TABLE OF (...)` (§6) |
| generated integers | `FROM RANGE(10)` / `FROM RANGE(2, 5)` — `lo..hi-1` as a `range` column; bounds are int literals or params |
| no source | `SELECT 1 AS x, now() AS t;` — a `SELECT` with no `FROM` yields one row of computed values |
| CTE | `FROM <name>` |

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched.

Source clauses, in any order after the source:

- **`PUSHDOWN(<expr>)`** — a raw predicate sent verbatim into the generated
  source query's `WHERE` (the successor of BSL `@[where]`). The argument is a
  string expression: a `$$...$$` literal (`PUSHDOWN($$D_E_L_E_T_ <> '*'$$)`),
  a loop-var value (`PUSHDOWN($where)`), or one built with `||`. ANDed with
  whatever the translated `WHERE` pushes down. Empty ⇒ no clause. Syntax errors
  surface at the source at runtime (permanent, exit 1).
- **Implicit pushdown** — the contiguous `WHERE` (filter) prefix directly after
  a SQL table/query read is translated into that source query's `WHERE`
  automatically. Translatable: comparisons, `AND`/`OR`/`NOT`, `IS [NOT]
  NULL`/`EMPTY`, `IN`, `LIKE`, `CASE`/`IF`, `CAST`, and the portable string
  functions (`lower upper length trim substr replace concat coalesce
  starts_with ends_with contains`). Untranslatable pieces (arithmetic,
  `now()`/`today()`, user funcs) stay in the engine — the filter is always
  kept, so results never change, only how much crosses the wire. `EXPLAIN`
  prints the descended predicate on a `pushdown:` line.
- **`PAGINATE BY page|offset|cursor (param = 'page', size = 100,
  total = 'count', field = 'next', start = 2, max = 50)`** — REST pagination.
  Friendly keys map to the engine hints (`param`→`page_param`/`cursor_param`,
  `size`→`page_size`, `total`→`total_field`, `field`→`cursor_field`,
  `start`→`start_page`, `max`→`max_pages`); unknown keys pass through.
- **`RETRY n [ON (429, 503)]`** — retries + retryable statuses.
- **`WITH (k = v, flag, ...)`** — residual source options: `items` (dotted
  path to the row array when the response nests it, e.g. `items = 'data.rows'`
  — a bare array needs nothing), `buffer` (drain the source fully before
  opening the sink), `prefetch`, `timeout_ms`, `header = 'Name: value'`,
  `auth` forms, `method`/`body` for POST sources, etc.

`WHERE` on a REST source runs in basalt after the fetch; on a SQL table it is
pushdown. Same word, different plan — `EXPLAIN` shows which.

A complete REST read, for orientation:

```sql
SELECT id AS crate, downloads
FROM HTTP('https://crates.io/api/v1/crates?per_page=100&sort=downloads')
  PAGINATE BY page (param = 'page', size = 100, total = 'meta.total', max = 5)
  RETRY 2 ON (429, 503)
  WITH (items = 'crates')
WHERE downloads > 0;
```

### Operators

| clause | plan stage |
|--------|-----------|
| `WHERE <expr>` | filter |
| `SELECT a, expr AS x` | projection |
| `SELECT * EXCLUDE (a, b)` / `EXCEPT` | all-but projection |
| `SELECT * RENAME (a AS b)` | rename projection |
| `COUNT(*) / SUM / AVG / MIN / MAX ... GROUP BY k` | aggregate (non-agg items must be group keys) |
| `COUNT(DISTINCT x)` | aggregate — combines freely with other aggregates; ignores nulls |
| `HAVING <expr>` | filter after the aggregate; aggregate calls in it refer to the columns it produced |
| `ORDER BY a DESC, b` | sort |
| `LIMIT n [OFFSET m]` | limit |
| `SELECT DISTINCT` / `DISTINCT ON (a, b)` | distinct |
| `CROSS JOIN UNNEST(SPLIT(tags, ',')) AS tag` | explode (also `UNNEST(col)`) |
| `[INNER\|LEFT\|RIGHT\|FULL\|CROSS\|SEMI\|ANTI] JOIN <cte> x ON a = b` | join (right side must be a CTE) |

Row order without `ORDER BY` is not defined: `GROUP BY` returns groups in
hash-partition order, and `DISTINCT` and map pipelines reorder under `-j > 1`
(which is the default, since `-j` defaults to the core count). Add `ORDER BY`
whenever the order matters.

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

- `GROUP BY <n>` is positional — it names the *n*-th `SELECT` item.
- `GROUP BY <expr>` accepts a computed key (`GROUP BY ClientIP - 1`).
- `ORDER BY` may name a column the `SELECT` list does not project. It is
  carried through the projection as a hidden column and dropped after the
  `LIMIT`, so sorting by an unselected column costs nothing in the output.

## 6. `UNION ALL BY NAME` — reconciliation by name

Alignment is **by column name**: NULL-fill missing, drop extra, cast type
differences. (ANSI `UNION ALL` is positional — this is the DuckDB
`UNION ALL BY NAME`.)

```sql
-- explicit branches: the tag is just a literal column
SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
UNION ALL BY NAME
SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
ANCHOR SCHEMA erp.dbo.CT2010;          -- schema authority (optional)
```

Each branch must be exactly `SELECT ['lit' AS col,] t.* FROM <conn>.<table>`.

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
- Loop variables may be typed: `AS (name, port:INT)`.
- A loop variable is also an ordinary expression **value**: `SELECT $name AS
  empresa`, `WHERE $port > 1000` (typed vars compare as their declared type).
  Inside the loop body a loop var shadows a same-named source column in
  expression position — the same rule params follow.
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
| a raw predicate value | `PUSHDOWN($where)` |
| a conditional key | `UPSERT ON (IDENTIFIER(if($pk = '', $name \|\| 'id', $pk)))` |
| a per-row column value | `SELECT $name AS empresa` — a plain expression, no quoting |

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
time. Bare names are row/local scope — columns, `LET … IN` bindings, aliases.
At a use site the innermost binding wins: loop var > LET/PARAM.

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
  parses as `YYYY-MM-DD[ HH:MM:SS]`.
- A `DATE`/`TIMESTAMP` column compares directly against an ISO string literal
  (`WHERE d >= '2013-07-01'`). The literal is coerced to the column's type,
  never the reverse, and it is validated at plan time — so `'2013-13-01'` and
  `'01/07/2013'` are errors from `check`, not silent text comparisons.
- `x LIKE 'a%'`, `x IN (1, 2, 3)` (expands to an OR-chain).
- `LET x = <val> IN <body>` — local binding, inlined at plan time.
- Scalar functions (case-insensitive): `now() today() lower() upper() length()
  strlen() trim() substr() replace() concat() coalesce() starts_with()
  ends_with() contains() like() date_trunc() extract() regexp_replace()` ·
  math `abs() floor() ceil() round(x[,n]) mod() power() sqrt() sign()` (round
  is half-away-from-zero, deliberately engine-side) · nulls `nullif()
  greatest() least()` (null args ignored, Postgres-style) · strings `lpad()
  rpad() left() right() split_part() strpos() repeat() reverse()` · dates
  `date_add(unit, n, ts) date_diff(unit, a, b) make_date() epoch()
  to_timestamp() strftime(ts, fmt)` (`%Y %m %d %H %M %S %y %%`; month/year
  arithmetic clamps the day-of-month).
- `TRY_CAST(x AS T)` — CAST that yields null instead of failing on a bad
  value; the workhorse for dirty inputs. Never pushed down.
- `DATE_TRUNC('minute', ts)` and `EXTRACT(minute FROM ts)` — units `year`,
  `month`, `day`, `hour`, `minute`, `second`. `EXTRACT` also accepts the
  ordinary two-argument call form. `STRLEN` is an alias for `LENGTH`.
- `REGEXP_REPLACE(s, pattern, replacement)` — replaces the first match;
  `\1`…`\9` in the replacement expand to captured groups (`\0` is the whole
  match). A literal pattern is compiled at plan time, so a malformed one fails
  `check`. See §11 for the supported syntax.
- `CREATE [OR REPLACE] FUNCTION nome(a [TYPE] [DEFAULT <expr>], ...)` — two
  body forms. `AS <expr>;` is a scalar function, inlined at plan time;
  recursion and arity mismatches are compile errors, declared types are
  checked against literal arguments at the call site, defaults fill omitted
  trailing arguments. A body starting with `LOAD`/`FOR`/`CALL`/`SELECT`/`WITH`
  is a **statement function** terminated by `END;` and invoked with
  `CALL nome(args);` — its params bind like loop variables (`$name`,
  `IDENTIFIER($name)`, `PUSHDOWN($f)`, `${name}` in strings), rendered per
  call through the same machinery as a `FOR EACH ROW OF` body. CALL nesting is
  depth-guarded (16); a statement function is not atomic — a mid-body failure
  leaves earlier loads committed, exactly as if the statements were inline.
  Plain re-declaration of a name is an error; `OR REPLACE` is the sanctioned
  overwrite.

### `EXPLAIN`

`EXPLAIN <statement>` prints the plan instead of running it.
`EXPLAIN ANALYZE <statement>` runs it and prints the operator tree with the
time and row count each stage actually cost — time is *exclusive*, so a
stage's figure excludes its inputs.

```console
$ basalt run -c "EXPLAIN ANALYZE SELECT g, COUNT(*) AS c FROM 'x.parquet' GROUP BY g;"
actuals (exclusive time):
  aggregate      13.1ms          601 rows        2 batches
    scan          6.2ms       500000 rows        6 batches
```

`EXPLAIN COSTS` is rejected at parse time — there is no cost model to report.
Actuals cover the serial pipeline; a parallel run reports per lane and is not
yet summed into one tree.

## 10. Running & exit codes

```
basalt run   <script>|-|-c "<inline>" [-p key=value ...] [-j threads] [--format table|json]
basalt serve <dir> [--port N] [--watch]
basalt check <script>|-|-c "<inline>" [--connect]
```

`--format json` makes stdout machine-readable: a terminal `SELECT` emits one
JSON object per row (NDJSON, streamed — decimals as strings, temporals as ISO
text, bytes as base64), and a `LOAD` run emits one summary object instead.
Logs are stderr-only, plain text, level `warn` by default (`--log-level`,
`--log-format json`, `-q`).

`basalt repl` executes on a top-level `;` and carries `CREATE CONNECTION` /
`CREATE FUNCTION` / `PARAM` declarations across entries (re-declaring a name
replaces it). Meta commands: `\connections` list the session's declarations ·
`\clear` drop them · `\format json|table` switch result output · `\help` ·
`\q`. For history/arrow keys run it under `rlwrap`.

| code | meaning |
|------|---------|
| `0`  | success |
| `1`  | permanent failure (bad script, data/schema error) — maps to HTTP 422 |
| `75` | transient (`EX_TEMPFAIL`) — safe to retry — maps to HTTP 503 |
| `130`| aborted (SIGINT) |

## 11. Designed but not yet implemented

Accepted design not yet in the engine:

- **Whole-CTE / cross-source pushdown** (§5's full Trino model): implicit
  pushdown currently translates the filter prefix of a *single* SQL read. A
  multi-stage CTE that is entirely one connection isn't yet collapsed into one
  descended query, and a cross-connection join still materializes the smaller
  side in the engine (correct, just not maximally pushed). The predicate
  translator (`runtime/pushdown.zig`) is the reusable core when this lands.

Deliberately partial:

- **The regex engine is a subset** (`exec/regex.zig`), sized for the patterns
  SQL actually carries rather than for a dependency. It supports `^ $ . |`,
  character classes with ranges and negation, capturing and non-capturing
  groups, the escapes `\d \w \s` (and their negations), the quantifiers
  `* + ?` and counted `{n} {n,} {n,m}` — each greedy, or lazy with a `?`
  suffix (`*?`, `{2,4}?`). Lookaround and backreferences inside the pattern
  are **rejected at compile time** rather than read as literal characters, so
  an unsupported pattern is an error and never a silently wrong match.
