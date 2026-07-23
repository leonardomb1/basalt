# The Basalt SQL Language (`.sql`)

Basalt scripts describe a **columnar data pipeline**: read from a source,
transform with a query, write to a sink. A script is plan-time static — parsed,
type-checked, and planned once, then executed as a streaming pull pipeline.

This is the reference for the SQL dialect (v0.2, migration.md). It is derived
from the parser (`src/lang/sql_parser.zig`) and reflects what the engine
actually accepts. Basalt SQL is the only dialect: the BSL (`.bsl`) parser was
removed in v0.2.0 — `examples/golden/` holds the frozen plans that gated the
removal.

---

## 1. Program structure

A script is a sequence of `;`-terminated statements:

```sql
CREATE ENDPOINT '/x' DOC '...';   -- only for HTTP mode; absent = batch
PARAM ...;                        -- request/CLI inputs
CREATE CONNECTION ...;            -- named data endpoints
CREATE FUNCTION f(a) AS <expr>;   -- user scalar functions (inlined at plan time)
LOAD INTO ... AS <query>;         -- output pipeline(s)
<query>;                          -- terminal SELECT = print to stdout
FOR EACH ROW OF (...) ... END FOR;
CASE ... END CASE;                -- plan-time dispatch
```

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

**Credentials by convention:** connection `erp` resolves `ERP_USER` /
`ERP_PASS` from the environment at connect time — the common case costs zero
characters. Explicit `user = ...` / `password = ...` options override the
convention. Secrets are never literals in the script; always environment
indirection.

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

- File target by path: `LOAD INTO '/out/x.csv' AS ...` (CSV writer).
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
| CSV file / HTTPS CSV | `FROM 'path-or-url.csv'` |
| REST (connection) | `FROM crm.'/v1/customers'` (path on the conn's base URL) |
| REST (bare URL) | `FROM HTTP('https://host/api/x')` |
| request body | `FROM BODY (col TYPE [NOT NULL], ...)` (§8) |
| discovered union | `FROM EACH TABLE OF (...)` (§6) |
| CTE | `FROM <name>` |

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
  kept, so results never change, only how much crosses the wire. `check -s`
  prints the descended predicate on a `pushdown:` line.
- **`PAGINATE BY page|offset|cursor (param = 'page', size = 100,
  total = 'count', field = 'next', start = 2, max = 50)`** — REST pagination.
  Friendly keys map to the engine hints (`param`→`page_param`/`cursor_param`,
  `size`→`page_size`, `total`→`total_field`, `field`→`cursor_field`,
  `start`→`start_page`, `max`→`max_pages`); unknown keys pass through.
- **`RETRY n [ON (429, 503)]`** — retries + retryable statuses.
- **`WITH (k = v, flag, ...)`** — residual source options: `buffer` (drain the
  source fully before opening the sink), `prefetch`, `timeout_ms`, `auth`
  forms, `method`/`body` for POST sources, etc.

`WHERE` on a REST source runs in basalt after the fetch; on a SQL table it is
pushdown. Same word, different plan — `check -s` shows which.

### Operators

| clause | plan stage |
|--------|-----------|
| `WHERE <expr>` | filter |
| `SELECT a, expr AS x` | projection |
| `SELECT * EXCLUDE (a, b)` / `EXCEPT` | all-but projection |
| `SELECT * RENAME (a AS b)` | rename projection |
| `COUNT(*) / SUM / AVG / MIN / MAX ... GROUP BY k` | aggregate (non-agg items must be group keys) |
| `ORDER BY a DESC, b` | sort |
| `LIMIT n [OFFSET m]` | limit |
| `SELECT DISTINCT` / `DISTINCT ON (a, b)` | distinct |
| `CROSS JOIN UNNEST(SPLIT(tags, ',')) AS tag` | explode (also `UNNEST(col)`) |
| `[INNER\|LEFT\|RIGHT\|FULL\|CROSS\|SEMI\|ANTI] JOIN <cte> x ON a = b` | join (right side must be a CTE) |

Table aliases (`FROM t a`, `JOIN c b`) are stripped at parse time — the engine
sees bare column names.

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
  loop vars positionally) or a JSON param path (`$tables`, `$job.tables`, …;
  object fields bound to the loop vars by name, a missing field ⇒ `""`).
- Loop variables may be typed: `AS (name, port:INT)`.
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
| a computed sink name | `LOAD INTO conn.IDENTIFIER('pre_' \|\| lower($name))` |
| a raw predicate value | `PUSHDOWN($where)` |
| a conditional key | `UPSERT ON (IDENTIFIER(if($pk = '', $name \|\| 'id', $pk)))` |

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

SQL-ish, Pratt-parsed. Precedence (high→low): unary `- NOT` → `* / %` →
`+ - ||` → comparisons `= == != <> < <= > >= LIKE IN IS` → `??` → `AND` → `OR`.

- `$name` — a reference to a param or (inside `FOR EACH ROW OF`) a loop
  variable, resolved by name. `$job.a?.b` navigates a JSON param.
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
- `CAST(x AS INT)` / `CAST(x AS DECIMAL(18,2))` — implicit widening is
  int→float/decimal only.
- `x LIKE 'a%'`, `x IN (1, 2, 3)` (expands to an OR-chain).
- `LET x = <val> IN <body>` — local binding, inlined at plan time.
- Scalar functions (case-insensitive): `now() today() lower() upper() length()
  trim() substr() replace() concat() coalesce() starts_with() ends_with()
  contains() like()`.
- `CREATE FUNCTION nome(a) AS <expr>;` — inlined at plan time; recursion and
  arity mismatches are compile errors.

## 10. Running & exit codes

```
basalt run   <script>|-|-c "<inline>" [-p key=value ...] [-j threads]
basalt serve <dir> [--port N] [--watch]
basalt check <script>|-|-c "<inline>" [-s|--show-plan] [--connect]
```

| code | meaning |
|------|---------|
| `0`  | success |
| `1`  | permanent failure (bad script, data/schema error) — maps to HTTP 422 |
| `75` | transient (`EX_TEMPFAIL`) — safe to retry — maps to HTTP 503 |
| `130`| aborted (SIGINT) |

## 11. Designed but not yet implemented

From migration.md, accepted design not yet in the engine:

- **Whole-CTE / cross-source pushdown** (§7's full Trino model): implicit
  pushdown currently translates the filter prefix of a *single* SQL read. A
  multi-stage CTE that is entirely one connection isn't yet collapsed into one
  descended query, and a cross-connection join still materializes the smaller
  side in the engine (correct, just not maximally pushed). The predicate
  translator (`runtime/pushdown.zig`) is the reusable core when this lands.
- **Generalized `EACH TABLE OF (SELECT ...)`** discovery — needs the same
  query→SQL translation; the raw `QUERY($$...$$)` form covers it meanwhile.
- **`$var` reflection is wired for names, not values.** `$name` works in
  `IDENTIFIER()`, `PUSHDOWN`, and target/key positions (§7). A loop var used
  as a computed `SELECT` **value** still needs the raw form
  (`SELECT '${emp}' AS empresa`), and a dynamic **CSV file path** still uses
  string interpolation inside the quote (`FROM 'dir/${name}.csv'`) — neither
  accepts a bare `$var`/`||` expression yet.

The golden corpus that gated the BSL parser's removal lives in
`examples/golden/` — see its README for the comparison rules.
