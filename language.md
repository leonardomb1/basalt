# The Basalt Language (`.bsl`)

Basalt scripts describe a **columnar data pipeline**: read from a source, transform
through a chain of operators, write to a sink. A script is plan-time static — it is
parsed, type-checked, and planned once, then executed as a streaming pull pipeline.

This document is the full surface syntax and the conventions the planner enforces.
It is derived from the parser/lexer (`src/lang/`) and runtime (`src/runtime/run.zig`),
so it reflects what the engine actually accepts, not an aspirational grammar.

---

## 1. Program structure

A script is a sequence of statements. The **first statement must be a `@kind` tag**
(`@batch` or `@http`). After that, statements may appear in any order the references
allow:

```
@batch | @http(path = "/x")     # required first; execution mode
param   ...                      # request/CLI inputs
connection name = connector ...  # named data endpoints
fn name(a, b) = <expr>           # user-defined scalar functions (inlined)
let name = <pipeline>            # reusable named pipeline (a binding)
match ... end                    # plan-time structural dispatch
for v,... in <source> <pipeline> # plan-time fan-out
<pipeline>                       # the output pipeline (read ... | ... | write ...)
```

Whitespace and newlines are insignificant. `#` starts a line comment. Identifiers are
`[A-Za-z_][A-Za-z0-9_]*`. Strings are `"..."` with escapes `\" \\ \n \t \r \'`
(an unknown escape like `\d` is kept verbatim, so Windows paths survive).

### `@kind`

- `@batch` — run once to completion. Exit code is the result (see §11).
- `@http(path = "/route")` — expose the pipeline as an HTTP endpoint. The request
  body/query/headers feed `param` declarations. `path` defaults to `/`. Served by
  `basalt serve <dir>`, which routes each script by its declared `path`.

The `@kind` parentheses take `key = value` attributes: `path` (the route) and an
optional `doc` (`@http(path = "/ingest", doc = "Ingest events")`) — a one-line route
description shown in the `basalt serve` startup banner.

---

## 2. Parameters

```
param since   timestamp                      # typed scalar, required
param limit   int = 1000                      # typed scalar with a default
param region  string from query               # bound from ?region=...
param token   string from header              # bound from a request header
param job     json from body                  # whole JSON body, navigated by path
```

- Types: `bool int float string bytes date time timestamp decimal(p,s)`.
- `from query | body | header` chooses the binding source (for `@http`). Without
  `from`, a `@batch` param comes from `-p key=value` on the CLI.
- `param x json from body` is special: `x` is a **JSON document**, not a scalar.
  Navigate it with dotted paths in expressions — `x.source.host`, `x.n` — which are
  resolved to literals at plan time (`expand.zig`). An unbound path (offline `check`)
  resolves to `null`.
- A JSON param is also the canonical **`for` fan-out source** (§8): `for t in job.tables`.

---

## 3. Connections

```
connection mssql = sqlserver
  host = "sql.internal" port = 1433
  database = "erp"
  user = env("DB_USER") password = secret("DB_PASS")
  tls = "require"

connection sr = starrocks
  fe_host = "10.0.0.7" fe_port = 9030
  be_url = "http://10.0.0.10:8040"
  database = "bronze"
  user = env("SR_USER") password = secret("SR_PASS")
```

Form: `connection <name> = <connector> <key = value>...`. Config values must be a
literal or `env("VAR")` / `secret("VAR")` (both read an environment variable; the two
names are interchangeable — `secret` only documents intent).

**Connectors and their config keys:**

| connector    | keys |
|--------------|------|
| `sqlserver`  | `host` `port`(1433) `database` `user` `password` `tls` `auth` `tenant` `client_id` `resource` |
| `mysql`      | `host` `port`(3306) `database` `user` `password` |
| `postgres`   | `host` `port`(5432) `database` `user` `password` |
| `starrocks`  | `host`/`fe_host` `fe_port` `be_url`/`load_url` `database` `user` `password` `buckets` `replication_num` `auto_create` `label_prefix` |
| `csv`        | path-based, no connection needed (`read csv "path-or-url"`) |
| `http`       | base URL + auth on the connection; see §5/§9 |

- `tls = "require"` forces an encrypted TDS connection (needed by Azure SQL / Dataverse).
- `auth = "aad"` switches SQL Server login to **Azure AD username+password** (federated
  ADFS or managed, auto-detected; no app registration). `tenant` / `client_id` /
  `resource` default sensibly (resource defaults to the org URL for `*.dynamics.com`).
- MySQL / StarRocks logins auto-negotiate the auth plugin: `mysql_native_password`,
  `caching_sha2_password`, and `mysql_clear_password` (sent when the server defers to an
  external IdP — e.g. StarRocks with LDAP / security integration). `mysql_clear_password`
  and `caching_sha2` full-auth put the password on the wire in the clear, so set
  `tls = "require"` whenever the network path to the server isn't already trusted.

---

## 4. Pipelines and operators

A pipeline is `stage | stage | ...`. The first stage is a **source**, the last is
usually a **sink** (`write`). Each stage may carry trailing `@[hints]` (§7).

```
read mssql query "select id, total from orders where total > 0"
  | filter total > 100
  | select id, amount = total, tier = if(total > 1000, "gold", "std")
  | sort amount desc
  | write sr stream_load orders upsert on id
```

### Sources (first stage)

| syntax | meaning |
|--------|---------|
| `read <conn> table <qualified.name>` | read a whole table |
| `read <conn> query "<SQL>"` | read a raw SQL query (no dialect translation) |
| `read csv "<path-or-https-url>"` | read a CSV file or HTTPS CSV |
| `read http "<url>"` / `read <httpconn> request` | read a REST/JSON source (§9) |
| `union ...` | reconcile + concatenate many tables (§6) |
| `<binding-name>` | use a `let` binding as the source |

### Transforms (middle stages)

| operator | syntax | notes |
|----------|--------|-------|
| `filter` | `filter <bool-expr>` | row predicate |
| `select` | `select <item>, ...` | projection; items below |
| `aggregate` | `aggregate n = sum(x), c = count() by region, day` | group-by; funcs: `count sum avg min max` |
| `sort` | `sort a desc, b asc` | `asc` default |
| `distinct` | `distinct` or `distinct on a, b` | dedup |
| `limit` | `limit 100` or `limit 100 offset 20` | row cap |
| `explode` | `explode tags as tag on ","` | one row per element (`as`/`on` optional) |
| `join` | `join <binding> on left.k = right.k` | kinds: `inner left right full semi anti cross` (`cross` takes no `on`) |

**`select` item forms:**
- `*` — all columns
- `* except (a, b)` — all but the named columns
- `* rename (old as new, ...)` — all columns, with the named ones renamed (an
  unknown `old` is an error)
- `field` or `a.b` — a column passthrough
- `name = <expr>` — a computed column. The alias may be a **quoted string** so a
  `${var}` can build it: `"${name}_EMPRESA" = emp`.

### Sink (last stage)

```
write <conn> [<form>] <target> [<mode>]
write stdout                          # bare: no target
write csv "/out/orders.csv"
write sr stream_load orders upsert on id @[split = id]
```

- `<form>` is an optional connector verb (e.g. `stream_load` for StarRocks).
- `<target>` is a qualified name or a **quoted string** (so `${var}` can template it,
  e.g. `"crm_${lower(name)}"`).
- `<mode>` (write disposition) — see §10.

---

## 5. Expressions

Pratt-parsed, SQL-ish. Precedence (high → low): unary `- not` → `* / %` → `+ -` →
comparisons `== != < <= > >=` → `??` → `and` → `or`. (`=` is also accepted as equality
inside a `join ... on`.)

- **Literals:** `null`, `true`, `false`, ints, floats, `"strings"`.
- **Columns:** `id`, `table.col`, `a.b.c`.
- **Conditional:** `if(cond, then, else)`.
- **Null tests:** `x is null`, `x is not null`.
- **Empty tests:** `x is empty`, `x is not empty` — true when `x` is null **or** an
  empty string. `x` must be a string operand (a non-string is a type error — use
  `is null`). Handy for `for`/JSON loop values, where a missing field binds to `""`
  (see §8), so `pk is empty` covers both the missing and the blank case.
- **Null-coalesce:** `a ?? b` — the first non-null of `a`, `b` (sugar for
  `coalesce(a, b)`; chains right-to-left: `a ?? b ?? c`).
- **Cast:** `cast(x as int)` / `cast(x as decimal(18,2))` (implicit widening is
  *int→float/decimal* only; everything else needs an explicit cast).
- **Safe navigation:** `a?.b` — like `a.b`, but on a JSON-param path a missing/null
  intermediate resolves the whole path to `null` instead of erroring (`job.src?.host`,
  and likewise on a `for ... in job?.tables` source, where a missing path yields zero
  iterations). Plain `.` still errors on a missing key. `?.` applies only to JSON-param
  paths — using it on a plain column reference is a type error.
- **Local binding:** `let name = <value> in <body>` — names an intermediate inside an
  expression (so a `fn` body or computed column need not repeat a subexpression). It is
  inlined at plan time: `let d = id + 1 in d * d` becomes `(id+1) * (id+1)`. Not
  available inside a `${...}` interpolation hole.
- **Match expression** (see §6).
- **Function call:** `name(args...)`.

**Built-in scalar functions:** `now()` `today()` `lower(s)` `upper(s)` `length(s)`
`trim(s)` `substr(s, start, len)` `replace(s, from, to)` `concat(a, b, ...)`
`coalesce(a, b, ...)` `starts_with(s, p)` `ends_with(s, p)` `contains(s, p)` `like(s, pat)`.

**User functions** are inlined at plan time and then disappear:
```
fn empresa(t) = substr(t, 4, 2)
... | select e = empresa(id)        # becomes substr(id, 4, 2)
```
Recursion and arity mismatches are compile errors. A `fn` body is a single expression,
but `let name = <value> in <body>` lets it name intermediates: `fn net(p) = let b =
cast(p as int) in b + b/10`.

---

## 6. `match` and `union`

**Match expression** — a value, two forms:
```
# subject form ( `,` alternation, `_` default )
grade = match status "paid", "ok" => "done"  _ => "open" end
# guard form (no subject; first true guard wins)
tier  = match amount >= 1000 => "gold"  amount >= 100 => "silver"  _ => "std" end
```

**Match statement** — plan-time structural dispatch; arm bodies are `{ ... }` blocks
of whole statements (pipelines). Runs once over params/loop variables:
```
match env
  "prod" => { read mssql table orders | write sr stream_load orders }
  _      => { read csv "sample.csv"   | write stdout }
end
```

**`union`** — reconcile N tables to a common schema and concatenate. Reconciliation is
by column name (take / NULL-fill missing / drop extra / cast type diffs).
```
# explicit branches, each with a tag value
union from erp table CT2010 as "01"
      from erp table CT2020 as "02"
  @[tag = CT2_EMPRESA, canon = CT2010]
  | write sr stream_load CT2_UNIFIED upsert on CT2_EMPRESA, R_E_C_N_O_

# discovered: a query returning (table_name, tag) rows
union erp tables "SELECT name, code FROM meta" @[tag = src]

# json: a JSON array of {table, tag} objects (e.g. from the request body)
union erp json "${source}" @[tag = src]
```
`@[tag = <col>]` names the per-branch tag column; `@[canon = <table>]` picks the
schema authority.

---

## 7. Hints — `@[...]`

Any stage (and the `for` header) may carry a trailing `@[key, key = value, ...]`.
Values are flags (bare key), strings, ints, idents, or sizes (`100mb`). Recognized:

| hint | where | meaning |
|------|-------|---------|
| `where = "<SQL>"` | `read` | raw predicate pushed down to the source (no translation); empty = no WHERE |
| `buffer` | `read`/pipeline | fully drain + close the source **before** opening the sink (avoids slow-consumer query aborts, e.g. Dataverse) |
| `split = <col>` | `write` | parallelize the load by key ranges of `<col>` |
| `mode = parallel \| sequential` | `for` | fan-out execution mode |
| `on_error = continue \| stop` | `for` | per-iteration error policy |
| `tag = <col>`, `canon = <table>` | `union` | tag column / schema authority |
| `tag_field`, `table_field`, `tag_substr` | `union` | discovery-row field mapping |

HTTP-source hints (`auth`, `bearer`, `basic`, `header`, `login_json`, `oauth2`, `page`,
`cursor`, `cursor_field`, `cursor_param`, `page_param`, `page_size`, `size_param`,
`start_page`, `max_pages`, `total_field`, `method`, `body`, `prefetch`, `timeout_ms`,
`retries`, `retry_statuses`, `stop_short`, `progress_ms`) are documented in §9.

---

## 8. `for` — plan-time fan-out

```
for name, cols, where in tables @[mode = parallel, on_error = continue]
  read crm query "SELECT ${cols} from ${name}" @[where = "${where}"]
    | select *, extraction_timestamp = now()
    | write sr stream_load "crm_${lower(name)}" upsert on "${lower(name)}id"
```

- The **source** is either a discovery `read` (`for x in mssql query "..."`, first N
  columns → N loop vars) or a **JSON array** (`for x in job.tables`, each object
  element's fields bound to the loop vars **by name**).
- For each row, the body is run with every `${var}` interpolated into read/write
  targets, `where`, upsert keys, hint values, and string literals.
- A loop variable may be **typed**: `for name, port:int in cfg`. The declared type
  applies wherever the body evaluates the variable as an expression — both a `match`
  guard (`match port >= 1000 => ...`) and a `${ <expr> }` interpolation
  (`"${if(port >= 1000, "big", "small")}"`) then compare it numerically. (Without the
  annotation, comparing a loop value to a non-string literal fails when that row is
  evaluated — declare the type or `cast(...)`. This is a per-row runtime error, not a
  `basalt check` plan-time one, since the values aren't known until discovery.) A
  missing/blank value binds to `null`. A bare `${var}` is always substituted as text
  regardless of type.
- `@[mode]` = `parallel` | `sequential`; `@[on_error]` = `continue` | `stop`.

### `for` body: a pipeline, a `match`, or a `{ }` block

The body is a **statement block**. A bare pipeline is sugar for a one-statement block.
It may also be a `match` statement (or a `{ ... }` block of statements), which branches
**per row** on the loop variables — bound as string values, so a guard like `pk == ""`
selects a branch. Use this when the branches are **different pipelines** (different
sources, extra stages, different sinks):

```
for name, cols, where, kind in tables @[mode = parallel, on_error = continue]
  match
    kind == "view" => {
      read crm query "SELECT ${cols} from ${name}" @[where = "${where}"]
        | write sr stream_load "crm_${lower(name)}"            # views: append-only
    }
    _ => {
      read crm query "SELECT ${cols} from ${name}" @[where = "${where}"]
        | select *, extraction_timestamp = now()
        | write sr stream_load "crm_${lower(name)}" upsert on "${lower(name)}id"
    }
  end
```

If you only need to choose a **value** (like the upsert key), don't reach for `match` —
put the conditional in the interpolation expression: `upsert on "${if(kind == "x", a, b)}"`.

### Interpolation `${...}` — the exact rules

A `${ ... }` placeholder has two body shapes:

- **`${var}`** — substitute the loop variable's value. An unknown variable (no matching
  loop var) is left **verbatim**, so non-loop `${...}` text rides through untouched.
- **`${ <expr> }`** — anything richer than a bare name is parsed as an **expression** and
  evaluated with the loop variables in scope (bound as **string** values), then formatted
  to text. This is where conditionals, coalesce, and case-folding live:
  ```
  upsert on "${if(pk == "", concat(name, "id"), pk)}"   # pk if given, else <name>id
  "crm_${lower(name)}"                                   # case-fold with a function
  key = "${coalesce(pk, "default")}"
  ```
  > The `${var:lower}` / `${var:upper}` modifier syntax is **deprecated** — it still
  > resolves but prints a deprecation warning; use the `lower(...)` / `upper(...)`
  > functions instead.
  All built-in scalar functions (§5) are available. **Interpolation is C#-style**: a
  `${...}` hole inside a `"..."` string is scanned brace-balanced and string-aware, so
  nested string literals inside the hole need **no escaping** — write `"${concat(a, "b")}"`,
  not `\"b\"`. A parse/eval failure is a permanent error (the offending placeholder is
  printed to stderr as `[interp] ${...}: <reason>`).

Two facts that matter for the conditional case:

- **Missing JSON field binds to `""`** (empty string), **never `null`**. So a
  `for name, cols, where, pk in tables` over objects lacking `pk` gives `pk == ""` —
  test with `pk is empty` (covers both null and `""`), or compare `pk == ""`.
- Loop values are strings, so compare against string literals (`pk == ""`), and reach for
  `is empty`, `??`, `concat`, `coalesce`, `if`, `substr`, etc.

For value selection (like choosing an upsert key) this is the simplest tool — no `match`
needed. Use a `match` body (below) when you need to choose a whole **pipeline** per row,
not just a value.

---

## 9. HTTP / REST source

```
connection api = http url = "https://host/api" auth = "bearer" ...
read http "https://host/api/v1/items"
  @[ auth = bearer, page, page_param = "page", page_size = 100,
     total_field = "count", retries = 3, retry_statuses = "429,503",
     timeout_ms = 30000, prefetch = 4 ]
```

- **Auth:** `auth = none | bearer | basic | header | login_json | oauth2`
  (token/credentials supplied via connection config or `secret()`).
- **Pagination:** `page` (page/size), `offset`, or `cursor` (`cursor_field`,
  `cursor_param`). `total_field` enables early stop; `max_pages`/`start_page` bound it.
- **Reliability:** `retries`, `retry_statuses`, `timeout_ms`, `stop_short`,
  `progress_ms`, and `prefetch = N` concurrent workers.
- **POST sources:** `method = post`, `body = "<json>"`.

---

## 10. Write modes (disposition)

```
write sr stream_load orders                 # default
write sr stream_load orders append
write sr stream_load orders overwrite
write sr stream_load orders upsert on id
write sr stream_load orders upsert on a, b                 # composite key
write sr stream_load orders upsert on id partial cols (a,b)  # partial-column update
write sr stream_load orders upsert                         # bare: infer the PK
```

- `upsert on <cols>` names the key columns explicitly. Keys may be quoted strings so
  `${var}` can build them.
- **bare `upsert`** (no `on`) leaves the keys empty and asks the runtime to **infer
  the primary key** from the source table's PK metadata at plan time. This requires a
  `table` read against a SQL source that *exposes* PK metadata. Sources that don't
  (e.g. the Dataverse TDS endpoint) will fail — name the key, or template it.
- An empty/unresolved upsert key is an error (`UpsertKeysUnresolved`), not a silent
  no-op. (So a `${pk}` that rendered empty fails the create — it does not default.)

---

## 11. Running & exit codes

```
basalt run   <script>|-|-c "<inline>" [-p key=value ...] [-j threads] [--port N]
basalt serve <dir> [--port N] [--watch]      # host every @http script, route by path
basalt check <script>|-|-c "<inline>" [-s|--show-plan] [--connect]
```

Exit codes (the control plane reads these):

| code | meaning |
|------|---------|
| `0`  | success |
| `1`  | permanent failure (bad script, data/schema error) |
| `75` | transient failure (`EX_TEMPFAIL`) — network/server-busy; safe to retry |
| `130`| aborted (SIGINT) |

`basalt check` validates and (with `-s`) prints the plan without running; `--connect`
also opens connections.
