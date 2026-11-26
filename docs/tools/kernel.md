# Kernel protocol

`basalt kernel` keeps one session alive for a program to drive, the way `repl`
keeps one for a person. Each script it runs sees every `CREATE CONNECTION`,
`CREATE FUNCTION`, `PARAM` and `LET` an earlier script declared: they are kept as text and replayed ahead of the next script, so a
script behaves exactly as it would in one file below them all, and re-declaring
a name replaces it. A `WITH` belongs to its query and is not kept — two scripts
may reuse a CTE name. A `LET` holds the value it was given in the script that
declares it (even one that only declares), whatever its type, as a JavaScript
`let` holds its value from line to line: `LET t = now()` is one instant for
every later script, and `LET n = (SELECT count(*) ...)` or a list from
`array_agg` is not queried again, however its source changes. Declare it again
to recompute it. The REPL keeps `LET`s the same way. A script whose declarations
do not check with the session's still runs to report why, but none of them is
kept, so it cannot fail the scripts after it.

## Requests

Requests are NDJSON on stdin, one object per line:

```text
{"op":"run","id":"c1","script":"SELECT ...;","params":{"days":7},"format":"arrow"}
{"op":"cancel","id":"c1"}     stop the running script; the session survives
{"op":"complete","id":"c2","script":"SELECT erp.","pos":11}
{"op":"check","id":"c3","script":"SELECT ...;","tables":["enrich"]}
{"op":"catalog","id":"c4"}
{"op":"hover","id":"c5","script":"SELECT tax(1);","pos":8}
{"op":"definition","id":"c6","script":"SELECT tax(1);","pos":8}
{"op":"format","id":"c7","script":"SELECT  a,\n b FROM t ;"}
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

`catalog` lists what the engine answers to, so an editor completes, colours and
documents it with no list of its own; its status carries `catalog: {version,
functions, keywords, types}`, as `basalt catalog --format json` prints it.
Each function is `{name, kind, signature, params, returns, doc, aliases}`:
`kind` is `scalar`, `aggregate`, `window` or `table`; `params` are `{name,
optional, variadic}` in order, one entry per function, its optional parameters
covering its shorter forms; `returns` is the type it gives whatever its
arguments, null when that depends on them (`abs`, `sum`, `lag`); `doc` is the
reference's one sentence; `aliases` are the other names it answers to
(`variance`, `year`, `datediff`). `keywords` are the reserved and contextual
words, and `types` each name a `CAST` or `PARAM` reads as `{name, is, form}`
(`bigint` is `int`, `varchar` takes `(n)`). `WITH` options are read by each
format on its own and are not listed. Nothing connects, and the answer is the
same for the life of the process.

`hover` says what the name at `pos` is, and `definition` where it is
declared, against the session's declarations, the script's own and what its
`@include`s export to it — a library's as `alias.name` — read from the text, so
a script half typed resolves too; `pos` and `utf16` work as for `complete`.
`hover` carries `{start, end, kind, name, signature, type, doc}`, or `null`
when nothing is under the cursor: `start`/`end` bound the name, and `kind` is
`function`, `param`, `let`, `connection`, `table` or `column`. A built-in
function adds `builtin`, its kind in the catalog, with the catalog's signature,
return type and doc. A declared function shows its signature as written, with
`type` `table` for a table function and its `body` for an expression function;
a `PARAM` its `type`, its `default` and the `value` the request's `params` bind
it to; a `LET` the `value` it holds, as JSON, and its `type`, once a script has
given it one; a connection its `TYPE` as `type`, and in `signature` the
`OPTIONS` whose values are literals — never a password, a token, an `env()` or
`secret()` value, or a URL's user and password; a `conn.schema.table` its
`columns`, asked as `complete` asks; a column its `type`, where the file or
table it comes from says it. A declaration's `doc` is the comment above it.
`definition` carries `{file, line, col, end_line, end_col}`, the name's place,
or `null` for a built-in, a column or nothing: `file` is `script`, an
`@include`d file's path — a pinned remote include's copy in the local cache — or
`session`, whose `line` and `col` count in the script that declared it, named by
its `id`. `col` counts bytes, or UTF-16 units with `"utf16": true`. Nothing runs,
and the session is left as it was.

`format` lays a script out as `basalt fmt` does ([Command line](cli.md)),
changing nothing but blanks, and its status carries the text as
`formatted`. The script is checked after the session's declarations: one that
does not parse is refused with `ok: false` and an `error` placed in it as a
`run`'s is, and the layout is refused too, the script left as it was, should
it ever read differently. Nothing runs, and the session is left as it was.

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
run — those its `@include`s export to it too, a library's as `"st.name"`
([Scripts](../language/scripts.md)) — and a key that names no `PARAM` of the script or the session is refused
(`check` lists it); `format` overrides `--format` (default `arrow`) and `max_rows` overrides
`--max-rows` for that script. A `cancel`
acts at once, even mid-script; one naming a different `id` than the running
script's is ignored, and one without an `id` stops whatever runs. SIGINT
cancels the running script too — however many times it is sent, it never ends
the process.

## Replies

Replies are frames on stdout, each a JSON header line. A `data` header is
followed by exactly `len` raw bytes: what the script wrote to stdout — its
results in `format`, in order, possibly over several frames. A `result` frame
follows the last byte of each result, so the data between two `result` frames
is exactly one result in any format — NDJSON rows of two SELECTs never run
together, and an Arrow reader that cannot read streams in turn gets one stream
at a time. Exactly one `status` closes each request, listing the results again,
and nothing for that request follows it:

```text
{"type":"data","id":"c1","len":1184}
<1184 bytes>
{"type":"result","id":"c1","statement":0,"kind":"select","line":2,"col":1,"rows":12,"elapsed_ms":40,"truncated":false}
{"type":"status","id":"c1","ok":true,"cancelled":false,"truncated":false,"elapsed_ms":42,"declared":[{"kind":"param","name":"days"}],"results":[...]}
{"type":"status","id":"c2","ok":false,"cancelled":false,"truncated":false,"elapsed_ms":3,"declared":[],
 "error":{"msg":"unknown field `nope`","file":"script","line":3,"col":8,"transient":false}}
```

## Progress and loads

A statement that runs past 400 ms also sends a `progress` frame a second —
`{"type":"progress","id":…,"target":"…","rows":…,"rows_per_sec":…,"elapsed_ms":…}`
— for a `SELECT` as well as a `LOAD`, since a notebook shows either filling.
Inside a `FOR EACH` it adds `loop_done` and `loop_total`: rows of the
outermost loop finished, and rows in all — the terminal's `[3/12]`.

Each `LOAD` sends a `load` frame as it finishes, written or failed, in the
order they finish — what the CLI's ` + target` and ` x target` lines say:

```json
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
class above.

Results written before a failing statement are still delivered. Logs and
`PRINT` stay on stderr (`--log-level`, `--log-format`); the item lines and
the per-run summary are left out — the `load` frames and the status carry
them.
