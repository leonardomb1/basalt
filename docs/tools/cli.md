# Command line

```text
basalt run      <script>|-|-c "<inline>" [-p key=value ...] [-j threads] [--format table|json|csv|tsv|arrow] [--max-rows N] [--port N] [--host IP]
basalt serve    <dir> [--port N] [--host IP] [--watch]
basalt check    <script>|-|-c "<inline>" [-p key=value ...] [--format json] [--known t1,t2]
basalt complete <script>|-|-c "<inline>" [--pos N] [--connect] [--utf16]
basalt repl
basalt kernel   [--format table|json|csv|tsv|arrow] [-j threads] [--max-rows N]
basalt version
```

`run` and `serve` take the log options `--log-level error|warn|info|debug`,
`--log-format text|json|auto` and `-q`; `kernel` takes the first two.

## Options

Options may come before or after the script path, and `-` (the script on
stdin) may sit anywhere among them: `basalt run --format json job.sql` and
`basalt run job.sql --format json` are the same run.

## Output formats

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
each describes itself. Its schema carries `custom_metadata`: `basalt.statement`
(the result's ordinal in the run, from `0`), `basalt.kind` (`select`, `show`,
`describe` or `explain`), and `basalt.line` / `basalt.col` where the statement
starts. Just before its end-of-stream marker comes a zero-row record batch whose
message metadata holds `basalt.rows`, `basalt.elapsed_ms` and `basalt.truncated`
— plain Arrow, so a reader that ignores metadata sees the same table. pyarrow
reads the streams in turn from one file object (`ipc.open_stream(f)` until `f`
is exhausted; the trailer through `read_next_batch_with_custom_metadata`). A
statement `EXPLAIN` under arrow is a result of its own — one string column,
`plan`, a row per line — rather than text on stderr, and so is a whole-program
`EXPLAIN`.

Logs are stderr-only, plain text, level `warn` by default (`--log-level`,
`--log-format json`, `-q`).

## Capping rows

`--max-rows N` keeps the first `N` rows of each result printed to stdout and
stops the query there: once a result has more rows than it keeps, every source
reports end of input at its next read, so `SELECT * FROM 'huge.parquet'`
returns its first rows after decoding one row group, not the file. A blocking
operator is unaffected — an aggregate, a sort or a join's build side has read
its whole input before the first row reaches stdout, so what is kept is exactly
the first `N` rows of the full answer. A cut result logs `result cut at
--max-rows N` (level `warn`), carries `basalt.truncated = true` in its Arrow
trailer, and in a kernel status. A `LOAD` is never capped.

## Errors and editors

Errors point at what is wrong, not at the statement it sits in: an unknown
column or function is reported at the name itself, on its own line of a
multi-line query. Under `--log-format json` an error is one NDJSON line in the
log's shape, with the range an editor underlines — `end_line`/`end_col` are just
past the offending text, and absent when the error is about a whole stage — and
whether a retry could help (`class`, the exit-`75` distinction below):

```json
{"ts":1790592941244,"level":"error","event":"script_error","msg":"unknown field `nope`",
 "file":"orders.sql","line":2,"col":14,"end_line":2,"end_col":18,"class":"permanent"}
```

`event` is `parse_error`, `script_error` or `aborted`; `file` is the script, or
the `@include`d file the fault is in.

For an editor: `basalt check --format json` prints its diagnostics as a JSON
array on stdout — `[]` when the script checks out, else one object per error in
the shape above without `ts`/`event` — and still exits `1` on an error. `check`
does not stop at the first problem: every statement is checked on its own and
each that fails is listed, in script order. A statement that does not parse is
skipped to its `;` and parsing resumes after it — except inside a `FOR`, `CASE`
or `CREATE FUNCTION` body, whose own `;`s make the statement's end unknowable,
so the problems after a broken block go unreported until it is fixed. `--known
enrich,daily` names tables the script reads but does not declare — a notebook's
other cells — so `FROM enrich` is checked as a table whose columns are unknown
rather than failing as an unknown source. `basalt complete --pos N` prints what
Tab would offer at byte offset `N` (the end, without `--pos`; `--utf16` counts
`N` and the answer in UTF-16 units):
`{"start":S,"end":N,"items":[{"text":…,"kind":…,"detail":…}]}`, the items
replacing `script[S..N]`, with `kind` one of `keyword`, `function`, `param`,
`cte`, `connection`, `table`, `column`, `path`. `S` is where the word under the
cursor begins even when nothing matches. `detail`, present only when there is
one, is a built-in function's signature (`date_add(unit, n, ts)`) or a column's
type — the engine's for a file, the database's `data_type` for a table. A word
comes back once: a column named `name` is not offered again as the keyword
`name`, and a column shadows a function of its name. It completes against the
script's own declarations, so a script half typed still has its names; local
files named in it give their columns; connections are asked for their tables and
columns only under `--connect`, since that is a round trip to each.

## Progress and summaries

While a `LOAD` runs, a terminal gets a live line on stderr — what is moving
where, rows so far, the rate and the clock, with `[3/12]` in front inside a
`FOR EACH`:

```text
⠹ [3/12] erp.dbo.SC5010 → sr.bronze.sc5010  1,204,112 rows  48.3k rows/s  0:24
```

A run that writes closes with one sentence, and one that has several `LOAD`s
(more than one statement, or a `FOR EACH` / `CASE` / `CALL`) reports each as it
finishes — ` +` loaded, ` x` failed, with the reason:

```text
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

The progress line is drawn only when stderr is a TTY, never under `-q` or
`--log-format json`, not for a terminal `SELECT` (whose rows go to the same
screen), and not for the first 400 ms, so short runs stay silent. Under
`--log-format json` the line becomes an event instead, once a second after those
400 ms, terminal or not — what a UI reading the log shows in its place
(`--no-progress` and `-q` still turn it off):

```json
{"ts":…,"level":"info","run_id":…,"event":"progress","target":"erp.dbo.SC5010 → sr.bronze.sc5010","rows":1204112,"rows_per_sec":48300,"elapsed_ms":24930}
```

`loop_done`/`loop_total` join it inside a `FOR EACH`. A log or `PRINT` line
erases it rather than colliding with it, and the run summary replaces it at the
end. Piped or redirected, stderr carries no control characters at all.
`--no-progress` turns it off.

## Printed results

`basalt run` prints a terminal `SELECT` whole — every row and every column, in a
plain left-aligned table — whether stdout is a terminal, a pipe or a file. What
`-c` returns never depends on who is watching.

## Exit codes

| code | meaning |
|------|---------|
| `0`  | success |
| `1`  | permanent failure (bad script, data/schema error) — maps to HTTP 422 |
| `2`  | a malformed command line: an unknown command or option, or a bad option value |
| `75` | transient (`EX_TEMPFAIL`) — safe to retry — maps to HTTP 503 |
| `130`| aborted (SIGINT) |

Transient means the network or the peer failed, not the script: a refused,
reset or timed-out connection, a failed name lookup, an HTTP 429/5xx, and a
database that closed the connection or failed on its socket — during login
(a server turning connections away under a burst of parallel logins) or
mid-query (`ServerClosedConnection`, `ConnectionIoFailed`). Past an HTTP
source's in-place backoff and a SQL sink's one reconnect mid-write, basalt does
not retry: the exit code hands the decision to whatever scheduled the run. A
`FOR EACH` whose failed items are all transient exits `75` too. The same
end-of-file or write failure on a local file stays permanent.
