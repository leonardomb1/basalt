# basalt

A lightweight SQL data movement engine in a single static binary.

A script describes a columnar pipeline — read from a source, transform with a
query, write to a sink. It is parsed, type-checked and planned once, then
executed as a streaming pull pipeline.

```sql
-- move yesterday's paid orders from SQL Server into the lake
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'totvs');

LOAD INTO 'az://lakeacct/bronze/orders.parquet' AS
SELECT id, customer, amount, placed_at
FROM erp.orders
WHERE status = 'paid';
```

Or the same shape against a published CSV that is neither comma-separated nor
UTF-8, which is most of them:

```sql
LOAD INTO 's3://lake/bronze/funds.parquet' AS
SELECT CNPJ_FUNDO, DENOM_SOCIAL, SIT
FROM 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
  WITH (delimiter = ';', encoding = 'latin1');
```

```console
basalt run orders.sql
```

## Install

Prebuilt binary (Linux x86-64, ~3.7 MB, statically linked — runs anywhere):

```console
curl -fsSL -o basalt https://github.com/leonardomb1/basalt/releases/latest/download/basalt-x86_64-linux
chmod +x basalt && ./basalt help
```

From source, with Zig 0.15.2:

```console
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl -Dstrip=true
./zig-out/bin/basalt help
```

`-Dstrip` drops debug info for a smaller binary.

## What it talks to

| | |
|---|---|
| **Files** | CSV and Parquet, local or over HTTP; Arrow IPC (`.arrow`, `.feather`, `.ipc`, `.arrows`), local, read and written; Excel (`.xlsx`) read, a sheet or a range of it — the extension picks the format, and an extension basalt does not read is refused rather than guessed at. `WITH (delimiter = ';', encoding = 'latin1')` for the CSV most of the world publishes |
| **Compressed & archived** | `orders.csv.gz`, `orders.csv.zst`, and `archive.zip :: inner.csv`. Members stream rather than expanding to memory or a temp file |
| **Object storage** | `az://account/container/path` (Azure Blob / ADLS Gen2) or `s3://bucket/key` (S3, MinIO). A trailing `/` reads a folder — local, SFTP or object storage — of Parquet files or CSVs as one table, subfolders included |
| **SFTP** | `sftp://conn/path`, read and written — host keys checked against `known_hosts` or a pinned fingerprint, password or Ed25519 key logins, writes through a `.part` renamed into place |
| **Databases** | PostgreSQL, MySQL, SQL Server, StarRocks, Apache Doris |
| **HTTP** | paginated REST sources; serve a pipeline as an endpoint |
| **Buffer** | a durable WAL buffer, replayed by a later run |

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched. That
holds over the network too: a remote `.parquet` is read by HTTP range request,
so projecting two columns of forty transfers two chunks, not the object. A
server that ignores `Range` is handled by falling back to a single whole-object
fetch. Column types follow the file's `LogicalType`, so the naive and nanosecond
timestamps polars, DuckDB, Spark and pyarrow write read as timestamps. Nested
columns — lists, lists of structs, maps, at any depth — read as JSON text that
`UNNEST(JSON_EACH(col))` and `json_get` take apart, or that `json_filter(col, x ->
…)`, `json_transform`, `json_any` and `json_all` work on in place; no column is
left out.

Arrow IPC is the fast way to hand a dataframe over: the file is memory-mapped
and copied out, with no encode or decode step. polars' `write_ipc` and pyarrow's
`write_feather` both read as written, compressed or not, categoricals and
nested columns included.

## Running things

```console
$ basalt run pipeline.sql -p days=7       # bind a PARAM
$ basalt run --format json -c "<query>"   # NDJSON rows on stdout, for scripts
$ basalt run --format arrow -c "<query>"  # Arrow IPC stream per result, for pyarrow/Polars/Arrow JS
$ basalt run --max-rows 500 -c "<query>"  # first 500 rows, and stop reading there
$ basalt check pipeline.sql               # validate without running
$ basalt run -c "EXPLAIN <query>"         # print the plan
$ basalt run -c "EXPLAIN ANALYZE <query>" # run it, print the plan with actuals
$ basalt repl                             # interactive: runs on `;`, keeps
                                          # connections/functions across entries
$ basalt kernel --format arrow            # a session for a notebook: NDJSON requests
                                          # on stdin, framed results + status on stdout
$ basalt serve ./endpoints --watch        # host every endpoint script in a dir
```

A terminal `SELECT ...;` prints a table — or one JSON object per row with
`--format json`. `LOAD INTO <target> AS <query>;` writes. A script that
declares `CREATE ENDPOINT` runs as HTTP; otherwise it runs once and exits.
Options go before or after the script path, and `-` reads the script from stdin.

Logging is quiet by default: plain-text errors and warnings on stderr, plus a
one-line summary when a run loads a sink. `--log-level debug` shows plan
detail; `--log-format json` switches stderr to NDJSON for collectors — errors
with the exact range of the offending name and whether a retry could help,
a `progress` event a second for long statements, and a `run_complete` summary
for every run, with what the sources were spared (row groups skipped, columns
not decoded, filters pushed).

## Notebooks and editors

`basalt kernel` keeps one session alive for a frontend. Each script sees the
connections, functions, params and `LET`s earlier scripts declared; a request
can bind params, pick a format and cap rows for that script alone, and a
`cancel` (or SIGINT) stops the running script without ending the session.
Results come back framed — a `result` frame after each one, `progress` frames
while a statement runs, and one JSON `status` per script — so several results
in one cell never run together.

```console
$ basalt kernel --format arrow --max-rows 5000
{"op":"run","id":"c1","script":"CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'db', database = 'erp'); PARAM since DATE;"}
{"op":"run","id":"c2","script":"SELECT * FROM erp.orders WHERE day >= $since;","params":{"since":"2026-01-01"}}
{"op":"complete","id":"c3","script":"SELECT * FROM erp.","pos":18}
```

For an editor without a session, `basalt check --format json` prints
diagnostics as a JSON array with their ranges, and `basalt complete --pos N`
prints what Tab would offer at byte `N`.

The summary's rate is rows **processed** per second — the volume that moved
through the pipeline, which for a straight move is also the rows written. (Before
0.5.8 it divided the *written* count by the clock, so an aggregate folding six
million rows into four reported `11 rows/s`.)

## Design

- Errors surface at plan time: an unknown column, an incomparable type or a
  missing credential fails `check`, before a row is read.
- Execution streams: a map pipeline's memory is bounded by batch size, not file
  size, however large the input. A stage that has to see the whole input first is
  bounded by its *result* instead — a `GROUP BY` holds one entry per group, a
  join holds its build side, failing fast past 4 GiB (`WITH (max_build = '16GB')`
  to raise it), and a window function holds the input it ranks. Aggregating a high-cardinality key is the case to watch: grouping
  2M distinct ids out of a 98 MB CSV peaks around 1.1 GB, and there is no spill to
  disk.
- `WHERE` against a database table runs in the database. The plan shows what
  was pushed down.
- Parquet pipelines run in parallel over row-group morsels, local CSV pipelines
  over byte-range chunks, a splittable database read over key ranges; `-j`
  controls it and `EXPLAIN` names which one a query gets.
- A rerun reproduces its output. Parallel aggregates total their slices in a
  fixed order rather than in completion order, so the same command over the same
  data writes the same bytes at the same `-j`.
- One process, one allocation strategy, no garbage collector.

## Credentials

Secrets never appear in a script. A connection named `erp` resolves `ERP_USER`
and `ERP_PASS` from the environment; explicit `user = ...` / `password = ...`
options override that. Azure Blob uses `AZURE_STORAGE_KEY`, and
`AZURE_BLOB_ENDPOINT` points it at an emulator. S3 uses `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` (plus `AWS_SESSION_TOKEN` and `AWS_REGION` when they
apply), with `AWS_ENDPOINT_URL` for MinIO and the like.

## Documentation

- [`language.md`](language.md) — the SQL dialect: sources, sinks, joins,
  unions, `FOR EACH`, parameters, endpoints
- [`examples/`](examples) — runnable scripts, one per feature

## Tests

```console
zig build test                    # unit tests, no services needed
./it/run.sh                       # integration suite (needs docker)
./it/run.sh azure parquet         # just those suites
./it/run.sh stdout kernel arrow   # the CLI's own contracts: no containers
```

The integration suite starts only the containers the selected suites need;
`kernel` needs `python3`, `arrow` needs `uv` (it checks results with pyarrow).
`KEEP=1` leaves the stack up afterwards.

## License

MIT — see [LICENSE](LICENSE).
