# basalt

A SQL-driven data pipeline engine. A script describes a columnar pipeline —
read from a source, transform with a query, write to a sink — which is parsed,
type-checked and planned once, then executed as a streaming pull pipeline.

Single static binary, no runtime dependencies.

```sql
-- move yesterday's paid orders from SQL Server into the lake
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'totvs');

LOAD INTO 'az://lakeacct/bronze/orders.parquet' AS
SELECT id, customer, amount, placed_at
FROM erp.orders
WHERE status = 'paid';
```

```console
$ basalt run orders.sql
```

## Install

```console
$ zig build -Doptimize=ReleaseFast
$ ./zig-out/bin/basalt help
```

Needs Zig 0.15.2. `-Dstrip` drops debug info for a smaller binary.

## What it talks to

| | |
|---|---|
| **Files** | CSV and Parquet, local or over HTTP — the extension picks the format |
| **Object storage** | `az://account/container/path` (Azure Blob / ADLS Gen2). A trailing `/` reads every blob under that prefix as one table |
| **Databases** | PostgreSQL, MySQL, SQL Server, StarRocks |
| **HTTP** | REST sources and sinks; serve a pipeline as an endpoint |
| **Buffer** | a durable WAL buffer, replayed by a later run |

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched.

## Running things

```console
$ basalt run pipeline.sql -p dias=7      # bind a PARAM
$ basalt check pipeline.sql              # validate without running
$ basalt run -c "EXPLAIN <query>"        # print the plan
$ basalt run -c "EXPLAIN ANALYZE <query>" # run it, print the plan with actuals
$ basalt repl                            # interactive
$ basalt serve ./endpoints --watch       # host every endpoint script in a dir
```

A terminal `SELECT ...;` prints a table. `LOAD INTO <target> AS <query>;` writes.
A script that declares `CREATE ENDPOINT` runs as HTTP; otherwise it runs once and
exits.

## Credentials

Secrets never appear in a script. A connection named `erp` resolves `ERP_USER`
and `ERP_PASS` from the environment; explicit `user = ...` / `password = ...`
options override that. Azure Blob uses `AZURE_STORAGE_KEY`, and
`AZURE_BLOB_ENDPOINT` points it at an emulator.

## Documentation

- [`language.md`](language.md) — the SQL dialect: sources, sinks, joins, unions,
  `FOR EACH`, parameters, endpoints
- [`examples/`](examples) — runnable scripts, one per feature

## Tests

```console
$ zig build test                    # unit tests, no services needed
$ ./it/run.sh                       # integration suite (needs docker)
$ ./it/run.sh azure parquet         # just those suites
```

The integration suite starts only the containers the selected suites need.
`KEEP=1` leaves the stack up afterwards.

## License

MIT — see [LICENSE](LICENSE).
