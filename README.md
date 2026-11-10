# basalt

**Move data with SQL scripts.** A data movement engine with a parallel,
vectorized query engine, in a single static binary: read from files, object
storage, file servers, databases and APIs, transform with real SQL (joins,
window functions, subqueries, aggregates), and write the result to a file, a
bucket, a share or a table.

[![CI](https://github.com/leonardomb1/basalt/actions/workflows/ci.yml/badge.svg)](https://github.com/leonardomb1/basalt/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/leonardomb1/basalt/graph/badge.svg)](https://codecov.io/gh/leonardomb1/basalt)
[![Release](https://img.shields.io/github/v/release/leonardomb1/basalt)](https://github.com/leonardomb1/basalt/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

[Documentation](https://leonardomb1.github.io/basalt/) ·
[Getting started](https://leonardomb1.github.io/basalt/getting-started.html) ·
[Language](https://leonardomb1.github.io/basalt/language/scripts.html) ·
[Functions](https://leonardomb1.github.io/basalt/reference/functions.html) ·
[Connectors](https://leonardomb1.github.io/basalt/connectors/databases.html)

The first five examples run as written, against public data; paste one into
`basalt run -c "..."` or a `.sql` file.

A CSV on the web, summarized into a local Parquet file:

```sql
LOAD INTO 'survival.parquet' AS
SELECT Pclass AS class, Sex AS sex, count(*) AS passengers,
       round(avg(Survived) * 100, 1) AS survived_pct
FROM 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
GROUP BY Pclass, Sex;
```

```console
$ basalt run titanic.sql
Read 891 rows, loaded 6 into survival.parquet in 320ms (2,784 rows/s)
$ basalt run -c "SELECT * FROM 'survival.parquet' ORDER BY class, sex;"
class  sex     passengers  survived_pct
-----  ------  ----------  ------------
1      female  94          96.8
1      male    122         36.9
2      female  76          92.1
2      male    108         15.7
3      female  144         50
3      male    347         13.5
(6 rows)
```

A JSON feed, its rows in a nested array, its fields in nested objects:

```sql
SELECT json_get(properties, 'place') AS place,
       CAST(json_get(properties, 'mag') AS DOUBLE) AS magnitude
FROM 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
  WITH (format = 'json', items = 'features')
ORDER BY magnitude DESC LIMIT 5;
```

A REST API, through a connection that names its base URL (retries, paging and
auth are options of the connection and the read):

```sql
CREATE CONNECTION gh TYPE http OPTIONS (base_url = 'https://api.github.com');

SELECT tag_name, CAST(published_at AS DATE) AS published, json_get(author, 'login') AS author
FROM gh.GET('/repos/duckdb/duckdb/releases', per_page = 10)
ORDER BY published DESC;
```

A 50 MB Parquet file over HTTPS, read by ranged requests: only the two columns
the query uses are fetched.

```sql
SELECT payment_type, count(*) AS trips, round(avg(tip_amount), 2) AS avg_tip
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY payment_type ORDER BY trips DESC;
```

Sources federate: each side of a join can come from anywhere. Taxi trips from
that Parquet file, joined to the zone names in a CSV at another URL:

```sql
SELECT z.Borough AS borough, count(*) AS trips, round(avg(t.total_amount), 2) AS avg_fare
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' t
JOIN 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv' z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough ORDER BY trips DESC;
```

```console
borough        trips    avg_fare
-------------  -------  --------
Manhattan      2646948  22.38
Queens         273128   68.25
Brooklyn       25258    32.81
Unknown        10360    29.36
Bronx          6905     35.7
N/A            1658     93.43
EWR            295      104.2
Staten Island  72       55.09
(8 rows)
```

A script is parsed, type-checked and planned before a row is read, so a typo
fails at once, not an hour into a load; `basalt check` finds it without running
anything wherever the columns are known offline, as for a local file:

```console
$ basalt run -c "SELECT Pclass, Survivd FROM 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv';"
<command>:1:16: error: unknown field `Survivd`
```

The same shape joins two databases on the way into a lake, each `WHERE` run by
the database it filters:

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'erp');
CREATE CONNECTION sales TYPE postgres OPTIONS (host = 'pg.internal', database = 'sales');

LOAD INTO 's3://lake/silver/paid_orders.parquet' AS
SELECT o.id, o.amount, o.placed_at, c.segment, c.region
FROM erp.dbo.orders o
JOIN sales.public.customers c ON c.id = o.customer_id
WHERE o.status = 'paid';
```

## Install

A prebuilt binary for Linux x86-64, statically linked:

```console
curl -fsSL -o basalt https://github.com/leonardomb1/basalt/releases/latest/download/basalt-x86_64-linux
chmod +x basalt
./basalt help
```

A container image, with scripts mounted rather than baked in:

```console
docker run --rm -v "$PWD:/scripts:ro" -w /scripts ghcr.io/leonardomb1/basalt check pipeline.sql
```

From source, with [Zig 0.15.2](https://ziglang.org/download/):

```console
zig build -Doptimize=ReleaseFast
./zig-out/bin/basalt help
```

## What it reads and writes

| | read | write |
|---|---|---|
| **Files** — CSV (any delimiter, Latin-1 and CP1252), Parquet, Arrow IPC, Excel, JSON and NDJSON, XML, Avro | ✓ | ✓ |
| **Record files** — fixed-width and bank or EDI lines a query composes | | ✓ |
| **Compressed and archived** — `.gz`, `.zst`, a member of a `.zip` | ✓ | `.gz` (CSV, JSON, XML) |
| **Delta Lake** — time travel and the change data feed; append, replace and upsert (copy-on-write, deletion vectors) | ✓ | ✓ |
| **Object storage** — S3 and S3-compatible, Azure Blob / ADLS Gen2 | ✓ | ✓ |
| **File servers** — SFTP, Windows and Samba shares (SMB 2.1–3.1.1, NTLMv2 or Kerberos); FTP | ✓ | ✓ (not FTP) |
| **Databases** — PostgreSQL, MySQL, SQL Server, StarRocks, Apache Doris | ✓ | ✓ |
| **Change feeds** — SQL Server CDC and a Delta table's feed, applied to any table by `UPSERT ... USING changes` | ✓ | ✓ |
| **HTTP APIs** — paginated REST, with bearer, basic, OAuth2 and login flows | ✓ | |
| **Standard input and output** — `FROM STDIN` reads CSV, JSON, NDJSON or XML; a final `SELECT` prints a table, CSV, TSV, JSON or Arrow | ✓ | ✓ |

A folder — local, remote or a storage prefix — reads as one table. Parquet is
read by column and row group, over the network too, so a narrow query fetches
only what it needs. A connection named `erp` takes its login from `ERP_USER` and
`ERP_PASS`, so scripts carry no secrets.
Each connector's page in the [documentation](https://leonardomb1.github.io/basalt/)
has the details.

## Reuse

`CREATE FUNCTION` names an expression, a block of statements or a parameterized
query (a table function, read with `FROM name(...)`). A file of them is a
library: `@include 'lib/finance.sql' AS fin;` makes them `fin.name(...)`, and a
library can live in a repository, pinned to a version and a hash:

```sql ignore
@include 'github.com/acme/sql-kit/finance.sql@v1.2.0' AS fin;

SELECT fin.business_days(opened, closed) AS sla FROM 'tickets.csv';
```

`basalt check --pin` fetches a remote library and records its SHA-256 in
`basalt.sum` beside the script; a run refuses a library that is not pinned or no
longer matches, and runs pinned ones from a local cache, with no network. See
[Scripts](https://leonardomb1.github.io/basalt/language/scripts.html).

## Commands

```console
$ basalt run pipeline.sql -p since=2026-01-01   # run once; -p binds a PARAM
$ basalt check pipeline.sql                     # validate without connecting
$ basalt check --pin pipeline.sql               # ...and pin its remote @includes
$ basalt repl                                   # an interactive session
$ basalt kernel                                 # a session a notebook or editor drives
```

`run` takes `--format table|json|csv|tsv|arrow` for what it prints, and `-j N`
for parallel lanes. Exit code `75` means a transient failure worth retrying,
`1` a permanent one. See [Command line](https://leonardomb1.github.io/basalt/tools/cli.html).

## Performance

Against DuckDB, ClickHouse Local, Polars and DataFusion on TPC-H scale factor 1 as
Parquet (6 million `lineitem` rows), on a bare-metal server (AMD Ryzen 7 PRO 8700GE,
8 cores), each engine run as a process with 8 threads, best of three; every engine's
answers match DuckDB's. A query's time is its wall time less the engine's startup.
The harness, its queries and the full results with memory are in
[`bench/`](bench/README.md) ([this run](bench/results/2026-11-10-amd-ryzen-7-pro-8700ge-w-radeon-780m-gra.md)).

| | basalt 0.13.3 | DuckDB 2.0 | ClickHouse 26.10 | Polars 2.1 | DataFusion 55 |
|---|---|---|---|---|---|
| startup | **1 ms**, 15 MB | 27 ms, 50 MB | 45 ms, 232 MB | 105 ms, 135 MB | 81 ms, 116 MB |
| `count(DISTINCT)` | **22 ms** | 68 ms | 83 ms | 60 ms | 67 ms |
| `LIKE` over long strings | **49 ms** | 79 ms | 99 ms | 64 ms | 75 ms |
| TPC-H Q6 (filter) | 23 ms | 25 ms | 68 ms | **19 ms** | 28 ms |
| TPC-H Q1 (aggregate) | 61 ms | **41 ms** | 100 ms | 70 ms | 69 ms |
| TPC-H Q3 (join, top-N) | 102 ms | 41 ms | 117 ms | **30 ms** | 47 ms |
| TPC-H Q18 (IN, HAVING) | 105 ms | **58 ms** | 132 ms | 66 ms | 147 ms |
| exact median | 95 ms | **59 ms** | 102 ms | 165 ms | 92 ms |
| `lineitem` Parquet → CSV | **0.42 s** | 0.66 s | 0.49 s | 0.49 s | 0.72 s |
| `lineitem` Parquet → Parquet | 0.69 s | **0.45 s** | 0.78 s | 0.47 s | 1.09 s |
| `lineitem` CSV → Parquet | 1.06 s | 0.88 s | 1.13 s | **0.77 s** | 1.15 s |
| `lineitem` NDJSON → Parquet | 3.08 s | **1.11 s** | 1.90 s | 1.36 s | 1.67 s |
| `lineitem` CSV.gz → Parquet | 5.90 s | 2.01 s | 1.30 s | **1.25 s** | 3.25 s |

basalt starts in a millisecond and holds the least memory on most queries; it is the
fastest at distinct counts, string matching and writing CSV, and on joins and
aggregates one and a half to three times slower than the fastest engine. Reading
gzipped CSV and NDJSON are its slowest paths today.

## What sets it apart

DuckDB, ClickHouse, Polars and DataFusion are query engines: fast at answering a
question about data they can read. basalt is built around the rest of a pipeline,
getting data out of the systems that hold it and into the ones that need it:

- **One script across systems.** A join can take a SQL Server table, a Postgres
  table and a CSV on a Windows share, and write the result to a lake or another
  database. Each `WHERE` runs in the database it filters, and a large SQL read is
  split into key ranges read in parallel.
- **The systems pipelines actually touch.** SFTP, FTP and SMB shares (NTLMv2 or
  Kerberos); SQL Server, StarRocks and Doris as well as Postgres and MySQL; REST
  APIs with paging, retries and OAuth2; Excel and XML written as well as read;
  fixed-width record files; Latin-1 and CP1252 text; a file inside a `.zip`.
- **Writes the way a target wants them.** `UPSERT` into every database it writes,
  StarRocks and Doris through Stream Load; a `REPLACE` into PostgreSQL, MySQL or
  SQL Server loads a staging table and swaps the rows in one transaction, so readers
  never see an empty table and grants, views and indexes stay.
- **A lake it can maintain.** Delta tables read at any version and written by
  append, replace or upsert (copy-on-write, deletion vectors, row tracking, the
  change data feed), compacted and vacuumed.
- **Change data capture without a server.** `UPSERT ... USING changes` applies a
  change feed, a SQL Server table's CDC or a Delta table's, to any table it writes,
  each run taking what is new since the target's last position.
- **Checked before it runs.** A whole script is parsed, typed and planned before a
  row is read; `basalt check` does it without connecting. Exit code `75` tells a
  scheduler a failure is worth retrying.
- **Scripts, not just queries.** `PARAM`, `LET`, `FOR EACH` (a load per row, to a
  target named per row), `CASE`, functions and table functions, libraries pinned
  by hash.
- **Easy to run anywhere.** One static 7.6 MB binary, no runtime, starting in a millisecond;
  logins from environment variables; `EXPLAIN ANALYZE` shows every request each
  source was sent.

## Development

```console
zig build test          # unit, end-to-end and documentation tests
zig build coverage      # line coverage of src/ (needs kcov)
zig fmt --check src tests tools build.zig
```

`tests/integration/run.sh` runs the connectors against real services in Docker.
The documentation is an [mdBook](https://rust-lang.github.io/mdBook/) in
[`docs/`](docs/); `zig build test-docs` checks every SQL example in it and in
this README.

## License

MIT — see [LICENSE](LICENSE).
