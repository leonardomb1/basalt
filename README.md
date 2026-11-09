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
Parquet (6 million `lineitem` rows), each engine run as a process with the same
12 threads, best of three, on one laptop (i5-1345U, under WSL); every engine's
answers match DuckDB's. A query's time is its wall time less the engine's startup.
The harness, its queries and the full results with memory are in
[`bench/`](bench/README.md) ([this run](bench/results/2026-11-09-13th-gen-intel-r-core-tm-i5-1345u.md)).

| | basalt 0.13.3 | DuckDB 2.0 | ClickHouse 26.10 | Polars 2.1 | DataFusion 55 |
|---|---|---|---|---|---|
| startup | **2 ms**, 16 MB | 33 ms, 40 MB | 49 ms, 190 MB | 138 ms, 61 MB | 102 ms, 114 MB |
| `count(DISTINCT)` | **51 ms** | 144 ms | 175 ms | 129 ms | 120 ms |
| `LIKE` over long strings | **75 ms** | 142 ms | 120 ms | 97 ms | 109 ms |
| TPC-H Q6 (filter) | 43 ms | 41 ms | 72 ms | **29 ms** | 48 ms |
| TPC-H Q1 (aggregate) | 140 ms | **80 ms** | 158 ms | 148 ms | 163 ms |
| TPC-H Q3 (join, top-N) | 185 ms | 76 ms | 199 ms | **61 ms** | 94 ms |
| TPC-H Q18 (IN, HAVING) | 226 ms | **108 ms** | 180 ms | 125 ms | 284 ms |
| exact median | 148 ms | **98 ms** | 156 ms | 316 ms | 174 ms |
| `lineitem` Parquet → CSV | **0.72 s** | 1.37 s | 0.79 s | 0.76 s | 1.61 s |
| `lineitem` Parquet → Parquet | 1.25 s | **0.93 s** | 1.58 s | 1.05 s | 1.74 s |
| `lineitem` CSV → Parquet | 2.08 s | 2.23 s | 2.43 s | **1.47 s** | 2.26 s |
| `lineitem` NDJSON → Parquet | 4.99 s | 2.89 s | 3.84 s | **2.76 s** | 4.04 s |
| `lineitem` CSV.gz → Parquet | 7.83 s | 3.60 s | **2.37 s** | 2.42 s | 4.34 s |

basalt starts in milliseconds and holds memory in line with DuckDB's; it is the
fastest at distinct counts, string matching and writing CSV, and on joins and
aggregates about one and a half to three times slower than the fastest engine.
Reading gzipped CSV and NDJSON are its slowest paths today.

## basalt and DuckDB

Both read files with SQL from a single binary, so they are often compared. They
are built for different jobs:

| | basalt | DuckDB |
|---|---|---|
| What it is | a script runner that moves data between systems | an embedded analytical database |
| Storage of its own | none: it reads and writes other systems | a database file, with transactions and indexes |
| Used from | the command line, a REPL, a notebook kernel | a library in Python, R, Java, Node, Wasm and more, and a CLI |
| CSV, Parquet, JSON, Excel | read and write | read and write (Excel through an extension) |
| XML, Avro | read and write | community extension for XML; Avro read through an extension |
| Delta Lake | read, time travel, change feed; append, replace, upsert | read, time travel, create and append (extension) |
| Iceberg | no | read and write through a catalog (extension) |
| S3, Azure | read and write | read and write (extensions) |
| SFTP, FTP, SMB shares | read; write but FTP | no |
| PostgreSQL, MySQL | read and write, filters run by the database, split reads in parallel, UPSERT | read and write by attaching (extensions) |
| SQL Server, StarRocks, Doris | read and write | SQL Server through an experimental community extension |
| REST APIs | paging, retries, bearer, basic, OAuth2 and login flows | files over HTTP(S) |
| Change data capture | SQL Server CDC, applied to any table in batches | no |
| A script checked before it runs | every statement typed and planned first; `basalt check` offline | each statement bound as it runs |
| Scripting | `PARAM`, `LET`, `FOR EACH`, `CASE`, functions and pinned libraries | SQL macros |
| Speed | starts in 2 ms; faster at distinct counts, `LIKE` and writing CSV, about 1.5–3× slower on joins and aggregates (above) | starts in 33 ms |

Use DuckDB to keep and query data in one place, from a program or a notebook;
use basalt to move data between the systems that hold it, on a schedule.

## When to use something else

- **Streaming replication.** basalt reads change feeds in batches: each run
  takes what is new since the target's last position and stops. It does not hold
  a connection open following a log; schedule it with cron, Airflow or the like.
- **One group far larger than memory.** A sort, a join's build side, `DISTINCT`
  and a `GROUP BY` spill to disk past `--op-memory`, a parallel `GROUP BY` staying
  parallel; but an ungrouped `count(DISTINCT)`, and the values `median`, the
  percentiles, `string_agg` and `array_agg` hold for one group, stay in memory.
- **Interactive analytics where milliseconds matter.** DuckDB is faster on
  analytical queries (above); basalt's edge is everything around the query:
  sources, sinks, scripts and checks.

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
