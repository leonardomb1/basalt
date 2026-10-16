# basalt

**Move data with SQL scripts.** A lightweight data movement engine in a single
static binary: read from files, object storage, file servers, databases and
APIs, transform with a query, and write the result to a file, a bucket, a share
or a table.

[![CI](https://github.com/leonardomb1/basalt/actions/workflows/ci.yml/badge.svg)](https://github.com/leonardomb1/basalt/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/leonardomb1/basalt/graph/badge.svg)](https://codecov.io/gh/leonardomb1/basalt)
[![Release](https://img.shields.io/github/v/release/leonardomb1/basalt)](https://github.com/leonardomb1/basalt/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

[Documentation](https://leonardomb1.github.io/basalt/) ·
[Getting started](https://leonardomb1.github.io/basalt/getting-started.html) ·
[Language](https://leonardomb1.github.io/basalt/language/scripts.html) ·
[Functions](https://leonardomb1.github.io/basalt/reference/functions.html) ·
[Connectors](https://leonardomb1.github.io/basalt/connectors/databases.html)

The first four examples run as written, against public data; paste one into
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

A script is parsed, type-checked and planned before a row is read, so a typo
fails at once, not an hour into a load; `basalt check` finds it without running
anything wherever the columns are known offline, as for a local file:

```console
$ basalt run -c "SELECT Pclass, Survivd FROM 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv';"
<command>:1:16: error: unknown field `Survivd`
```

The same shape moves a database table into a lake, with the `WHERE` run by the
database:

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'erp');

LOAD INTO 's3://lake/bronze/orders.parquet' AS
SELECT id, customer, amount, placed_at
FROM erp.dbo.orders
WHERE status = 'paid';
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
| **Files** — CSV (any delimiter, Latin-1 and CP1252), Parquet, Arrow IPC, Excel, JSON and NDJSON, XML | ✓ | ✓ |
| **Compressed and archived** — `.gz`, `.zst`, a member of a `.zip` | ✓ | `.gz` (CSV, JSON, XML) |
| **Object storage** — S3 and S3-compatible, Azure Blob / ADLS Gen2 | ✓ | ✓ |
| **File servers** — SFTP, Windows and Samba shares (SMB 2.1–3.1.1, NTLMv2 or Kerberos) | ✓ | ✓ |
| **Databases** — PostgreSQL, MySQL, SQL Server, StarRocks, Apache Doris | ✓ | ✓ |
| **HTTP APIs** — paginated REST, with bearer, basic, OAuth2 and login flows | ✓ | |
| **Standard input** — `FROM STDIN`, as CSV, JSON, NDJSON or XML | ✓ | |

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

## When not to use it

- **Continuous replication.** basalt runs a script to completion; it does not
  follow a change log. Schedule it with cron, Airflow or the like.
- **Inputs far larger than memory in a blocking step.** Pipelines stream, and a
  sort, a join's build side, `DISTINCT` and a one-lane `GROUP BY` spill to disk
  past `--op-memory`; but the per-lane tables of a parallel `GROUP BY` and
  `count(DISTINCT)` stay in memory.
- **Ad-hoc analytics over large local data.** A columnar database such as
  DuckDB is the better tool for that.

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
