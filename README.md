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

```sql
-- active funds from a Latin-1, semicolon-separated CSV, into Parquet
LOAD INTO 'funds.parquet' AS
SELECT cnpj, nome AS name,
       CAST(replace(patrimonio, ',', '.') AS DECIMAL(18,2)) AS net_assets
FROM 'funds.csv' WITH (delimiter = ';', encoding = 'latin1')
WHERE situacao = 'EM FUNCIONAMENTO NORMAL';
```

```console
$ basalt check funds.sql
ok: funds.sql checks out
$ basalt run funds.sql
Read 3 rows, loaded 2 into funds.parquet in 4ms (750 rows/s, 12 lanes)
$ basalt run -c "SELECT * FROM 'funds.parquet';"
cnpj                name                         net_assets
------------------  ---------------------------  ----------
11.222.333/0001-81  Fundo Ação Brasil            1500000.50
33.444.555/0001-02  Fundo Imobiliário São Paulo  820000.00
(2 rows)
```

A script is parsed, type-checked and planned before a row is read, so a typo
fails in `check` rather than an hour into a load:

```console
$ basalt check -c "SELECT cnpj, situaco FROM 'funds.csv' WITH (delimiter = ';', encoding = 'latin1');"
<command>:1:14: error: unknown field `situaco`
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
docker run --rm -v "$PWD:/scripts:ro" -w /scripts ghcr.io/leonardomb1/basalt check funds.sql
```

From source, with [Zig 0.15.2](https://ziglang.org/download/):

```console
zig build -Doptimize=ReleaseFast
./zig-out/bin/basalt help
```

## What it reads and writes

| | read | write |
|---|---|---|
| **Files** — CSV (any delimiter, Latin-1 and CP1252), Parquet, Arrow IPC, Excel | ✓ | ✓ (not Excel) |
| **Compressed and archived** — `.gz`, `.zst`, a member of a `.zip` | ✓ | `.gz` |
| **Object storage** — S3 and S3-compatible, Azure Blob / ADLS Gen2 | ✓ | ✓ |
| **File servers** — SFTP, Windows and Samba shares (SMB 2.1–3.1.1, NTLMv2 or Kerberos) | ✓ | ✓ |
| **Databases** — PostgreSQL, MySQL, SQL Server, StarRocks, Apache Doris | ✓ | ✓ |
| **HTTP APIs** — paginated REST, with bearer, basic, OAuth2 and login flows | ✓ | |

A folder — local, remote or a storage prefix — reads as one table. Parquet is
read by column and row group, over the network too, so a narrow query fetches
only what it needs. A connection named `erp` takes its login from `ERP_USER` and
`ERP_PASS`, so scripts carry no secrets.
Each connector's page in the [documentation](https://leonardomb1.github.io/basalt/)
has the details.

## Commands

```console
$ basalt run pipeline.sql -p since=2026-01-01   # run once; -p binds a PARAM
$ basalt check pipeline.sql                     # validate without connecting
$ basalt serve ./endpoints                      # each script as an HTTP endpoint
$ basalt repl                                   # an interactive session
$ basalt kernel                                 # a session a notebook or editor drives
```

`run` takes `--format json|csv|arrow` for programs reading its output, and
`-j N` for parallel lanes. Exit code `75` means a transient failure worth
retrying, `1` a permanent one. See [Command line](https://leonardomb1.github.io/basalt/tools/cli.html).

## When not to use it

- **Continuous replication.** basalt runs a script to completion (or once per
  HTTP request); it does not follow a change log. Schedule it with cron, Airflow
  or the like.
- **Inputs larger than memory in a blocking step.** Pipelines stream, but a
  `GROUP BY` holds its groups, a join its build side and a sort its input, with
  no spill to disk.
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
