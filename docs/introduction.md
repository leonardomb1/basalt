# Introduction

basalt is a SQL data movement engine in a single static binary. A script
describes a **columnar data pipeline**: read from a source, transform with a
query, write to a sink. A script is plan-time static — parsed, type-checked,
and planned once, then executed as a streaming pull pipeline.

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'totvs');

LOAD INTO 'az://lakeacct/bronze/orders.parquet' AS
SELECT id, customer, amount, placed_at
FROM erp.orders
WHERE status = 'paid';
```

The sources and sinks are files (CSV, Parquet, Arrow IPC, Excel), object
storage, SFTP servers, Windows shares, SQL databases and HTTP APIs; the same
script runs once from the command line, or as an HTTP endpoint under
`basalt serve`.

This book is the reference for the dialect, derived from the parser
(`src/lang/sql_parser.zig`); it describes what the engine actually accepts.

- [Getting started](getting-started.md) runs a first query and a first load.
- **The language** covers scripts, parameters, connections, queries and the
  statements around them.
- **Reference** lists the expressions, types and functions.
- **Connectors** has what is particular to each kind of database, API and file
  server.
- **Tools** describes the command line, the REPL and the kernel a notebook
  drives.
