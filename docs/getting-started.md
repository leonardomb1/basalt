# Getting started

Download the binary for Linux x86-64 from the latest release, or build it from
source with Zig 0.15.2 (see the
[README](https://github.com/leonardomb1/basalt#install)):

```console
$ curl -fsSL -o basalt https://github.com/leonardomb1/basalt/releases/latest/download/basalt-x86_64-linux
$ chmod +x basalt
```

## A first query

Save this as `orders.csv`:

```text
id,region,amount,placed_at
1,north,120.50,2026-03-01
2,south,80.00,2026-03-01
3,north,42.25,2026-03-02
4,east,310.00,2026-03-02
5,south,15.75,2026-03-03
```

A `SELECT` on its own prints its rows. `-c` runs a script given on the command
line:

```console
$ basalt run -c "SELECT * FROM 'orders.csv' LIMIT 3;"
id  region  amount  placed_at
--  ------  ------  ----------
1   north   120.5   2026-03-01
2   south   80      2026-03-01
3   north   42.25   2026-03-02
(3 rows)
```

The file's extension picks the reader, and the column types are read from the
data: `amount` is a number and `placed_at` a date. Queries are SQL:

```console
$ basalt run -c "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
                 FROM 'orders.csv' GROUP BY region ORDER BY revenue DESC;"
region  orders  revenue
------  ------  -------
east    1       310
north   2       162.75
south   2       95.75
(3 rows)
```

## A first load

A script in a file can take parameters and write its result instead of
printing it. Save this as `daily.sql`:

```sql
PARAM since DATE DEFAULT '2026-03-02';

LOAD INTO 'daily.parquet' AS
SELECT placed_at AS day, region, SUM(amount) AS revenue
FROM 'orders.csv'
WHERE placed_at >= $since
GROUP BY placed_at, region
ORDER BY day, region;
```

`check` validates a script without running it — every name, type and
parameter — and `run` executes it. `-p` binds a parameter:

```console
$ basalt check daily.sql
ok: daily.sql checks out
$ basalt run daily.sql
Read 5 rows, loaded 3 into daily.parquet in 86ms (58 rows/s, 12 lanes)
$ basalt run daily.sql -p since=2026-03-01
Loaded 5 rows into daily.parquet in 90ms (55 rows/s, 12 lanes)
```

A mistake is reported where it is, before anything runs:

```console
$ basalt check -c "SELECT regoin FROM 'orders.csv';"
<command>:1:8: error: unknown field `regoin`
```

## Seeing the plan

`EXPLAIN` prints how a query will run without running it:

```console
$ basalt run -c "EXPLAIN SELECT region, SUM(amount) AS revenue FROM 'orders.csv'
                 WHERE amount > 50 GROUP BY region;"
plan
  write  stdout  (default)
    aggregate  1 agg(s), 1 group(s)
      schema: region:string?  revenue:float?
      filter
        schema: id:int?  region:string?  amount:float?  placed_at:date?
        scan  csv  orders.csv
          schema: id:int?  region:string?  amount:float?  placed_at:date?
  physical: morsel-parallel candidate (per-lane partials, combined)
```

## Next

- [Scripts](language/scripts.md) describes what a script may hold.
- [Connections](language/connections.md) reaches databases, APIs and file servers.
- [Sources](language/sources.md) and [LOAD INTO](language/load-into.md) list what
  can be read and written.
- [REPL](tools/repl.md) is the interactive way to explore data.
