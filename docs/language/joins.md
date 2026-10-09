# Joins

Joins are hash equi-joins: one side is materialized and indexed once, the other
streams through — the right side is indexed unless the left is estimated much
smaller ([Which side is held in memory](#which-side-is-held-in-memory)). The
right side is a CTE, a `(SELECT ...)`, a table
function, or any source a `FROM` reads — a path (`JOIN 'smb://fs/x.xlsx' x`, its
`WITH (...)` after the alias) or a connection's table (`JOIN sr.db.t AS t`),
read as `(SELECT * FROM it)` would be. A key is an `=` between a value of each
side, `AND`-combined for composite keys, written in either order; a null key
never matches. `USING (k, d)` joins on the columns of those names on both sides
and lists each once, first, as SQL does; under a right or full join that column
is the first non-null of the two. `NATURAL JOIN` is refused — name the shared
columns with `USING`. A key may be computed — `trim(t.code) = CAST(x.code AS
string)`, each value naming one side's columns — and each side then computes it
before the join, out of sight of `SELECT *`. Columns need no table when the names say which
side they are: `trim(code) = CAST(cr AS varchar)` finds `code` and `cr` in the two
sides' columns, in either order, and is refused as ambiguous only when both
values' columns exist on both sides. Keys of two types (a spreadsheet's number
against a table's text) do not join until one is cast. The rest of an `ON` is a
condition. In an inner or left join, one naming only the right side, or no
column (`1 = 1`), narrows the right side before the join, pushed down to a SQL
source as that side's `WHERE` — so `JOIN sr.t AS t ON t.D_E_L_E_T_ <> '*' AND
t.k = x.k` reads only live rows. In an inner join any other condition (the left
side alone, the two sides compared otherwise than by `=`) filters the joined
rows. In an outer join it decides which key matches count instead, and a row
none of whose matches pass still comes out, unmatched — the way to look up the
value valid on a date:

```sql
SELECT s.id, p.price
FROM 'sales.csv' s
LEFT JOIN 'prices.csv' p
  ON p.product = s.product
 AND CAST(s.sold_on AS DATE) BETWEEN CAST(p.valid_from AS DATE) AND CAST(p.valid_to AS DATE);
```

A right or full join keeps every right row the same way, so there a condition
on the right side alone decides matches too rather than narrowing it. `CROSS
JOIN <cte>` takes no `ON`.
Right-side columns that collide with a left name come back suffixed `_r`, and
`_r2`, `_r3`, … if that name is taken too — that is the name `SELECT *` shows. A
qualified reference needs no suffix: with `FROM t a JOIN r b`, `b.amt` is the
right side's `amt` everywhere in the query (`SELECT`, `WHERE`, `GROUP BY`,
`ORDER BY`, a later join's `ON`), and `SELECT b.amt` calls its output `amt`
unless `a.amt` is already in the list. A pipeline shaped `read | filters | join
| filters | write` probes in parallel under `-j` — over local CSV/Parquet
morsels, and over key-range splits for a splittable SQL source. A chain of
joins followed by `GROUP BY` fans out the same way (`read | filters |
join+ | filters | aggregate | sort/limit | write`). Right and full joins probe
in parallel in the first shape: every lane marks the build rows it matched in
one shared set, and once all lanes are done the build rows nothing matched are
written once, in build order, through the filters after the join, after
every other row, as a serial run writes them. Under a `GROUP BY` they stay
serial.

## Which side is held in memory

An inner join holds its right side in memory and streams the left side through
it, unless the left side is estimated at least four times smaller: then the left
side is indexed and the right side streams. A chain of inner joins that all key
on the columns of the table they start from (a fact table and its dimensions,
`f JOIN a ON f.x = a.x JOIN b ON f.y = b.y`) runs its smallest side first. The
result is the written query's — same columns in the same order, same names
(`_r` suffixes included), same rows — and only the order of the rows can differ.
So neither choice is made unless the rows are reordered anyway: a `GROUP BY`,
an aggregate or an `ORDER BY` must follow the joins, with only `WHERE`
conditions, projections and other joins between. A pipeline that only filters,
projects and joins keeps its source's order, as [Queries](queries.md) promises.

The estimates are cheap and taken when the query is planned: a Parquet file's
row count from its footer, a CSV, TSV or JSON-lines file's from its size and the
lines in its first 64 KiB, another local file's size, a SQL table's catalog
statistics (PostgreSQL's `pg_class.reltuples`, MySQL, StarRocks and Doris's
`information_schema.TABLES.TABLE_ROWS`, SQL Server's `sys.partitions`), asked once
per run. A side's `WHERE` keeps its table's estimate. Rows are compared with
rows, else bytes with bytes; a side that cannot be estimated — a remote file, a
folder, a SQL query, a REST read, a side that aggregates or joins before this
join, a table never analyzed — keeps the join as written. A left side whose
estimated bytes pass the join's spill threshold is not moved into memory.

Only inner joins are turned around; left, right, full, semi, anti, cross and
`NOT IN` joins always index their right side. The choice applies where a join
runs serially; a pipeline whose joins run in parallel lanes (see above) probes
with its head read in every lane and keeps the written order — a right side too
big for the lanes sends the pipeline back to the serial plan, where the choice is
made. Key pushdown follows the sides: a SQL right side streamed through an
indexed left side takes the left side's keys once that side is indexed, with no
read-ahead. `EXPLAIN ANALYZE` (and `run --explain`) prints the choice under the
join, e.g. `build: left (est. 1.2k rows vs 3.4M rows)`; plain `EXPLAIN` only says
when it will be decided. `WITH (join_order = 'written')` on a join keeps it as
written, its right side in memory and its place in the chain:

```sql
SELECT o.id, c.name
FROM 'customers_eu.csv' c
JOIN 'orders.parquet' o ON o.cust = c.id WITH (join_order = 'written')
ORDER BY o.id;
```

## Spilling

The build side is held in memory up to `--op-memory` (2 GiB by default), or the
join's own `WITH (max_build = '16GB')`. Past that the join spills: both sides
are split by key into 16 partitions, a file per side each, in the run's scratch
directory, and joined one partition at a time, so memory holds one partition's
build rows. A partition whose build rows still pass the limit is split again
into 16, and so on up to 4 levels (65,536 partitions); each file is deleted as
soon as it is split or joined, so the disk holds about one copy of the data. A
spilled join gives the same rows, but not in the same order — add `ORDER BY` if
order matters. Splitting cannot separate rows of one key: a key whose rows alone
pass 4 GiB (or `--op-memory`, if larger), or `max_build` when given, fails at
once with "a single join key holds N rows, more than --op-memory allows" —
raise `--op-memory` or `max_build`, or filter that key out; one under it is held
whole. A partition still past that after 4 levels fails too. A `NOT IN` build side
never spills, since one null on it changes every row's answer, and fails with
the build-too-large error past the limit.
Under `-j` a build side past the limit sends the pipeline back to a serial run,
which reads that side again — a second query for a SQL source. `--spill-cap`
bounds the disk a run's spills hold at once (8 GiB by default; a partition's files
count until it is joined) and `--spill-dir` puts them elsewhere; the files are
removed when the run ends.

## Key pushdown

When one side of a join is a SQL table, the other side's key values narrow its
read, so joining a small sheet to a large table reads only the table's matching
rows:

```sql
CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'fe', database = 'erp');
SELECT s.cr, c.name
FROM 'centers.xlsx' s
JOIN sr.erp.cost_centers c ON trim(c.code) = CAST(s.cr AS varchar);
-- the table is read as … WHERE TRIM(`code`) IN ('1005', '1048')
```

A SQL right side takes the left side's keys: the left side is read first, into
memory, and replayed into the join. Past 100,000 rows or 64 MB the read-ahead
stops and the right side is read in full, so a large left side costs nothing
extra; a join turned around (above) sends the keys once the left side is
indexed instead. A local file of up to 64 MB joined to a SQL table runs serially even
under `-j`, so the table can take its keys; a bigger one keeps its lanes. Otherwise a SQL left read right before the first join takes the right
side's keys once that side is indexed, under `-j` too, where every split reads
with them. A side takes keys only where the join never outputs its unmatched
rows — the right side of an inner, left, semi or anti join, the left side of an
inner, right or semi one — and not under `NOT IN`. A side with anything but
filters and selects between its read and the join, or a key that is no
function of the read's columns, takes nothing.

Up to 1,000 distinct values go as `IN (…)`; past that a number, decimal, date or
timestamp key goes as its `>= min AND <= max` range, and a text key as nothing,
since the database's collation may order text otherwise. A value the dialect
cannot spell exactly drops that key's predicate, never just the value: the
read may return more rows than match, which the join discards, never fewer. An
empty side reads nothing (`WHERE 1 = 0`). `EXPLAIN` names the side that takes
the keys; `WITH (key_pushdown = false)` on the join turns it off.
