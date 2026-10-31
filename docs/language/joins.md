# Joins

A join with an `=` key is a hash join: one side is materialized and indexed
once, the other streams through — the right side is indexed unless the left is
estimated much smaller ([Which side is held in memory](#which-side-is-held-in-memory)).
One whose `ON` has no `=` between a left and a right value is a nested-loop or
range join ([Joins without a key](#joins-without-a-key)), which holds the right
side in memory. The right side is a CTE, a `(SELECT ...)`, a table
function, or any source a `FROM` reads — a path (`JOIN 'smb://fs/x.xlsx' x`, its
`WITH (...)` after the alias) or a connection's table (`JOIN sr.db.t AS t`),
read as `(SELECT * FROM it)` would be. A key is an `=` between a value of each
side, `AND`-combined for composite keys, written in either order; a null key
never matches. `USING (k, d)` joins on the columns of those names on both sides,
and `SELECT *` lists each once, first, as SQL does; under a right or full join
that column is the first non-null of the two. Each side keeps its own copy too:
`a.k` and `b.k` are that side's value (null on the rows an outer join adds
for the other side), and `a.*` and `b.*` list that side's columns in its own
order, `k` among them. `NATURAL JOIN` is refused — name the shared
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
JOIN <cte>` takes no `ON`; an `ON` with no condition across the sides (only
`b.del <> '*'`) pairs every left row with every right row it leaves. A cross
join holds its right side in memory like a [join without a
key](#joins-without-a-key), up to its `max_build` or 4 GiB, and emits the pairs
65,536 at a time, so a `WHERE` after it filters them as they come. The join's
own options (`max_build`, `join_order`, `key_pushdown`) may sit in a joined
path's `WITH (...)` after the alias — `CROSS JOIN 'big.parquet' p WITH
(max_build = '1GB')` — and apply to the join.
Right-side columns that collide with a left name come back suffixed `_r`, and
`_r2`, `_r3`, … if that name is taken too — that is the name `SELECT *` shows.
In the joining query, a bare `amt` that both sides have is refused as
ambiguous, as SQL refuses it (`SELECT`, `WHERE`, `GROUP BY`, `ON`, a window), never
read as the left one; a `USING` key is the one merged column, and `ORDER BY amt`
under `SELECT *` names the output column `amt`, the suffixed one being `amt_r`.
Past the query — from a CTE or derived table over it — the output names are
plain names. A qualified reference needs no suffix: with `FROM t a JOIN r b`, `b.amt` is the
right side's `amt` everywhere in the query (`SELECT`, `WHERE`, `GROUP BY`,
`ORDER BY`, a later join's `ON`), and `SELECT b.amt` calls its output `amt`
unless `a.amt` is already in the list (then `amt_r`). `b.*` is the right side's
columns alone, each named so too, and `a.*` the FROM table's. A qualifier must
name a table of the query — its alias, or its own name when it has none
(`FROM c`, `FROM sr.db.t` as `t`, `FROM 'dir/l.csv'` as `l`) — or the query is
refused (`unknown table`); `b.x` where `b` has no `x` is an unknown column,
never another table's `x` — for the FROM table's alias too, and for a table the
chain joins only later (`ON c.x = a.x` before `JOIN c`). A pipeline shaped `read | filters | join
| filters | write` probes in parallel under `-j` — over local CSV/Parquet
morsels, and over key-range splits for a splittable SQL source. A chain of
joins followed by `GROUP BY` fans out the same way (`read | filters |
join+ | filters | aggregate | sort/limit | write`). Right and full joins probe
in parallel in the first shape: every lane marks the build rows it matched in
one shared set, and once all lanes are done the build rows nothing matched are
written once, in build order, through the filters after the join, after
every other row, as a serial run writes them. Under a `GROUP BY` they stay
serial.

## Joins without a key

An `ON` with no `=` between a left and a right value — `a.x < b.y`, `a.d
BETWEEN b.from AND b.to`, `a.k = b.k OR a.alt = b.k`, any condition — is a
nested-loop join, of any kind: inner, left, right, full. The right side is read
into memory, then each left row is paired with the right rows and the whole `ON`
decides which pairs join, evaluated over up to 65,536 pairs at a time; a
comparison with a null is no match, and the unmatched rows of an outer join come
out as with a hash join. When the `ON` holds, `AND`-ed, a comparison of a left
value with a right one (`L BETWEEN R1 AND R2`, `L >= R`, `L < R`, …) of one
orderable type — numbers, or text, dates, times or timestamps on both sides —
it is a range join: the right rows are sorted by that bound and each left row is
paired only with those a binary search leaves, which makes looking up the period
a date falls in cheap:

```sql
SELECT s.id, p.name AS period
FROM 'sales.csv' s
LEFT JOIN 'periods.csv' p
  ON CAST(s.sold_on AS DATE) BETWEEN CAST(p.starts AS DATE) AND CAST(p.ends AS DATE);
```

With both bounds the search keeps the rows that start on or before the date
whose running latest end reaches it, so periods that do not overlap cost one
row each. An `OR` of equalities has no key and no range: it runs as a plain
nested loop, comparing every pair — correct, but its cost grows with the product
of the sides; split it into two keyed joins when the sides are large. `EXPLAIN`
names the join `(range)` or `(nested-loop)`, or — when a side's columns are only
known once it is read, as for a SQL table or a remote file — `(no key: range or
nested-loop, decided at run time)`; `EXPLAIN ANALYZE` then says which ran under
the join. Such a join never spills, never
runs in parallel lanes and takes no key pushdown: its right side must fit in
memory, up to the join's `WITH (max_build = '…')` or 4 GiB, past which it fails
with the build-too-large error.

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

Inner, left, right and full joins are turned around; a `LEFT JOIN` run that way
indexes its left rows, streams the right side through them, and emits the left
rows that matched nothing once the right side ends, with nulls for the right
columns, as written. Semi, anti, cross and `NOT IN` joins always index their
right side. Where a pipeline's joins run in parallel lanes (see above), the first
join of a `GROUP BY` pipeline whose right side is a local Parquet or CSV read is
turned around the same way: its left side is indexed once and the lanes read the
right side, the unmatched left rows folded in last, so the result is the same at
every `-j`. The last join of such a pipeline, past the first, is decided on what
the run meets instead: the joins before it run on the lanes first, into memory and
narrowed to the columns the rest reads, and if they leave fewer rows than a
quarter of its right side's estimate they are indexed and the lanes read that
side (TPC-H Q3: customers and orders indexed, lineitem streamed). Past that many
rows the held rows are dropped and the pipeline runs as planned; a join after it,
such as an `IN`, would lose its [runtime filter](#runtime-filters), so only the
last join is taken. A single join whose head is filtered is decided the same way:
the filtered head is read into memory first, and indexed if it stays under that
cap (TPC-H Q12: 31K of 6M lineitem rows indexed, the 1.5M orders streamed). It
is tried only when the head's first row group or chunk passes rows at a rate that
stays under the cap, and also at `-j 1`, on one worker, where plain `EXPLAIN` adds
it to the serial path as an `unless`. Any other lane pipeline probes with its head read in every lane and
keeps the written order — a right side too big for the lanes sends the pipeline
back to the serial plan, where the choice is made. Key pushdown follows the sides: a SQL right side streamed through an
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
an error naming the limit once it passes `--op-memory`.
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

A SQL read that heads a chain of joins takes the keys of every later join too,
AND-ed into the one query it sends, so files or other databases joined further
on narrow what the database returns:

```sql
SELECT o.id, s.qty, t.label
FROM pg.orders o
JOIN 'sheet.csv' s ON s.cr = o.code
JOIN 'tags.csv' t ON t.k = o.kind
-- orders is read as … WHERE "code" IN (…) AND "kind" IN (…)
```

A later join hands its keys on only where the joins before it were all on the
read's side, with nothing but filters in between, and it is of a kind that drops
unmatched left rows; a key qualified by another side's name is never sent. Such a
join is not reordered among the others.

Up to 1,000 distinct values go as `IN (…)`; past that an integer key goes as up to
32 ranges cut at its widest gaps, when they cover under half its span, else, as
any number, decimal, date or timestamp key does, as its `>= min AND <= max` range,
and a text key as nothing, since the database's collation may order text otherwise. A value the dialect
cannot spell exactly drops that key's predicate, never just the value: the
read may return more rows than match, which the join discards, never fewer. An
empty side reads nothing (`WHERE 1 = 0`). `EXPLAIN` names the side that takes
the keys; `WITH (key_pushdown = false)` on the join turns it off.

The SQL side also asks only for the columns the join and what follows it use:
above, the table is read as `SELECT code, name FROM …`, since the key reads
`code` and the output `name`. A Parquet, Arrow or CSV file joined in is read the
same way, decoding only those columns and the ones its own filters name, serial
or on lanes. A query that keeps every column (`SELECT *`, or a join with nothing
after it) reads them all.

A Parquet file or folder, local or remote (`https://`, `s3://`, `az://`, SFTP,
SMB), takes keys on the same terms, when no SQL side does: on the right of the
join from the left side's read-ahead, and on the left once the right side is
indexed. A Parquet left side skips under `-j` too, in every lane; a Parquet right
side only on a serial run. It reads no less of a row group it reads, but skips
each row group whose min/max statistics hold none of the key values (or, past
1,000 of them, no part of their range); a remote file's skipped row groups are
never fetched. Only a number,
decimal, date or timestamp key that is one of the file's columns as it is counts:
a key computed from them, a text key (whose statistics' byte order the file does
not pin down), or a row group without statistics never skips.

```sql
SELECT o.id, o.amount, c.name
FROM 'today.csv' c
JOIN 'orders/' o ON o.customer_id = c.id;
-- reads only the row groups of orders/ whose customer_id range holds a c.id
```

## Runtime filters

In a chain of joins over one read, the keys a later join holds also narrow that
read, before the joins in between see its rows:

```sql
SELECT count(*) AS n
FROM 'lineitem.csv' l
JOIN 'orders.csv' o ON o.o_orderkey = l.l_orderkey
JOIN 'part.csv' p ON p.p_partkey = l.l_partkey
WHERE p.p_brand = 'Brand#23';
-- the lineitem rows whose l_partkey is no Brand#23 part are dropped as they are
-- read: the orders join never hashes, probes or copies them
```

Once a join has indexed its side, a filter directly above the read checks each row
against a compact summary of that side's keys (a Bloom filter): a row whose key is
null, or certainly not there, is dropped. The summary never misses a key that is
there, and the join still checks every row it gets, so the answer and its row
order are the same with the filter or without, at any `-j`.

A join's filter reaches the read its keys come from: followed through filters,
through selects that pass them on unchanged, and through each join before it,
into its left side when the keys are left columns, or into its right side when
they are right columns of an inner, left, right or full join. A key that is also
one of an earlier join's `=` keys crosses to that join's other side too, as the key
it equals: `WHERE o.o_orderkey IN (SELECT ...)` narrows the lineitem a join on
`l.l_orderkey = o.o_orderkey` holds. On the right side the filter narrows what that
join holds in memory:

```sql
SELECT count(*) AS n
FROM 'lineitem.csv' l
JOIN 'orders.csv' o ON o.o_orderkey = l.l_orderkey
JOIN 'customer.csv' c ON c.c_custkey = o.o_custkey
WHERE c.c_mktsegment = 'BUILDING';
-- orders is read without the orders of customers outside BUILDING, so the orders
-- join indexes a fifth of them and most lineitem rows find no match at once
```

A filter needs at least one join between it and its owner, since the first join's
own lookup is the same check, exact. Only a join that drops rows matching nothing
has one: an inner, semi or right join, never `NOT IN`. Keys split over both sides
of an earlier join, or computed, have none, and nothing that counts, orders or
numbers rows (`LIMIT`, a window, `DISTINCT`, an aggregate) may lie between. A join
whose side spilled to disk has none either. A filter on an earlier join's right
side needs its owner's side in memory first, so the joins are indexed last first;
an owner whose right side is a SQL table that takes the left side's keys reads its
left side ahead for them instead, and gets no such filter. A Parquet right side
gives its read-ahead up to the filter.

A filter that drops fewer than one row in eight of its first 65,536 turns itself
off: checking costs a hash per row. `EXPLAIN ANALYZE` shows each filter above its
read, the joins it stands for, and the rows it checked and dropped;
`WITH (runtime_filter = false)` on a join keeps it from having one.
