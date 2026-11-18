# INSERT, DELETE and UPDATE

These change the rows of a table that already exists: a database table
(PostgreSQL, MySQL or SQL Server) or a Delta table (a folder, `'path/'`). Each
statement lands whole or not at all. `LOAD INTO` is the statement that creates,
replaces and upserts tables and writes files; these leave the table's definition
alone and change only its rows.

## INSERT

```sql ignore
INSERT INTO pg.public.orders SELECT * FROM 'orders_today.parquet';
INSERT INTO pg.public.orders (id, amount) SELECT order_id, total FROM erp.dbo.pedidos WHERE dia = $dia;
INSERT INTO 's3://lake/orders/' SELECT * FROM 'orders_today.parquet';
```

The query's columns are matched to the table's by name, in any order. A column
list names them instead, in order: `INSERT INTO t (id, amount) SELECT a, b`
puts `a` in `id` and `b` in `amount`, and the list and the query must give as many
columns. A column the rows lack gets the table's default on a database, and NULL on
a Delta table (a NOT NULL one is refused there). A column the table lacks is an
error, as is a table that does not exist: `INSERT` never creates one (`LOAD INTO`
does).

On a database the rows load, on every lane in parallel, into a staging table of
the run's own, holding only the inserted columns, typed as the table types them
and with none of its constraints. Once they are all in, one `INSERT ... SELECT`
in a transaction copies them into the table, and the staging table is dropped.
So a failure anywhere, a NULL into a NOT NULL column or a lost connection mid
load, leaves the table as it was; readers see the new rows only once they are
all there; and the table's defaults, identity columns, triggers and constraints
apply as to any insert. On SQL Server an identity column the rows give is taken
as given. If the connection breaks while that last statement runs, the run fails
saying it cannot know whether the rows landed, rather than sending them twice. On
a Delta table the rows are one commit, as an `APPEND`.

`INSERT` takes the options of a `LOAD INTO` to its target after the column list,
`WITH (merge_schema = true)` on a Delta table among them, and a query starting
with `WITH` is its own: `INSERT INTO t WITH q AS (...) SELECT ...`. StarRocks and
Doris tables, and CSV, Parquet and other files, are not `INSERT` targets yet;
`LOAD INTO ... APPEND` adds to them.

## DELETE

```sql ignore
DELETE FROM pg.public.orders WHERE status = 'cancelled' AND created < '2025-01-01';
DELETE FROM pg.public.orders o WHERE o.id IN (SELECT order_id FROM 'refunds.csv');
DELETE FROM pg.public.orders o
 WHERE NOT EXISTS (SELECT 1 FROM erp.dbo.pedidos p WHERE p.id = o.id AND p.empresa = o.empresa);
DELETE FROM pg.public.staging_orders;
DELETE FROM 's3://lake/orders/' WHERE dia < '2024-01-01';
```

The `WHERE` holds conditions on the table's columns, joined by `AND`, and `IN`,
`NOT IN`, `EXISTS` and `NOT EXISTS` subqueries over any source: a file, another
connection, the same database. The table may take an alias, as in a `SELECT`.
Without a `WHERE` every row is deleted.

### On a database

The conditions on the table's columns go to the database as SQL, as they are
written, so the database decides them as it would its own statement: its
collation compares the text, its functions run. A `$param`, `LET` or loop
variable in them is sent as its value. A condition that cannot be sent, such as
one calling a function only basalt has, is an error naming it.
Each subquery runs in basalt, and its key columns load, on the lanes as any load,
into a staging table of the run's own on the target database. Then one statement,
`DELETE FROM t WHERE ... AND [NOT] EXISTS (SELECT 1 FROM staging WHERE ...)`,
deletes the rows, in the database's own transaction, and the staging tables are
dropped whether it ran or failed. A subquery is matched to the table by
equalities, written as `col IN (SELECT ...)` or in the `EXISTS`'s `WHERE`,
`p.id = o.id AND p.empresa = o.empresa`; any other correlation, `p.id > o.id`, is
refused, as is an `IN` whose left side is an expression.

`check` and `EXPLAIN` connect to nothing, and `EXPLAIN ANALYZE` names the
statement it would run without loading the subqueries or deleting a row.
StarRocks and Doris tables are not `DELETE` targets yet.

### On a Delta table

basalt finds the rows itself, evaluating the conditions and subqueries as a
`SELECT` would, and deletes them in one commit, recorded as a `DELETE`. A file
whose partition values rule the conditions out is not read; one whose partition
values make every condition true, with no subquery, is removed whole without
being read. A file holding a deleted row is rewritten without it, in the same
commit that removes it, and a file holding none is left as it is. On a table that
takes deletion vectors (`delta.enableDeletionVectors`) the file is kept and its
deleted rows marked in its vector instead, and a table keeping a change data feed
(`delta.enableChangeDataFeed`) gets each deleted row as a `delete` change. An
append-only table (`delta.appendOnly`) is refused. The commit fails as a conflict
when another writer removed a file the `DELETE` read since it started; appends
beside it are not a conflict. `EXPLAIN` says how many files it would read, and
`EXPLAIN ANALYZE` how many rows it would delete, committing nothing.

### Either way

A `NOT IN` keeps SQL's rule: when the subquery returns a NULL, no row is
deleted, and the run warns naming how many it returned. `WHERE col IS NOT NULL`
in the subquery, or a `NOT EXISTS`, is the usual intent. The run log says how
many rows were deleted. Nothing is deleted when the run fails before the last
statement or commit.
