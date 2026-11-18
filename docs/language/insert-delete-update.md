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

## UPDATE

```sql ignore
UPDATE pg.public.orders SET status = 'late' WHERE due < '2025-01-01' AND status = 'open';
UPDATE pg.public.orders SET amount = amount * 1.1 WHERE id IN (SELECT order_id FROM 'repriced.csv');
UPDATE pg.public.orders o
   SET amount = p.total, status = p.status
  FROM erp.dbo.pedidos p
 WHERE o.id = p.id AND o.empresa = p.empresa AND p.dia = $dia;
UPDATE 's3://lake/orders/' SET region = upper(region) WHERE region = 'eu';
```

`SET` gives each column it names a value computed from the row: the table's
columns, the FROM source's, constants, `$param`s and `LET`s. A column the table
lacks is an error, as is one set twice. The `WHERE` holds conditions and `IN`,
`NOT IN`, `EXISTS` and `NOT EXISTS` subqueries, as a `DELETE`'s does; without one
every row is updated.

`FROM` names a source, of any kind, whose rows give the new values: the `WHERE`
matches them to the table's by equalities between their columns, `o.id = p.id`,
and may add conditions on either side. A row of the table that no source row
matches is left as it is. A row two source rows match is an error, as it would
take one of several values, and nothing is changed. A `WHERE` beside a `FROM`
holds no subquery; join what it would read into the source instead.

On a database the `SET` and `WHERE` go to the database as SQL, as a `DELETE`'s
conditions do, so a value is computed and converted as the database computes and
converts it (its arithmetic, its rounding into the column's type). The FROM
source's columns that the statement reads load into a staging table of the run's
own, as a `DELETE`'s subqueries do, and the statement joins the table to it: in
PostgreSQL `UPDATE ... FROM`, in MySQL `UPDATE ... JOIN`, in SQL Server
`UPDATE ... FROM ... JOIN`. The run log says how many rows the database reports
it changed, which in MySQL leaves out a row set to the values it already had.

On a Delta table basalt computes the new values, each cast to its column's type,
and writes the changed rows to new files in the same commit that rewrites, or on
a table taking deletion vectors marks, the files they came from, recorded as an
`UPDATE`. A row whose partition column changes moves to its new partition. Files
the conditions on the table's partition columns rule out are not read. A table
keeping a change data feed gets each row's `update_preimage` and
`update_postimage`. A table with a variant column is not an `UPDATE` target yet.

`EXPLAIN` and `EXPLAIN ANALYZE` behave as for a `DELETE`: on a database they name
the statement without staging a row; on a Delta table `EXPLAIN` says how many
files it would read and `EXPLAIN ANALYZE` how many rows it would update.
StarRocks and Doris tables are not `UPDATE` targets yet.
