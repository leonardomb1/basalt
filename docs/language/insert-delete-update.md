# INSERT, DELETE and UPDATE

These change the rows of a table that already exists: a database table
(PostgreSQL, MySQL, SQL Server, StarRocks or Doris), a Delta table (a folder,
`'path/'`), or a single file ([Files](#files)). Each
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

On StarRocks and Doris the staging table is made by `CREATE TABLE ... AS SELECT`
over the inserted columns, the rows reach it by Stream Load, and one
`INSERT ... SELECT`, a load transaction of its own, moves them into the table.
StarRocks keeps the target's NOT NULL on the staging table, so there a NULL
into such a column fails the Stream Load into it, naming the rejected rows' log;
either way the table is left as it was. Into a StarRocks Primary Key table or a Doris Unique Key
table a row whose key is already there replaces it, as their own `INSERT` does.

`INSERT` takes the options of a `LOAD INTO` to its target after the column list,
`WITH (merge_schema = true)` on a Delta table among them, and a query starting
with `WITH` is its own: `INSERT INTO t WITH q AS (...) SELECT ...`. Into a
file, see [Files](#files).

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

StarRocks and Doris take the same statement, and decide what their table's
model allows: a StarRocks Primary Key table or a Doris Unique Key table (merge on
write) takes any condition and subquery, while a Duplicate Key table takes only
simple conditions on its columns; StarRocks refuses a subquery there with its
own message.
They report no count of deleted rows for a table that is not keyed, so the run
log then says only that it deleted.

### On a Delta table

basalt finds the rows itself, evaluating the conditions and subqueries as a
`SELECT` would, and deletes them in one commit, recorded as a `DELETE`. A file
whose partition values rule the conditions out is not read, nor is one whose
statistics in the log (`minValues`, `maxValues`, `nullCount`) do for a
condition comparing a column with a constant, and a file read skips the row
groups its footer rules out the same way; one whose partition values make every
condition true, with no subquery, is removed whole without being read. A file holding a deleted row is rewritten without it, in the same
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
take one of several values, and nothing is changed. Beside a `FROM` the `WHERE`
may hold `IN` and `EXISTS` subqueries too; it needs at least one condition
without one, the match to the source.

On a database the `SET` and `WHERE` go to the database as SQL, as a `DELETE`'s
conditions do, so a value is computed and converted as the database computes and
converts it (its arithmetic, its rounding into the column's type). The FROM
source's columns that the statement reads load into a staging table of the run's
own, as a `DELETE`'s subqueries do, and the statement joins the table to it: in
PostgreSQL, StarRocks and Doris `UPDATE ... FROM`, in MySQL `UPDATE ... JOIN`, in
SQL Server `UPDATE ... FROM ... JOIN`. The run log says how many rows the
database reports it changed, which in MySQL leaves out a row set to the values it
already had. StarRocks updates only a Primary Key table and Doris only a Unique
Key one; another table is refused with their own message.

On a Delta table basalt computes the new values, each cast to its column's type,
and writes the changed rows to new files in the same commit that rewrites, or on
a table taking deletion vectors marks, the files they came from, recorded as an
`UPDATE`. A row whose partition column changes moves to its new partition. Files
and row groups that the conditions on the table rule out, by partition values or
statistics, are not read. A table keeping a change data feed gets each row's
`update_preimage` and `update_postimage`. A variant column is carried through as
the table holds it, so an updated row keeps its other variants' types, and `SET`
gives one a value as JSON text; a `SET` or `WHERE` may not read a variant
column yet.

`EXPLAIN` and `EXPLAIN ANALYZE` behave as for a `DELETE`: on a database they name
the statement without staging a row; on a Delta table `EXPLAIN` says how many
files it would read and `EXPLAIN ANALYZE` how many rows it would update.

## Files

```sql ignore
INSERT INTO 'vendas.csv' WITH (delimiter = ';', encoding = 'latin1') SELECT * FROM erp.dbo.vendas WHERE dia = $dia;
DELETE FROM 's3://lake/clientes.parquet' WHERE ativo = false;
UPDATE 'precos.xlsx' WITH (sheet = 'Tabela') SET preco = preco * 1.05 WHERE categoria = 'A';
UPDATE 'clientes.ndjson' c SET email = n.email FROM 'novos.csv' n WHERE c.id = n.id;
```

A single file takes the three statements too: a CSV (or TSV), NDJSON or JSON
array, Parquet, Avro or Arrow file, or one sheet of an Excel workbook, gzipped
or not, local or on S3, Azure, SFTP or SMB. Its read options follow its name, as
in a `SELECT`: `delimiter`, `encoding`, `sheet`, `format`. The `WHERE`, its
subqueries and an `UPDATE`'s `FROM` are those of a table's statement, evaluated
by basalt as on a Delta table, and a row two `FROM` rows match is an error here
too.

A `DELETE` or `UPDATE` writes the file again whole and puts it in place of the
old one in one step, a local file's new copy renamed over it and a remote one's
written in one upload, so a reader sees the old file or the new one, never a
mix, and a run that fails leaves the file as it was. Rows keep their order, and
nothing is written when no row changes. What the new file keeps depends on the
format:

- A CSV or NDJSON file is copied byte for byte. Every record the statement does
  not change stays exactly as it was, with its quoting and line ending, and so do
  a byte order mark, blank lines and the file's text encoding. A deleted record
  goes with its line ending. An updated CSV record is written as `LOAD INTO`
  writes a row, in the file's delimiter and encoding; an updated NDJSON object
  keeps its other keys as they were and changes only the ones set.
- An Excel workbook keeps every part but the sheet as it was: the other sheets,
  the shared strings, the styles. In the sheet each row kept is copied as it
  is, moved up past the rows deleted above it, and an updated row is written as
  `LOAD INTO` writes one, losing its cells' own formatting. A sheet whose rows
  something names by place is refused, as moving them would leave it pointing
  at other cells: a formula, merged cells, conditional formats, data
  validations, hyperlinks, tables, drawings or comments in it, or a formula or
  defined name elsewhere in the workbook naming it. So is a sheet read through
  `range`, only part of one.
- Any other format is read and written again as `LOAD INTO` writes it, the
  updated rows in their places; a JSON array stays an array.

`INSERT` adds rows to a file that exists; one that does not is an error, and
`LOAD INTO` makes it. The rows' columns are matched to the file's by name: a
column the rows lack is left empty, one the file lacks is an error, and the
file's own columns do not change. Each value becomes the type its column is read
as from the file, so the file reads back whole: `40.00` goes into a CSV column
that reads as INT as `40`, and `40.5` is refused rather than rounded. A local CSV or NDJSON file takes the rows at
its end, in place: a CSV's in its own delimiter, encoding and line ending, a
gzipped one's as a gzip member of their own, which gzip readers read on into.
If the write fails, the file is cut back to where it ended. A file whose last
byte is not a line break is refused, as its last line may be one a failed write
cut short. A remote CSV or NDJSON file is written again whole, the new rows
after its own; an Excel sheet takes them after its last row, its workbook
otherwise as it was; any other format is written again whole, its rows first,
each new value cast to its column's type.

A local file whose size, modification time or inode changed between the read
and the swap, another writer's doing, is not replaced: the run fails saying so,
and running it again starts from the new file. On S3 and Azure the upload is
sent with `If-Match` on the ETag read first, so the store refuses it in the
same case. SFTP and SMB have no such check, so two runs changing one file there
at once can lose one's changes. A local symbolic link, a file with other hard
links or one that is not a regular file is refused, as the rewrite would put a
file of its own in their place.

What a rewrite would lose is refused: an XML file (basalt writes XML in its
own shape), a `records` file, a JSON document read through `items`, a file
inside an archive (`'a.zip :: b.csv'`), a zstd file and a file read over HTTP
or FTP.
`EXPLAIN` says how the file would be written, and `EXPLAIN ANALYZE` how many
rows would change, writing nothing.
