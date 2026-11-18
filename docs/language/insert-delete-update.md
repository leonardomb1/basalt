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
