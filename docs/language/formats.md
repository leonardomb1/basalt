# File formats

A path's extension picks its reader and writer — `.csv`, `.parquet`, Arrow IPC
and Excel — unless `WITH (format = ...)` names one ([Sources](sources.md#source-clauses)).

## CSV

A header name in double quotes loses its quotes (`""` inside is one quote) and
may hold the delimiter: `"Valor Total"` is the column `Valor Total`. Lines end in
`\n`, `\r\n` or a bare `\r` (old Mac exports), a quoted value keeps any line break
inside it, and a record may run to 64 MiB — past that the read stops with an error
naming the file and line, as a file that is not CSV or a quote left open would
otherwise be read into memory whole. Every row must have one field per
header column: a row with more or fewer is an error naming the file, the line and
both counts, never padded with nulls or cut short.

A CSV column's type is sniffed from the first 1024 rows: int ⊂ float ⊂ string,
and a column whose every non-empty, unquoted cell is an ISO `YYYY-MM-DD` reads as
a `DATE` — so `WHERE day >= '2026-01-01'`, `date_add`, `date_diff` and `EXTRACT`
work on it directly, and a parquet sink stores it as a date. A string function
on it (`substr(day, 1, 7)`, `day LIKE '2026%'`) still sees the ISO text. A cell
past the sample that does not parse as the inferred type is an error, never a
silent coercion. Timestamps are not sniffed; `CAST(ts AS TIMESTAMP)` as before.

A column the query reads only through `CAST` is not sniffed: the cast converts the
cell as written. `SELECT CAST(cep AS STRING) AS cep FROM 'clientes.csv'` keeps
`01310100` even where the first rows made `cep` look like an INT, and
`CAST(valor AS DECIMAL(18,2))` parses the digits exactly instead of through a
float. This holds when every use of the column in that query is a cast — a bare
`cep` anywhere (a `WHERE cep > 0`, a `SELECT *`) reads the sniffed type again, and
the error on a mismatched cell says so. It applies to the CSV a query reads in its
own FROM, not to one joined to it or read through a CTE defined elsewhere.
A column with no value at all — a header-only file, or one whose every cell is
empty — is all NULL, and when the sample covered the whole file it is of unknown
type, as SQL types a NULL: it compares and unifies with any type (`id NOT IN
(SELECT k FROM 'empty.csv')` against an `INT` column, a join, a `UNION`), and it
is a string where shown or written. A larger file's empty sample stays a string,
since the rows past it may hold text.

## Parquet

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched. A
remote `.parquet` (`https://...`, `az://...`, `s3://...`) is read the same way, by HTTP
range request, so a projected query transfers only the chunks it decodes. The
chunks a row group needs are fetched together, nearby ones in one request, and
under `-j` a file with several row groups is read on lanes, each over its own
connection, as a local file is. A server that ignores `Range` falls back to one
whole-object fetch, read on one lane. Parquet writes
store `DECIMAL` in the narrowest physical type its precision allows — INT32 to 9
digits, INT64 to 18, FIXED_LEN_BYTE_ARRAY up to 38; past 38 digits (the engine's
own ceiling) a value is refused rather than silently truncated.

Pages are compressed with Snappy unless the sink says otherwise:
`LOAD INTO 'orders.parquet' WITH (compression = 'gzip') AS ...` writes GZIP
pages, smaller than Snappy's for somewhat more CPU (a fast DEFLATE level), and
`compression = 'none'` leaves them uncompressed. `compression` is a Parquet
sink's option only; any other target refuses it at plan time.

## Delta Lake

A folder holding a `_delta_log/` is a Delta table, and a folder read of it —
local, `s3://`, `az://`, SFTP or SMB — reads the table, not its files: the files
its log keeps live at one version, each read by column name and typed by the
table's schema. Read as a plain folder it would also return the rows of every
file a later commit removed (an overwrite, a `DELETE`, a compaction), so a folder
read looks for the log first, with or without `WITH (format = 'delta')`.

```sql
SELECT region, sum(amt) AS total
FROM 's3://lake/sales/'
WHERE day >= '2026-01-01'
GROUP BY region;

SELECT count(*) AS n_then FROM 's3://lake/sales/' WITH (version = 12);
```

The log is replayed as the protocol says: the newest complete checkpoint at or
below the version read (one Parquet file, or every part of a multi-part one),
then each JSON commit after it, an `add` putting a file in the table and a
`remove` taking it out. A file is the one its path names, relative or absolute,
together with its deletion vector, so a commit that replaces a file's deletion
vector reads the same whatever order its lines are in. `WITH (version = 12)` reads the table as commit 12 left
it, as long as the log still holds what that takes; an older version, or a
commit missing from `_delta_log/`, is refused naming the versions that can be
read. `_last_checkpoint` is not needed, since the log's listing is read.

The schema is the table's (`metaData.schemaString`): a column a file lacks reads
as NULL, as one added after the file was written does, and a file whose column
is of another type fails the read naming the file. A partition column's value
comes from the log, as the protocol asks, whether or not the file holds it too;
an empty or null value is NULL. A filter on a partition column skips the files
whose value it rules out without opening them, and everything a Parquet folder
does still applies: lanes under `-j`, projection, row-group statistics, a join's
keys, ranged reads of remote files. Delta types map as Parquet's do — `long`,
`integer`, `short` and `byte` to `INT`, `float` and `double` to `FLOAT`,
`timestamp` and `timestamp_ntz` to `TIMESTAMP`, a struct to its dotted columns
(`addr.city`), an array or map to one JSON text column.

A file's deletion vector — the rows a `DELETE`, `UPDATE` or `MERGE` removed
without rewriting the file — is applied as the file is read, so those rows are
never returned: stored inline in the log, in a `deletion_vector_*.bin` file under
the table, or at an absolute path, read through the same storage as the table.
A deleted row is skipped by its position in the whole file, whichever row groups
a filter skips, under lanes and `LIMIT` alike, and a row group it deletes whole
is not fetched. `count(*)` counts the rows left. Each vector's size,
CRC-32 and row count are checked against the log, and one that fails, one in
the non-portable (native) bitmap format, or one stored in a way the protocol
does not define (`r`) fails the read naming the file, rather than return the
rows it deletes.

Under column mapping (`delta.columnMapping.mode` `name` or `id`, which counts
where the protocol enables the feature: reader version 2, or 3 with
`columnMapping`) a column is found in the files by its physical name or by its
Parquet field id, nested struct fields too, and partition values by physical
name; a list's or map's JSON names its struct fields as the table does. A
renamed column reads under its new name, a dropped one is not read, a column a
file lacks reads as NULL, and a column dropped and added again under its old
name reads NULL from files written before, as their data is the old column's.
In `id` mode a file without field ids fails the read naming it, rather than read
as all NULLs.

A table using a reader feature basalt does not honour is refused, naming it,
rather than read into wrong rows: V2 checkpoints, a catalog-managed table,
`typeWidening`, `variantType`, a reader version past 3, and any feature basalt
does not know. `deletionVectors`, `columnMapping`, `timestampNtz` and
`vacuumProtocolCheck` read as described. A live file missing from storage (a
`version` older than a `VACUUM` kept, say) fails the read naming it.

A Delta table is found by listing its log, which an `https://` URL cannot do; it
is read from a local path, object storage, SFTP or SMB. `format = 'parquet'`
reads the folder's files plain, removed ones included. EXPLAIN and `check` name
the version and file count of the table they read, and `check` types a local
table from its log.

### Writing Delta

A Delta table is a table target for [LOAD INTO](load-into.md): a folder path
(ending in `/`) whose `_delta_log/` holds the table, or, with `WITH (format =
'delta')`, one where a write creates it. Local folders, `s3://` and `az://` are
written; SFTP, SMB and `https://` are refused, as they offer no create-if-absent
write a commit needs.

```sql
LOAD INTO 's3://lake/sales/' WITH (format = 'delta', partition_by = 'region, day') AS
SELECT region, CAST(day AS DATE) AS day, order_id, amt FROM 'sales_2026.csv';

LOAD INTO 's3://lake/sales/' AS                          -- APPEND, the default
SELECT region, CAST(day AS DATE) AS day, order_id, amt FROM 'sales_late.csv';

LOAD INTO 's3://lake/sales/' REPLACE AS
SELECT region, CAST(day AS DATE) AS day, order_id, amt FROM 'sales_fixed.csv';

LOAD INTO 's3://lake/sales/' UPSERT ON (order_id) AS
SELECT region, CAST(day AS DATE) AS day, order_id, amt FROM 'sales_changed.csv';

LOAD INTO 's3://lake/sales/' UPSERT ON (order_id) PARTIAL COLS (amt) AS
SELECT order_id, amt FROM 'amounts_corrected.csv';
```

`APPEND` (the default, as for a database table) adds the rows as new files in
one commit; `REPLACE` is one commit that removes every file live at the version
it read and adds the new ones, so a reader sees the old rows or the new, never
neither, and every earlier version stays readable with `WITH (version = N)`.
`UPSERT` merges by key (below). A write of no rows commits nothing, except that
it still creates a table, and a `REPLACE` of no rows empties one.

A created table is version 0 with protocol 1/2 and the rows' schema: `INT` as
`long`, `FLOAT` as `double`, `DECIMAL(p,s)` as `decimal(p,s)`, `TIMESTAMP` as
`timestamp` (UTC-adjusted in its files), `BOOL`, `STRING`, `BYTES` and `DATE` as
`boolean`, `string`, `binary` and `date`, every column nullable. A `TIME`, an
array, a struct or an untyped NULL column is refused, to be cast first.
`WITH (partition_by = 'c1, c2')` partitions it; a later write takes the table's
own partition columns, and naming different ones is refused. A write to an
existing table must give exactly its columns — by name, in any order; one
missing or one more is refused, as the schema is never changed — each of the
table's type: an integer column of the table's `integer`, `short` or `byte` is
written as one, its range checked; `float` as a 4-byte float; a decimal whose
scale and integer digits fit the table's is rescaled to it, and a wider one is
refused; `timestamp_ntz` as a local timestamp. A NULL in a column the table
declares not nullable fails the write naming the column.

Data files are `part-<uuid>.snappy.parquet` (`compression = 'gzip'` or `'none'`
picks another codec), under Hive-style `col=value/` folders for a partitioned
table, escaped as Spark escapes them, NULL as `__HIVE_DEFAULT_PARTITION__`; the
partition columns are not written into the files, as the log carries them. A
partition value is written as the protocol serializes it, a `timestamp` in its
recommended UTC form (`2026-01-01T01:02:03.123456Z`), a `timestamp_ntz` as
`2026-01-01 01:02:03.123456`, a `double` or `float` in plain digits for a
magnitude from 1e-4 to below 1e16 and in exponent form (`1e300`) past it, NaN
and the infinities as `NaN`, `Infinity` and `-Infinity`; an empty string is
NULL, as Delta reads it. Each `add` carries the
file's statistics: `numRecords`, every data column's `nullCount`, and `minValues`
and `maxValues` for numbers, dates, timestamps (cut to the millisecond, as the
protocol allows) and strings (a string over 32 bytes has its 32-byte prefix as
its minimum and no maximum); booleans and binary have none. At most 16 files are
open at once — the least recently written is closed when another partition
needs one — and a file is closed at 128 MiB, so the data written holds memory
near 16 Parquet row groups, and an object store's upload buffer each, however
many partitions there are; a batch's rows are grouped by partition in time and
memory linear in the batch. The commit is built in memory and grows with the
files written, by its `add` line (a few hundred bytes) and path per file. The
file count is what interleaving drives up: rows of more than 16 partitions
interleaved across batches reopen a file per partition per batch, so ordering
the rows by the partition columns keeps one file per partition, and with it the
commit small. No checkpoint is written; a table basalt alone writes is replayed
from its JSON commits.

`UPSERT ON (k1, k2)` merges the rows into the table copy-on-write, in one
commit: a row whose key matches a live row replaces it, a row with a new key is
inserted, and every file holding a matched row is rewritten and removed while
the files holding none are kept as they are. Bare `UPSERT` takes the key from
the source table's primary key, as for a database; with none found it is an
error. The key columns must be the table's and in the rows; without `PARTIAL
COLS` the rows give every column, as for `APPEND`. `PARTIAL COLS (a, b)` lists
the only columns a matched row takes from the incoming one, the rest keeping
the table's values; the rows must carry the key and the listed columns and may
leave out any other column, which an inserted row then holds as NULL (refused
for a column declared not nullable, and for one with a default value,
`CURRENT_DEFAULT`, which basalt does not apply). Keys match as the table stores
them: a key of a 4-byte `float` column is rounded to one before it is matched
or checked for repeats, so `0.1` matches the `0.1` the table holds. Differences from a database's upsert,
where the columns that change are the ones the rows carry and `PARTIAL COLS`
only switches StarRocks and Doris to their partial update: here a column the
rows carry but `PARTIAL COLS` does not list fills inserted rows only, and a
listed key column is refused. As a primary key would, a NULL in a key column
fails the write naming the column, and a key two incoming rows share fails it
naming the key (whatever their order, which parallel lanes do not keep); a live
row whose key is NULL matches nothing and stays. A row may move partition: a
matched row is written where its new values put it. An UPSERT into a folder with
`format = 'delta'` and no table creates it with the rows.

Only the live files that may hold an incoming key are read: those whose
partition value (for a key column that partitions the table) or statistics
(`nullCount`, `minValues`, `maxValues`; a string maximum of 32 bytes or more, as
a writer may cut one, bounds nothing) leave the key's range between the
incoming rows' smallest and largest, column by column, are skipped unopened; a
file without statistics is read. Their key columns are joined against the
incoming keys to find the files holding a match, and only those are read whole,
through a FULL hash join with the incoming rows as its build side. The incoming
rows are spooled to the run's spill space as they arrive, and both joins spill
past `--op-memory` as any join does, so memory stays near `--op-memory` plus a
row group of each file read and the 16 open output files, whatever the table's
or the rows' size. A partitioned table's rewritten and inserted rows are sorted
by partition before they are written (an external sort, also within
`--op-memory`), so each touched partition gets one new file however the join
ordered them. A file with a deletion vector is read without its deleted rows,
so its rewrite drops them, and its `remove` names the deletion vector. A table
recording a change data feed (`delta.enableChangeDataFeed`) is refused, as its
rewritten files would misreport every row as changed. basalt writes no deletion
vectors of its own.

The commit is created only if no writer made that version first: a local one by
hard-linking a fully written, synced hidden file to its name, an object store's
by one PUT with `If-None-Match: *` (a store that ignores the condition is caught
by sending the same create again, and the write fails naming the version it
committed, so the table is not written there again). A version taken by another
writer is read: one that changed the table's metadata or protocol, or created it,
fails the write; so does the removal of a file the write read (by any path form
of it), any change to the table's files under a `REPLACE`, and, under an
`UPSERT`, an added file whose partition values or statistics may hold an
incoming key, as that row would then be in the table twice; an `APPEND`, or an
`UPSERT` whose concurrent writers touched none of its keys, otherwise moves to
the next version, up to ten times. The data files are
written before the commit, so no version names a file not yet there, and locally
each is synced to disk, with the folders holding them, before the commit, and
`_delta_log/` after it, so a commit reported made survives a crash. A run that
fails before or at its commit deletes its local data files, and the folders it
made for them; an object store's are named in the error, as no version
references them and `VACUUM` removes them. A failed commit names the table and
the version it tried; one whose outcome cannot be read back keeps its files and
says so.

A table that asks a writer for what basalt does not do is refused, naming it: a
writer version past 7, a writer feature other than `appendOnly`, `invariants`,
`checkConstraints`, `changeDataFeed`, `generatedColumns`, `columnMapping`,
`identityColumns`, `deletionVectors`, `timestampNtz`, `domainMetadata`,
`vacuumProtocolCheck` and `allowColumnDefaults`, and among those any the table
actually uses — a column invariant, a CHECK constraint, a generated or identity
column, column mapping by name or id (read, but not written: its files name
columns by physical name or field id), a column default an UPSERT's `PARTIAL
COLS` leaves to it — as well as a `REPLACE` or `UPSERT` of a
`delta.appendOnly` table and every reader feature a read refuses. EXPLAIN and
`check` name the table's version and file count, or that the write creates it,
and `check` refuses what it can see of a local table: its features, its columns
and types, its partitioning, an UPSERT's key and `PARTIAL COLS`.

## JSON

`LOAD INTO 'events.ndjson'` (also `.jsonl` and `.json`) writes a JSON object a
row, keyed by column name, one per line; `WITH (array = true)` writes one
top-level array instead, as DuckDB's `COPY ... (ARRAY true)` does. Either reads
back as the same rows, and `--format json` prints a `SELECT` the same way, as
NDJSON. Decimals are written as strings, since most JSON readers hold a number
as a double and would drop digits; a NaN or infinite float is `null`, dates,
times and timestamps ISO text and bytes base64. Text that is not valid UTF-8 is
repaired, each invalid byte U+FFFD. A `.gz` name gzips the file, and it goes to
an object store, SFTP or SMB as a CSV does. `APPEND` adds rows to an NDJSON file
(a newline first if its last line has none); an array is refused, as it closes
on its last byte.

## XML

`FROM 'orders.xml'` reads a row from each child of the document's root, or from
each element `WITH (row = 'Order')` names, wherever it sits. A row's columns are
its attributes and its child elements:

```xml
<orders>
  <order id="1"><customer>Ana</customer><total currency="BRL">10.50</total>
    <items><item sku="A">2</item><item sku="B">1</item></items></order>
</orders>
```

reads as `id`, `customer`, `total`, `total@currency` and `items`. A child holding
only text is a column of it, and its attributes are `child@attr` columns. A child
holding elements is JSON text that `json_get` and `JSON_EACH` take apart, with
`@attr` keys and `#text` for text beside elements; a name that repeats within an
element is an array, and is an array in every row if it repeats anywhere in the
first 1,024. Here `json_get(items, 'item')` is `[{"@sku":"A","#text":"2"}, …]`,
and `WITH (row = 'item')` reads the items themselves as rows. An attribute of the
row named like one of its children is `@attr`. Text directly inside the row,
beside its children, is a column named for the row.

Columns come from the first 1,024 rows, in the order first met; a name first met
later is dropped, as a JSON key is. Values are typed as a CSV's cells are (INT,
FLOAT, DATE, else text, a leading zero keeping a code text), and a later value
its typed column cannot read is an error naming the row, not a NULL; read the
column with `CAST(col AS VARCHAR)` to keep it text. An empty element (`<a/>`)
is the empty string in a text column and NULL in a typed one; an absent element
or `xsi:nil="true"` is NULL. Namespace prefixes are dropped, and names encoded as
SQL Server's `FOR XML` writes them (`Valor_x0020_Total`) are decoded. The
declared encoding is honoured (UTF-8, ISO-8859-1, windows-1252; `WITH (encoding =
'latin1')` overrides a wrong declaration), UTF-16 is refused, and invalid UTF-8 is
repaired. An XML file streams, row by row, on one lane; a folder of `.xml` files
reads as one table, and `.xml.gz`, objects, SFTP and SMB work as for CSV. A
document that ends before its root closes, holds no element or has text outside
its root is an error, never a shorter table, and so is one row past 64 MB or
100,000 elements, as when the root's child is a wrapper and `row` should name the
element inside it. An attribute named like a child element is told apart only if
both appear in the first 1,024 rows; met first later, it reads into the child's
column where the child is absent.

`LOAD INTO 'orders.xml'` writes one `<row>` a line inside a `<rows>` root, a
column an element, as `FOR XML PATH` does; `WITH (root = 'Pedidos', row =
'Pedido')` renames them. A NULL is an element left out, so `<a></a>` stays the
empty string, and a name an element cannot hold is encoded (`Valor Total` as
`Valor_x0020_Total`), so the file reads back as written. Numbers are written as
SQL shows them, a NaN or infinite float as `NaN`, `INF`, `-INF`, dates and times
ISO text and bytes base64; a character XML 1.0 cannot carry (a control other
than tab, LF and CR) is U+FFFD. A `.gz` name gzips the file, and it goes to an
object store, SFTP or SMB as a CSV does; `APPEND` is refused, as the root closes
on the last line.

## Records

`WITH (format = 'records')` writes each row's one value verbatim followed by a
newline, for the fixed-width, bank and EDI files a query composes itself:

```sql
LOAD INTO 'remessa.ret' WITH (format = 'records', newline = 'crlf', encoding = 'latin1')
AS SELECT '1' || rpad(nome, 30, ' ') || lpad(CAST(valor AS STRING), 13, '0') AS rec
FROM 'boletos.csv'
ORDER BY nosso_numero;
```

The query yields exactly one STRING or BYTES column, or the plan is refused. BYTES
are written as they are; text is written in `encoding` (`utf8`, the default,
`latin1` or `cp1252`; it applies to text only), and a character it cannot hold
is an error naming the row. `newline` is `lf` (the default), `crlf`, `cr` or
`none` (records back to back), also spelled `'\n'`, `'\r\n'`, `'\r'` or `''`. A
NULL row is an error naming its row in the output, never skipped or written
empty — a `||` with any NULL is NULL, so `coalesce` the parts that may be. The
format has no extension; it is chosen by `format` alone, and it is written, not
read. A `.gz` name gzips the file, it goes to an object store, SFTP or SMB as a
CSV does, and `APPEND` adds records (the newline first if the file's last record
has none).

## Excel

An Excel workbook — `.xlsx` or `.xlsm`, local, by URL or in object storage — reads
its first worksheet; `WITH (sheet = 'Vendas')` names another (case does not
matter, and a name that is not there is an error listing the ones that are).
The first non-empty row is the header — an empty header cell is named by its
column letter, a repeated one numbered (`total_2`) — or, with `header = false`,
every row is data and the columns are `A`, `B`, …. `range = 'B3:F200'` reads
that block, its first row the header, for a sheet with a title above its table;
`B3:F` reads to the last row.

A workbook's cells carry their own types, so a column's type is decided from
every row, not a sample (a CSV's first 1024): numbers are `INT` when each is an
integer below 2^53 and `FLOAT` otherwise; numbers in a date or time format are
`DATE`, `TIMESTAMP` or `TIME` (to the millisecond, as Excel keeps them; a
1904-based workbook is read as such, and a date before 1 March 1900 reads as
the one Excel shows, though Excel's calendar holds a 29 February 1900 that never
was: that serial reads as 28 February); booleans are `BOOL`; and any text in a column makes it
`STRING`, a number there keeping the digits the file stored — so a "Total" row
of text at the bottom turns its column to text instead of failing the load.
Empty cells and error values (`#N/A`, `#DIV/0!`) are null; a formula reads as
the value Excel last saved, and a merged cell only in its first cell. Rows
stream; what stays in memory is the workbook's shared-string table (up to 512
MB). The old binary `.xls`, `.xlsb` and `.ods` are not read.

### Writing Excel

`LOAD INTO 'report.xlsx' AS ...` (or `WITH (format = 'xlsx')`) writes a workbook
of one worksheet, `Sheet1` unless `WITH (sheet = 'Vendas')` names it: a header row
of column names, then the rows. It streams in constant memory, to a local file
(through a `.part`, as every file target) or to object storage, SFTP and SMB.

```sql
LOAD INTO 'fechamento.xlsx' WITH (sheet = 'Vendas') AS
SELECT loja, dia, SUM(valor) AS total FROM 'vendas.csv' GROUP BY loja, dia;
```

A null is an empty cell; booleans are booleans; numbers are numbers — but Excel
holds a number as a double, so an `INT` beyond 2^53 or a `DECIMAL` of more than
15 significant digits is written as text keeping every digit rather than
rounded, and a NaN or infinite float is an empty cell. `DATE`, `TIMESTAMP` and
`TIME` are Excel dates in the formats `yyyy-mm-dd`, `yyyy-mm-dd hh:mm:ss` and
`hh:mm:ss`, so they read back as dates (in basalt, DuckDB, LibreOffice), kept to
the millisecond (finer digits are cut); a date before 1 March 1900 — where
Excel's calendar holds a day that never was — or after 9999 is written as ISO
text. Text is written as is, line breaks and control characters included
(invalid UTF-8 becomes U+FFFD); `BYTES` is base64 text. An array or struct column
is refused.

Excel's limits are errors, never truncation: 1,048,575 data rows under the
header, 16,384 columns, 32,767 characters a cell, a sheet name of 1 to 31
characters without `[ ] : * ? / \`, and 4 GiB of sheet XML. `APPEND` and a
`.xlsx.gz` name are refused: a workbook is a zip whose directory is written
last. A macro-enabled `.xlsm` is read, never written.

## Arrow IPC

Arrow IPC — `.arrow`, `.feather`, `.ipc` (the file format, Feather v2) or
`.arrows` (the stream format) — is how a dataframe reaches basalt fastest: the
file is memory-mapped and its columns copied out, so polars' `write_ipc` or
pyarrow's `write_feather` hands a frame over with no encode or decode step.
Both formats read, uncompressed or with LZ4-frame or ZSTD buffers, and only
the columns a query uses are converted. Every integer width reads as `int`
but `UInt64`, which reads as the exact `DECIMAL(20,0)`, so a value above
2^63−1 keeps its value; floats read as `float`,
all three string layouts (`utf8`, `large_utf8`, polars' default `utf8_view`)
as `string` and the binary ones as `bytes`, `decimal128` as `decimal`, and
dates, times, timestamps and durations in any unit as basalt's own
(nanoseconds floor to the microsecond; a zoned timestamp is its UTC
wall-clock). Dictionary-encoded columns — a polars `Categorical` — read as
their values. Lists, structs and maps read as JSON text, so `json_get` and
`CROSS JOIN UNNEST(JSON_EACH(col))` reach inside them. A union is refused with the
column named when the query reads it (or a struct holding one); a file with a union
column still reads every other column. Run-end and list-view encodings are refused
with the column named. Only the first stream of a
`.arrows` holding several is read. An Arrow read is one lane; `-j` does not
split it yet.

## Avro

`FROM 'events.avro'` reads an Avro object container file, the format Kafka
Connect, Spark and Hadoop jobs leave behind. A folder of `.avro` files reads as one
table, stdin reads one `WITH (format = 'avro')`, and objects, SFTP, SMB, URLs, a
`.avro.gz` and a zip member work as for CSV. The schema in the file's header fixes
the columns, so nothing is sampled: the top record's fields are the columns, a
nested record's fields read as flat dotted columns (`addr.city`) as a Parquet
struct's do, and a file whose schema is not a record reads as one column, `value`.

```sql
SELECT status, count(*) AS n, sum(amount) AS total
FROM 'orders.avro'
WHERE "addr.city" = 'Lisboa'
GROUP BY status;
```

`boolean` reads as `bool`, `int` and `long` as `int`, `float` and `double` as
`float`, `string` and `enum` as `string`, `bytes` and `fixed` as `bytes`. The
logical types read as basalt's own: `date` as `date`, `time-millis` and
`time-micros` as `time`, `timestamp-*` and `local-timestamp-*` in any unit as
`timestamp` (nanoseconds floor to the microsecond, and a UTC timestamp reads as
its UTC wall-clock), `decimal` on `bytes` or `fixed` as `decimal(p,s)`, `uuid` as
`string`. A decimal wider than 38 digits is refused naming its field rather than
read inexactly. A union of `null` and one type is that type, nullable; a union of
numbers only is `int`, or `float` once a float is among them, and one of strings
and enums is `string`. Arrays, maps, a record that names itself and any other
union read as one `string` column of JSON text, so `json_get` and `JSON_EACH`
reach inside them: bytes as base64, dates and decimals as quoted text. A `null`
branch of any union is a SQL `NULL`, never the text `null`.

Blocks compressed with `null`, `deflate`, `snappy` (its CRC checked) or
`zstandard` are read; `bzip2` and `xz` are refused naming the codec. Each block's
sync marker is checked against the header's, and a block whose bytes do not hold
exactly the objects it claims, a value running past its block, a union branch or
enum symbol out of range, or a decimal with more digits than its precision fails
the read, never a shorter table. A later file of a folder reads into the first
one's columns by name: a column it lacks is `NULL`, one the first lacks is dropped,
and one of another type is widened (`int` into `float` or `decimal`) or refused
naming the column. An Avro read streams block by block on one lane.

`LOAD INTO 'orders.avro'` (or `WITH (format = 'avro')`) writes a container file
whose schema is one record, `row`, with a field per column, each a union of `null`
and its type so that any column may hold `NULL`: `bool` as `boolean`, `int` as
`long`, `float` as `double`, `string` and `bytes` as themselves, `decimal(p,s)` as
`bytes` with the `decimal` logical type, `date` as `date`, `time` as `time-micros`
and `timestamp` as `local-timestamp-micros`, since basalt's timestamps carry no
timezone. A reader older than Avro 1.10's `local-timestamp-micros` (DuckDB's
`avro` extension among them) refuses a file holding a `timestamp` column, and
DuckDB reads a `bytes` decimal as a blob; cast either to text first when DuckDB
reads the file.

```sql
LOAD INTO 'orders.avro' WITH (compression = 'snappy') AS
SELECT id, cliente, CAST(total AS DECIMAL(12,2)) AS total, dia FROM 'orders.csv';
```

Blocks are `deflate`, the one codec besides none every reader must support,
unless `compression` names `snappy` or `none`; zstandard is read but not written.
Each batch is one block, encoded and compressed on its lane, so `-j` spreads the
work and only finished blocks are written in turn. A column name Avro does not
allow (letters, digits and `_`, not starting with a digit, so not a flattened
`addr.city`) and an array or struct column are refused naming the column; rename
one with `AS`. A decimal is written at its column's scale, rounded as Parquet's
writer rounds it, and one with more digits than its column's precision fails the
write naming the column. The file goes to a local path, an object store, SFTP or
SMB; `APPEND` and a `.avro.gz` name are refused, as the header fixes the schema
and the sync marker every block repeats, and the blocks compress themselves.

## Parquet types

A parquet column's type comes from its `LogicalType` annotation when the writer
set one, else from the legacy `ConvertedType`. That matters for files from
polars, DuckDB, Spark or pyarrow: they omit the legacy annotation on a naive
timestamp and on every nanosecond one, so without the logical type those
columns would read as bare `int`. `TIMESTAMP` and `TIME` in milliseconds,
microseconds or nanoseconds all read as basalt's microsecond `timestamp`/`time`;
nanoseconds floor to the microsecond. A UTC-adjusted timestamp reads as its UTC
wall-clock time: basalt has no zoned timestamp, so `isAdjustedToUTC` is not
carried. `DATE`, `DECIMAL`, `STRING`/`ENUM`/`JSON` and `INTEGER` map as their
converted twins do; a `UUID` stays `bytes`. An unsigned integer (`UINT8` to
`UINT64`) reads as its unsigned value: `UINT8` to `UINT32` as `int`, and a
`UINT64` as the exact `DECIMAL(20,0)`, so a value above 2^63−1 keeps its value
rather than fail or wrap, and its statistics still prune row groups.

A struct's fields read as flat dotted columns (`addr.city`). Everything
repeated — a `LIST` of scalars or of structs, a `MAP`, and any nesting of them —
reads as one `string` column named for it, holding each row's value as JSON:
lists as arrays (`[1,2]`, `[]` when empty), structs as objects, maps as objects
keyed by each key's text (`{"k1":3}`), `NULL` for a null column value and
`null` for a null element or field, dates and timestamps as quoted text.
`CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag` gives a row per element and
`json_get` reaches into one. No column of the file is left out, so
`LOAD INTO 'copy.parquet' AS SELECT * FROM 'x.parquet'` carries every one — the
nested ones as JSON text, since that is how basalt holds them. A filter on a
nested column is never used to skip row groups: the file's statistics describe
its leaves, not the JSON.

## Compressed files and archives

A compression suffix is read through: `FROM 'orders.csv.gz'` and `FROM 'orders.csv.zst'`
decompress as they stream, and it is the *inner* name that picks the reader. Both
work over HTTP too. A `.gz` of several gzip members — what `pigz`, `bgzip` and an
append write — reads whole. `.xz` is not supported (std's decoder has the wrong
shape for this reader), nor is bzip2.

`LOAD INTO 'orders.csv.gz'` writes gzip, at about `gzip -6`'s size, and `APPEND`
adds a member to an existing one. Other codecs are refused at plan time: zstd
output is not supported, and Parquet and Arrow compress their own pages
(`WITH (compression = ...)` picks Parquet's codec; a CSV file's comes from its
suffix alone). A `.zip`
is read only: `LOAD INTO 'x.zip'` and `LOAD INTO 'x.zip :: a.csv'` are refused
at plan time too.

A file inside a zip is addressed with `::`, the separator ClickHouse uses for the
same idea:

```sql
SELECT COUNT(*) FROM 'inf_diario_fi_202607.zip :: inf_diario_fi_202607.csv'
  WITH (delimiter = ';');

SELECT * FROM 'cnpj.zip';        -- an archive holding one file needs no `::`
```

An archive holding **more than one** file and no `::` is a plan-time error naming
the candidates, rather than a silent pick of the first — the wrong file read
successfully is worse than no read at all. Stored and deflated members are
supported, which is every zip in practice.

Members stream: nothing is expanded to memory or to a temp file, so a 59 MB CSV in
a 12 MB zip costs the same memory as reading it loose.

Two consequences worth knowing:

- **Neither is parallel.** There is no mapping from a byte offset in a compressed
  stream to a row, so `-j` cannot cut one up — the same reason gzip is not
  splittable under Hadoop. `EXPLAIN` reports `Path: serial` for both, and a
  loose CSV is the faster shape if you have the disk for it.
- **Parquet cannot be read through either.** It needs to seek — footer first, then
  the chunks the query wants — and neither a codec nor an archive member offers
  that. Both are refused at plan time rather than quietly expanded somewhere.

Reading an archive over HTTP is not supported yet: a zip's index sits at the end of
the file, so it needs a ranged fetch before anything else can happen.
