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
both counts, never padded with nulls or cut short. A cell that is not valid UTF-8
is an error naming the file, the line, the column and the first bad byte, never
repaired, as DuckDB's reader refuses it: read a file that is not UTF-8 `WITH
(encoding = 'latin1')` or `'cp1252'`, which decode every byte. A header name that
is not UTF-8 is repaired (each invalid byte U+FFFD), so it cannot hide the rest.

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
SELECT count(*) AS n_monday FROM 's3://lake/sales/' WITH (timestamp = '2026-11-02 06:00:00');
SELECT id, amt, _change_type, _commit_version FROM 's3://lake/sales/' WITH (changes_from = 41);
```

The log is replayed as the protocol says: the newest complete checkpoint at or
below the version read (one Parquet file, or every part of a multi-part one),
then each JSON commit after it, an `add` putting a file in the table and a
`remove` taking it out. A file is the one its path names, relative or absolute,
together with its deletion vector, so a commit that replaces a file's deletion
vector reads the same whatever order its lines are in. `WITH (version = 12)` reads the table as commit 12 left
it, as long as the log still holds what that takes; an older version, or a
commit missing from `_delta_log/`, is refused naming the versions that can be
read. `WITH (timestamp = '2026-11-02 06:00:00')` reads the table as it stood
then (UTC; a date alone is its midnight): the newest version committed at or
before that time, by each commit's `commitInfo` (`inCommitTimestamp` where the
table records one, else `timestamp`), found by halves among the readable
commits. A time before the oldest readable commit or after the newest is
refused naming that commit's version and time, as Spark refuses it, and
`version` and `timestamp` together are refused. `_last_checkpoint` is not
needed, since the log's listing is read.

`WITH (changes_from = 41)` reads the table's change data feed instead: the rows
each commit from version 41 on changed, `changes_to = 45` ending the range (it
defaults to the newest version), either a version or a time, a range by time
starting at the first commit at or after its start and ending at the last at or
before its end. Each row has the table's columns, then `_change_type`
(`insert`, `delete`, `update_preimage` or `update_postimage`), `_commit_version`
and `_commit_timestamp` (the commit's time as `timestamp` reads it); a filter on
the last two or on a partition column skips whole files. A commit that wrote
change data files (`cdc` actions, under `_change_data/`) is read from those
alone; any other commit's added files read as inserts and its removed files as
deletes, every row each held less what its deletion vector already deleted, and
a file it removes and adds again with a new deletion vector as the rows that
vector newly deletes (or no longer deletes, as inserts). Files added or removed
without changing data (`dataChange` false, as OPTIMIZE writes) hold no change.
The rows are read in the table's schema at the range's end, older files fitted to
it by name. A range is refused where `delta.enableChangeDataFeed` is not `true`
at its start or a `metaData` inside it turns it off, where a commit in it is no
longer in `_delta_log/`, where it starts after it ends or past the newest
version, and where the table has a column of one of the three names;
`changes_from` goes with neither `version` nor `timestamp`.

The schema is the table's (`metaData.schemaString`): a column a file lacks reads
as NULL, as one added after the file was written does, and a file whose column
is of another type fails the read naming the file. A partition column's value
comes from the log, as the protocol asks, whether or not the file holds it too;
an empty or null value is NULL. A filter on a partition column skips the files
whose value it rules out without opening them, and so does a comparison (`=`,
`<`, `<=`, `>`, `>=`, `BETWEEN`, joined by `AND`) on any other column or a
struct's field (`WHERE s.id > 5`), by each file's statistics in the log: a file
whose `minValues` and `maxValues` put every row outside the comparison, or whose
`nullCount` says the column is NULL in every row, is never opened. Statistics
keyed by physical name under column mapping are read as such; a file without
statistics for the column is read, and a string cut to its prefix or a timestamp
kept to the millisecond bounds as loosely as the protocol allows. Everything a
Parquet folder does still applies: lanes under `-j`, projection, row-group statistics, a join's
keys, ranged reads of remote files. Delta types map as Parquet's do — `long`,
`integer`, `short` and `byte` to `INT`, `float` and `double` to `FLOAT`,
`timestamp` and `timestamp_ntz` to `TIMESTAMP`, and a struct, an array or a map
to one nested column of that type (`STRUCT`, `LIST`, `MAP`). A file written before
a struct gained a field reads that field as NULL, and a field the table dropped
is not read.

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
name; a list's or map's struct fields take the table's names too. A
renamed column reads under its new name, a dropped one is not read, a column a
file lacks reads as NULL, and a column dropped and added again under its old
name reads NULL from files written before, as their data is the old column's.
In `id` mode a file without field ids fails the read naming it, rather than read
as all NULLs.

Under type widening (`typeWidening`) a file written before a column widened
reads in the table's wider type: an integer as a decimal or a double, a decimal
at a larger scale, a date as a timestamp, struct fields and list and map parts
alike. A `variant` column (`variantType`) reads as its JSON text, a STRING:
objects with their keys in order, a decimal as its exact digits, dates,
timestamps, times and UUIDs as quoted text, binary as quoted base64, a NaN or
infinite float as `null`, each as basalt's JSON writes its own values;
nanosecond timestamps keep their microseconds. A variant nested inside a struct, list or map, a shredded one
(`variantShredding`) and a file whose variant column is not the struct of binary
`value` and `metadata` the protocol defines are refused.

A table using a reader feature basalt does not honour is refused, naming it,
rather than read into wrong rows: a catalog-managed table,
`variantShredding`, a reader version past 3, and any feature basalt does not
know. `deletionVectors`, `columnMapping`, `timestampNtz`, `vacuumProtocolCheck`,
`typeWidening`, `variantType` and `v2Checkpoint` read as described; a V2 checkpoint, named by a UUID and written
as JSON or Parquet, is read like a classic one, its `add`s and `remove`s
inline or in the sidecar files under `_delta_log/_sidecars/` it names, of
which only those two kinds of action are taken. A live file missing from storage (a
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
`boolean`, `string`, `binary` and `date`, every column nullable; a `STRUCT`,
`LIST` or `MAP` as Delta's `struct`, `array` and `map` of those types, every
field, element and value nullable. A `TIME` (at any depth) or an untyped NULL
column is refused, to be cast first, and a list, map or struct cannot partition
a table.
`WITH (partition_by = 'c1, c2')` partitions it; a later write takes the table's
own partition columns, and naming different ones is refused. A write to an
existing table must give exactly its columns — by name, in any order; one
missing or one more is refused, as the schema is not changed unless the write
asks (below) — each of the
table's type: an integer column of the table's `integer`, `short` or `byte` is
written as one, its range checked; `float` as a 4-byte float; a decimal whose
scale and integer digits fit the table's is rescaled to it, and a wider one is
refused; `timestamp_ntz` as a local timestamp. A struct, array or map column
takes rows of its shape — the same field names in order, leaves of the same
kinds, a decimal leaf the table's holds — its leaves written as the table types
them, so a Spark table's `struct<id: integer>` gets a 4-byte `id`, and a value
past it fails the write. A NULL in a column the table declares not nullable
fails the write naming the column.

`WITH (merge_schema = true)` lets an `APPEND` or `UPSERT` change the schema, as
Spark's `mergeSchema` does: a column the rows have and the table lacks is added
to the table, nullable, at the end, and a struct field likewise to its struct at
any depth (inside a list's element or a map's value too, the rows' struct
giving its fields in any order); a table column the rows lack is written as
NULL — refused for one declared not nullable, and for a partition column, which
says where a row goes. Under an UPSERT a matched row is replaced whole, so a
column the rows lack becomes NULL there too unless `PARTIAL COLS` keeps it. The
commit carries the new schema in a `metaData` action that keeps the table's id,
properties and creation time. A column's type is never changed: one of another
type is refused as without the option. `WITH (overwrite_schema = true)` on a
`REPLACE` gives the table the rows' schema whole, as Spark's `overwriteSchema`
does, partitioned by `partition_by` when given, else by the table's partition
columns when the rows still have them, else not at all; earlier versions keep
their schema for `WITH (version = N)`. The two options are exclusive.

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
its minimum and no maximum); booleans and binary have none. A struct's fields
have theirs nested under its name (`"minValues":{"s":{"id":1}}`), as Spark
writes them; a list or a map has none. At most 16 files are
open at once — the least recently written is closed when another partition
needs one — and a file is closed at 128 MiB, so the data written holds memory
near 16 Parquet row groups, and an object store's upload buffer each, however
many partitions there are; a batch's rows are grouped by partition in time and
memory linear in the batch. The commit is built in memory and grows with the
files written, by its `add` line (a few hundred bytes) and path per file. The
file count is what interleaving drives up: rows of more than 16 partitions
interleaved across batches reopen a file per partition per batch, so ordering
the rows by the partition columns keeps one file per partition, and with it the
commit small.

A commit landing at every tenth version (or every `delta.checkpointInterval`th,
when the table sets it) is followed by a checkpoint of that version,
`_delta_log/<version>.checkpoint.parquet`, and `_last_checkpoint` pointing at it,
so a reader replays from there rather than from version 0. The checkpoint is the
table's state as the protocol lays it out, one action a row: the protocol, the
metadata, each application's last `txn`, each live `domainMetadata`, every live
file's `add` whole (its statistics, tags and deletion vector as the log had
them, whoever wrote it) and each `remove` still within
`delta.deletedFileRetentionDuration` (a week by default), with Spark's column
types, so Spark, delta-rs and DuckDB read it. It is written under a hidden name
and renamed into place; one that cannot be written is a warning, never the
write's failure, as the commit it follows has landed. A newer `_last_checkpoint`
another writer left is not moved back. After a checkpoint the log is cleaned as
Spark cleans it: of the checkpoints, the newest whose commit is older than
`delta.logRetentionDuration` (30 days by default) is the oldest version kept,
and the commits, checkpoints and checksum files below it are deleted, oldest
first, so time travel reaches back that far and no further.
`delta.enableExpiredLogCleanup = false` keeps every log file. A commit's age is
its `commitInfo` timestamp, so a log copied elsewhere keeps its ages. Data files
are never deleted here; a removed file stays until `VACUUM`.

Two built-in procedures maintain a table, run with `CALL` (a FUNCTION of the
same name, if the script declares one, is called instead):

```sql ignore
CALL delta_optimize('s3://lake/orders/');                  -- every partition
CALL delta_optimize('s3://lake/orders/', 'region = ''eu''');
CALL delta_vacuum('s3://lake/orders/');                    -- the table's retention
CALL delta_vacuum('s3://lake/orders/', 336);               -- or a longer one, in hours
```

`delta_optimize` compacts small files: of the live files a predicate on the
partition columns keeps (every file without one), those under the writer's
128 MiB roll in a partition holding two or more of them, or one with a deletion
vector, are read and written again into as few files as the roll allows, their
deleted rows dropped. The commit is an `OPTIMIZE` whose actions all say
`dataChange: false`, as no row changes; it fails as a conflict only when a
concurrent writer removed a file it read, so appends go on beside it. A
predicate naming another column is refused. The files it replaced stay on disk,
as earlier versions read them, until `delta_vacuum` deletes them.

`delta_vacuum` deletes the files no version within the retention reads: every
file under the table's folder but the newest version's data files and deletion
vectors, and those a `remove` still within the retention names, once it is older
than the retention — `delta.deletedFileRetentionDuration`, a week when unset, or
the longer number of hours given. A shorter retention than the table's is
refused, as Spark refuses it: a concurrent writer's files not yet committed are
as new as that. The log and every path with a part starting `_` or `.` are never
touched, but for the change data files under `_change_data/`, which age out as
data files do; nothing is committed; versions older than the retention may then
no longer be readable. A file's age is its modification time (an object's
`Last-Modified`).

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

`USING changes (...)` makes an UPSERT apply a change feed, the position it has
read to kept in the table itself, so each run applies exactly what is new:

```sql ignore
LOAD INTO 's3://lake/silver/orders/' UPSERT ON (order_id)
  USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn, name = 'orders_cdc')
AS SELECT * FROM 'landing/orders_changes/' WHERE lsn > coalesce($since, 0);
```

Each argument is an expression over the query's columns. Of a key's rows, the
one of the highest `sequence_by` is the one applied, as the latest change; two
of a key at one sequence fail the run, as which applies would otherwise depend
on the run. The winner upserts its key, or, when `delete_when` holds for it,
deletes the key's live row (a delete of a key the table lacks does nothing).
The commit records the rows' largest `position` as the table's position for
`name` (the target's path when unset) — Delta's `txn` action, which Spark and
delta-rs keep for idempotent writes — in the same commit as the rows, so the
rows and the position land together or not at all. `$since` is that position
for the statement's query, NULL until a run records one, so `WHERE lsn > $since`
reads only what is new and pushes down to the source like any `$param`; a run
with nothing new commits nothing. A position behind the one the table holds
fails the run, as the query then read changes already applied, and a writer
committing a position for the same `name` concurrently fails it as a conflict,
so a rerun after either applies from where the table stands. The position is a
number Delta keeps, so `position` takes an INT, DATE or TIMESTAMP. The columns
the arguments read are not written to a table that lacks them, so a feed's
`op`, `seq` and `lsn` stay out of a table the write creates; a column wanted in
the table as well is given twice (`SELECT *, at AS changed_at` with `position =
changed_at`). `PARTIAL COLS` applies to the updates; a delete removes the whole
row. A checkpoint carries the positions, as Delta's checkpoints keep `txn`.

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
so its rewrite drops them, and its `remove` names the deletion vector. basalt
writes no deletion vectors of its own.

On a table recording a change data feed (`delta.enableChangeDataFeed`) an UPSERT
that rewrites files also writes what it changed to change data files under
`_change_data/` (partitioned as the table is and sorted with the rewritten rows,
so each touched partition gets one, each a `cdc` action of the commit), so a reader of the feed does not take every row of a rewritten file as
deleted and inserted again: a matched row's old values as `update_preimage` and
its new ones as `update_postimage` (under `PARTIAL COLS`, the columns it leaves
as they were), an inserted row as `insert`, and a row a `USING changes` delete
removes as `delete`. An UPSERT that rewrites no file, an append and a `REPLACE`
write none, as their added and removed files say what changed, and
`delta_optimize` none either, as it changes no row.

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

Databricks' writer features are honoured as the protocol asks:

- **Column mapping** (`name` or `id`): files are written under each column's
  physical name, nested struct fields too, with its id as the Parquet field id;
  statistics, partition values and partition folders use physical names; a
  column or struct field `merge_schema` adds, and every one under
  `overwrite_schema`, gets a `col-<uuid>` name and the next id, the commit
  raising `delta.columnMapping.maxColumnId`.
- **In-commit timestamps** (`delta.enableInCommitTimestamps`): each commit's
  `commitInfo`, its first action, carries one past the newest commit's, raised
  past a concurrent writer's when the commit moves to a later version; time
  travel by timestamp reads them.
- **Liquid clustering** (`clustering`): rows are written unclustered, as the
  protocol lets a writer that does not cluster, its `delta.clustering` domain
  kept; `delta_optimize` leaves a clustered table to an engine that clusters.
- **V2 checkpoints** (`v2Checkpoint`): basalt's checkpoint of such a table
  carries the `checkpointMetadata` row, under the classic name the protocol
  allows.
- **Row tracking** (`rowTracking`): each new file is numbered on from the
  table's row-id high-water mark (`baseRowId`, its `defaultRowCommitVersion` the
  commit's version), the mark raised in the `delta.rowTracking` domain and
  renumbered past a concurrent writer's when the commit moves to a later
  version. Where `delta.enableRowTracking` holds, a row an UPSERT or
  `delta_optimize` copies keeps its row id and commit version in the table's
  materialized columns (an updated row keeps its id and takes the new version,
  an inserted one is numbered afresh); a table that enables it without naming
  those columns refuses such a rewrite.
- A widened column (`typeWidening`) takes rows of its wider type; a `variant`
  column takes JSON text, each value written as Variant binary (integers in the
  narrowest width, a number with a fraction as a decimal when its digits fit,
  else a double) with no statistics, and a row whose text is not JSON fails the
  write. basalt does not create variant columns: a STRING creates a `string`.

A table that asks a writer for what basalt does not do is refused, naming it: a
writer version past 7, a writer feature other than `appendOnly`, `invariants`,
`checkConstraints`, `changeDataFeed`, `generatedColumns`, `columnMapping`,
`identityColumns`, `deletionVectors`, `timestampNtz`, `domainMetadata`,
`vacuumProtocolCheck`, `allowColumnDefaults`, `inCommitTimestamp`, `clustering`,
`v2Checkpoint`, `rowTracking`, `typeWidening` and `variantType`, and among those any the
table actually uses — a column invariant, a CHECK constraint, a generated or
identity column, a column default an UPSERT's `PARTIAL COLS` leaves to it — as
well as a `REPLACE` or `UPSERT` of a
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
'latin1')` overrides a wrong declaration), UTF-16 is refused, and text or an
attribute value that is not valid UTF-8 is an error naming the file, the row and
the element or attribute, as a CSV cell is (a name is repaired). An XML file streams, row by row, on one lane; a folder of `.xml` files
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
(invalid UTF-8 becomes U+FFFD); `BYTES` is base64 text. A list, map or struct column
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
wall-clock; a count of seconds or milliseconds past the ±292,277 years a
microsecond timestamp holds is a corrupt file, never clamped). Dictionary-encoded columns — a polars `Categorical` — read as
their values. A list (`list`, `large_list` or `fixed_size_list`), a struct and a
map read as one nested column of that type (`LIST(...)`, `STRUCT(...)`,
`MAP(...)`), their children copied out as columns, so `s.city`, `l[1]`,
`m['k']` and `CROSS JOIN UNNEST(l)` reach inside them as they do a Parquet
struct's, and a query naming only `s.city` converts only that child. A union is refused with the
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
nested record reads as one `STRUCT` column, an array as a `LIST` and a map as a
`MAP` keyed by `string`, as a Parquet file's do, and a file whose schema is not a
record reads as one column, `value`.

```sql
SELECT status, count(*) AS n, sum(amount) AS total
FROM 'orders.avro'
WHERE addr.city = 'Lisboa'
GROUP BY status;
```

`boolean` reads as `bool`, `int` and `long` as `int`, `float` and `double` as
`float`, `string` and `enum` as `string`, `bytes` and `fixed` as `bytes`. The
logical types read as basalt's own: `date` as `date`, `time-millis` and
`time-micros` as `time`, `timestamp-*` and `local-timestamp-*` in any unit as
`timestamp` (nanoseconds floor to the microsecond, a UTC timestamp reads as
its UTC wall-clock, and milliseconds past the ±292,277 years a microsecond
timestamp holds are a corrupt file, never clamped), `decimal` on `bytes` or `fixed` as `decimal(p,s)`, `uuid` as
`string`. A decimal wider than 38 digits is refused naming its field rather than
read inexactly. A union of `null` and one type is that type, nullable; a union of
numbers only is `int`, or `float` once a float is among them, and one of strings
and enums is `string`; inside a record, array or map these read as they do at
the top. A record that names itself (directly or through its parts) and any
other union read as one `string` column of JSON text, so `json_get` and
`JSON_EACH` reach inside them: bytes as base64, dates and decimals as quoted
text. A `null` branch of any union is a SQL `NULL`, never the text `null`.

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
allow (letters, digits and `_`, not starting with a digit, so not a dotted
`addr.city`) is refused naming the column; rename it with `AS`. A struct is
written as a record (`r1`, `r2`, … in order), a list as an array and a map as a
map, each part a union of `null` and its type as a column is; Avro's map keys
are strings, so a map keyed by anything else is refused, to be cast first. A decimal is written at its column's scale, rounded as Parquet's
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
nanoseconds floor to the microsecond. A millisecond or legacy `INT96` value past
the ±292,277 years a microsecond timestamp holds is a corrupt page, an error,
never clamped to the last instant, as DuckDB refuses it too.
A UTC-adjusted timestamp reads as its UTC
wall-clock time: basalt has no zoned timestamp, so `isAdjustedToUTC` is not
carried. `DATE`, `DECIMAL`, `STRING`/`ENUM`/`JSON` and `INTEGER` map as their
converted twins do; a `UUID` stays `bytes`. An unsigned integer (`UINT8` to
`UINT64`) reads as its unsigned value: `UINT8` to `UINT32` as `int`, and a
`UINT64` as the exact `DECIMAL(20,0)`, so a value above 2^63−1 keeps its value
rather than fail or wrap, and its statistics still prune row groups.

Each top-level field of a file is one column. A struct, a `LIST` (three-level,
or the legacy two-level and bare repeated forms), a `MAP`, and any nesting of
them read as one nested column of that type: `STRUCT(...)`, `LIST(...)`,
`MAP(K, V)`, as `DESCRIBE` shows, a field OPTIONAL in the file nullable, a
REQUIRED one not. They are assembled from the leaves' repetition and definition
levels into columns of their own, nothing parsed or rendered, and shown or
written as text as JSON: lists as arrays (`[1,2]`, `[]` when empty), structs as
objects, maps as objects keyed by each key's text (`{"k1":3}`), `NULL` for a
null column value and `null` for a null element or field, dates and timestamps
as quoted text, a NaN or infinite float as `null`, and a binary leaf or map key
(`BYTE_ARRAY` with no `STRING` annotation, a `UUID`) as base64, as the JSON
writer spells `BYTES`, so no byte is lost. A struct's fields are not separate
dotted columns (`"addr.city"`): `addr.city` reads the field, `tags[1]` an
element, `m['k']` a map's value and `CROSS JOIN UNNEST(tags)` a row per element
(see [Expressions](../reference/expressions.md)); `CAST(addr AS STRING)` is its
JSON, for the JSON functions.
`LOAD INTO 'copy.parquet' AS SELECT * FROM 'x.parquet'` carries every column,
the nested ones as the spec's three-level `LIST`, its `MAP` and nested groups,
each leaf encoded as a flat column of its type is, with repetition and
definition levels; a `CAST(... AS STRUCT(...))`, `LIST(...)` or `MAP(K, V)` (see
[Expressions](../reference/expressions.md)) makes one from JSON text. A query
that reads only some of a struct's fields (`doc.id`, `doc.addr.zip`) decodes
only theirs. A filter on a struct's field (`WHERE doc.id > 5`) skips row groups
by that field's statistics, as a flat column's does, unless a list lies above
it (a list's statistics describe its elements, not a row); a filter on a nested
value as a whole never does. Files basalt writes declare their statistics' sort
order, which Arrow-based readers (pyarrow, DuckDB, Polars) require before they
use a min or max.

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
