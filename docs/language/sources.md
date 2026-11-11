# Sources

A query reads from one source, named after `FROM`, and joins may read more
([Joins](joins.md)).

| source | syntax |
|--------|--------|
| SQL table | `FROM erp.dbo.SC5010` |
| SQL table (per-row name) | `FROM erp.dbo.IDENTIFIER($name)` ([dynamic names](for-each.md#dynamic-names--var-identifier-)) — still a table read |
| raw query | `FROM erp.QUERY($$SELECT ...$$)` (no dialect translation) |
| file — CSV, Parquet, Arrow IPC or Excel | `FROM 'path.csv'` / `FROM 'path.parquet'` / `FROM 'path.arrow'` (also `.feather`, `.ipc`, `.arrows`) / `FROM 'path.xlsx'` (also `.xlsm`) — the extension picks the reader; local or HTTPS URL (Arrow IPC: local only). Any other extension is a plan-time error unless `WITH (format = 'csv' \| 'parquet' \| 'arrow' \| 'xlsx' \| 'json' \| 'ndjson' \| 'xml')` names one |
| file — JSON or NDJSON | `FROM 'events.ndjson'` (also `.jsonl`) / `FROM 'rows.json'`; see [JSON and NDJSON](#json-and-ndjson) |
| file — XML | `FROM 'orders.xml'`, `WITH (row = 'Order')` naming the row element; see [XML](formats.md#xml) |
| standard input | `FROM STDIN` — CSV, or `WITH (format = 'json' \| 'ndjson' \| 'xml')`; see [Standard input](#standard-input) |
| compressed file | `FROM 'path.csv.gz'` / `.csv.zst` / `.ndjson.gz` — the inner name picks the reader |
| file inside a zip | `FROM 'archive.zip :: inner.csv'`, or just `FROM 'archive.zip'` when it holds one file |
| folder | `FROM 'sales/'` — a trailing `/`, local or remote, reads every Parquet file under it (subfolders too, as Spark's `year=2026/` layout) as one table, or every `.csv`/`.tsv`/`.txt` when it holds CSVs; see below |
| object storage | `FROM 'az://account/container/path.parquet'` or `FROM 's3://bucket/key.parquet'`; a trailing `/` reads the prefix as a folder |
| SFTP | `FROM 'sftp://bank/retorno/2026-10.csv'` — `bank` a `CREATE CONNECTION bank TYPE sftp` (below), or `sftp://user@host[:port]/path`; any file format, a trailing `/` a folder; `LOAD INTO 'sftp://…'` writes |
| FTP | `FROM 'ftp://ftp.example.org/dados/arquivo.csv'` — anonymous, `ftp://user:pass@host/path`, or `ftp://name/path` through a `CREATE CONNECTION name TYPE ftp`; downloaded once per run, then read like a local file in any format; a trailing `/` a folder; read only ([FTP](../connectors/ftp.md)) |
| Windows share (SMB) | `FROM 'smb://fs/share/2026-10.xlsx'` — `fs` a `CREATE CONNECTION fs TYPE smb` (below), or `smb://[domain;]user@host/share/path`; any file format, a trailing `/` a folder; `LOAD INTO 'smb://…'` writes |
| REST (connection) | `FROM crm.GET('/v1/customers', status = 'open')` — path on the conn's base URL; each `name = value` is a URL-encoded query param (`name = NULL`, or a value that is `NULL`, such as a `PARAM` defaulting to `NULL`, leaves it out), and path and values are expressions (`'/v1/customers/' \|\| $id`). `crm.POST('/search', body = $$...$$)` sends a body. `crm.'/v1/customers'` is the older spelling of a bare GET |
| REST endpoint | `FROM crm.GET('/customers')`, or named once as a table function, `FROM customers()` ([HTTP APIs](../connectors/http.md)) |
| REST (raw URL) | `FROM HTTP('https://host/api/x')` — the URL exactly as written, the way `QUERY()` is raw SQL |
| discovered union | `FROM EACH TABLE OF (...)` ([UNION ALL BY NAME](set-operations.md)) |
| raw records of a file | `FROM lines('app.log')` / `fixed('cnab.ret', 240)` / `split('order.edi', '~')` / `chunks('blob.bin', 65536)` — one row per record as `n`, `offset`, `rec`; see [Raw records](#raw-records) |
| generated integers | `FROM RANGE(10)` / `FROM RANGE(2, 5)` — `lo..hi-1` as a `range` column; bounds are integer constants: literals, params or expressions of them |
| no source | `SELECT 1 AS x, now() AS t;` — a `SELECT` with no `FROM` yields one row of computed values |
| CTE | `FROM <name>` |
| table function | `FROM paid_orders($since) p` — a `CREATE FUNCTION ... RETURNS TABLE` ([user functions](user-functions.md)), also as a `JOIN`'s right side |

Every source takes an alias, with or without `AS`: `FROM 'x.xlsx' AS xl`, `FROM
sr.db.t t`.

## Folders

A folder read lists the folder — subfolders included, names starting `_` or
`.` skipped (`_SUCCESS`, `_temporary/`, `.crc`) — and reads the files sorted
by path. A folder of Parquet files reads as Parquet, of CSVs as CSV, of JSON
files (`.json`, `.ndjson`, `.jsonl`, compressed too) as JSON, other files
ignored; a folder holding two of these kinds is refused unless `WITH (format =
'parquet')`, `'csv'` or `'json'` picks one. Every Parquet file must have the first one's columns, in its
order and of its types, among those the query reads: a file with one missing,
moved or retyped fails the read naming it, rather than putting values under the
wrong name. Types are compared as basalt reads them and are not widened across
files, so a `UINT64` column (`DECIMAL(20,0)`) in one file and an `INT64` one in
another fail the read, whichever sorts first. Each file still skips the columns and row groups the query does
not need. CSV files must repeat the first one's header. JSON files take the
first file's columns, as a REST read's later pages do.

A folder holding a `_delta_log/` is read as the Delta table it is — its live
files, typed by its schema, without the rows its deletion vectors delete and
under the names its column mapping gives, `WITH (version = N)` (or `timestamp =
'2026-11-02 06:00:00'`) for an earlier
version, `WITH (changes_from = N)` (and `changes_to`) for the rows each commit
since changed — not as its files ([Delta Lake](formats.md#delta-lake)). One holding
`metadata/*.metadata.json` is an Iceberg table, which basalt does not read yet; it
is refused rather than read plain, and `WITH (format = 'parquet')` reads its files.

## JSON and NDJSON

A JSON read takes NDJSON (one value per line), a top-level array whose
elements are the rows, or a single object as one row; the first byte decides,
and `WITH (format = 'ndjson')` reads one value per line whatever it starts with.
`WITH (items = 'data.rows')` reads the array at that path of the document
instead, as a REST read's `items` does.

```sql
SELECT user_id, count(*) AS events
FROM 's3://logs/2026/10/events.ndjson.gz'
WHERE json_get(payload, 'kind') = 'purchase'
GROUP BY user_id;
```

Columns come from the first 1,024 rows: their keys, in the order first met, each
typed from the values it took there. Integers are `INT`, any float makes it
`FLOAT`, booleans `BOOL`, and strings, nested objects and arrays, or a mix of
kinds `STRING`, nested values as compact JSON text that `json_get` and
`JSON_EACH` take apart. JSON has no date type, so a date stays text until
`CAST(d AS DATE)`. A key that is `null` in every row read is a `STRING`; when
those rows were the whole input (one file, not a folder) it is of unknown type,
as a CSV column with no value is, so it unifies with any type in a `UNION` or a
join. A later row is read into those columns as `CAST` reads a value: a missing
key is `NULL`, a key not among the columns is dropped, and a value its column
cannot read is `NULL` rather than a guess.

Rows stream: an NDJSON file or an array is never held whole, only `items` reads
its document whole to find the path. A local NDJSON file splits across lanes at
line ends as a CSV does; a JSON array, a compressed or remote file reads on one.
Every location a CSV reads from works the same way: a path, a URL, `s3://`,
`az://`, SFTP, SMB, FTP, a `.gz`/`.zst` suffix, a zip member, a folder. Writing
JSON is in [File formats](formats.md#json).

## Standard input

`FROM STDIN` reads the process's stdin as a CSV, or as JSON, NDJSON or XML with
`WITH (format = 'json' | 'ndjson' | 'xml')`, taking a CSV's `delimiter` and
`encoding`, JSON's `items` or XML's `row`:

```sh
curl -s https://api.example.org/export.ndjson | basalt run -c "
  LOAD INTO wh.events AS SELECT * FROM STDIN WITH (format = 'ndjson');"
```

Stdin must be free for data: a script read from stdin (`basalt run -`), the
REPL and the notebook kernel already use it, and `FROM STDIN` is refused there.
Parquet, Arrow and Excel need random access and are read from a path.

## Raw records

Four table sources read a file's bytes as records rather than parsing a
format, for the files no reader fits — logs, fixed-width bank and mainframe
exports, EDI:

| call | one row per |
|------|-------------|
| `lines(p)` | line; `\n` and `\r\n` both end one (the `\r` is not in `rec`), a last line without one is a row |
| `fixed(p, width [, skip_newlines])` | `width` bytes, back to back; `skip_newlines = TRUE` skips a `\n` or `\r\n` between records. A short last record fails the read naming its offset and length |
| `split(p, terminator)` | record ended by the terminator's bytes (`'~'`, `''''`, any text), which is not in `rec`; a lone line end after the last one is not a record |
| `chunks(p, size)` | block of `size` bytes, the last one shorter |

Each returns `n INT` (the record's number, from 1), `offset INT` (its first
byte in the decoded file, from 0) and `rec BYTES`. The path and sizes are
constants: literals, `$params`, loop variables or expressions of them. Every
location and codec a CSV reads from works — `s3://`, `az://`, SFTP, SMB, a URL,
`.gz`/`.zst`, a zip member — one file at a time, not a folder. The read streams
in constant memory; a line or `split` record past 64 MiB fails naming where it
began, so a binary file is not read as one giant row; `lines('logs')` on a
folder fails saying it is one.

Under `-j`, a local uncompressed file splits across lanes as a CSV does: `fixed`
and `chunks` at record multiples, `lines` and a one-character `split` after a
record's end, each lane numbering its records where the file has them, so `n`,
`offset` and the rows are the ones a single lane reads. A `fixed` file whose
line ends between records are not all alike, a longer `split` terminator, and
every remote or compressed file read on one lane.

```sql
LOAD INTO 'retorno.csv' AS
SELECT n, substr(CAST(rec AS VARCHAR), 1, 3) AS banco, length(rec) AS bytes
FROM fixed('sftp://bank/retorno/2026-10.ret.gz', 240, TRUE)
WHERE n > 1;
```

## SQL sources

A raw `QUERY($$…$$)` is sent as it is and may hold several statements — a
`DELETE` and an `INSERT` before its `SELECT`, a statement after it. The batch runs
to its end: every statement executes, and an error in any of them fails the
read with the server's message. It may return one result set; a second is an
error (`the query returned more than one result set`) rather than dropped or,
if it has the same columns, appended as if it were more rows. A read that stops
early — a `LIMIT`, a row cap — cancels whatever of the batch had not run yet.
MySQL and StarRocks refuse a multi-statement `QUERY` outright.

A SQL Server session is opened as SSMS and the ODBC, JDBC and .NET drivers open
one: `ANSI_WARNINGS`, `ANSI_NULLS`, `ANSI_PADDING`, `ANSI_NULL_DFLT_ON`,
`QUOTED_IDENTIFIER`, `CONCAT_NULL_YIELDS_NULL` and `ARITHABORT` on. So a value too
long for its column is refused (`String or binary data would be truncated`)
rather than cut, a division by zero is an error rather than NULL, a `varchar`
keeps its trailing spaces, a column created without `NULL` / `NOT NULL` allows
nulls, and a table with a filtered index or an indexed view can be written.

### A table's change feed

`WITH (changes_from = <position>)` reads what changed in a SQL Server or
PostgreSQL table since a position, through SQL Server's change data capture or
PostgreSQL's logical decoding, instead of the table: each
row has the table's columns, then `_change_type` (`insert`, `update_postimage`
or `delete`), `_commit_version` (the commit's LSN as 20 hex digits),
`_commit_timestamp` and `_change_seq`, the change's order in the read (commit
order, then order within the commit). These are the names a Delta table's change
feed reads with, so `USING changes` applies either alike, into any table that
takes an UPSERT, `$since` the target's largest `_commit_version`
([Change feeds](load-into.md#change-feeds)):

```sql ignore
LOAD INTO 'lake/orders/' UPSERT ON (id)
  USING changes (sequence_by = _change_seq, delete_when = _change_type = 'delete',
                 position = _commit_version)
AS SELECT * FROM erp.dbo.orders WITH (changes_from = $since);
```

- The read ends where the log stood when the run started (or at `changes_to`),
  so a rerun reads the same changes.
- With no position (`$since` NULL, a feed's first run) the whole table is read
  as inserts at that end. A change made during the read is read again by the next
  run and applied by key, leaving the same rows.
- A position the CDC cleanup job has already passed fails the read rather than
  skip changes; under `USING changes` the table is read whole instead, and the
  keys it no longer holds are kept as deleted. A database or table without CDC
  fails the read, with the statements that enable it. CDC needs SQL Server Agent
  running.
- A table with two capture instances (during a schema change) is read through the
  newest; `capture_instance = '<name>'` picks one.
- An option's value may be a `$param` or `LET`, resolved as the statement runs:
  `changes_from = $since`.

A PostgreSQL table's feed is read by logical decoding, with the same columns,
`_commit_version` the commit's LSN as 16 hex digits:

```sql ignore
LOAD INTO sr.orders UPSERT ON (id)
  USING changes (sequence_by = _change_seq, delete_when = _change_type = 'delete',
                 position = _commit_version)
AS SELECT * FROM shop.public.orders WITH (changes_from = $since, slot = 'basalt_orders');
```

- Setup is the database owner's, as CDC is on SQL Server: `wal_level = logical`, a
  publication carrying the table (`CREATE PUBLICATION basalt_pub FOR TABLE
  orders;`) and a `pgoutput` slot for the feed (`SELECT
  pg_create_logical_replication_slot('basalt_orders', 'pgoutput');`, a role with
  REPLICATION), named by `slot`. The publication is found from the table;
  `publication = '<name>'` picks one when it is in several. A missing one fails the
  read, naming the statement that creates it.
- The changes are peeked through SQL and never consumed; the one thing basalt does
  to the source is move the slot up to the position its target holds, at the next
  run's start, so the server can drop the WAL before it. Give each feed its own
  slot: two targets on one slot move it past each other's positions.
- The read ends where the WAL stood when the statement started, so a rerun reads
  the same changes; `changes_to` is not taken. A run decodes its range into memory
  once, so it suits runs frequent enough to keep that range small.
- With no position the whole table is read as inserts at that end, as above.
- Under `USING changes` the table is also read whole, and the keys it no longer
  holds kept as deleted, when the slot was invalidated or moved past the position
  (a slot dropped and created again), when the table was truncated in the range,
  and when an update left a large (TOASTed) value unchanged on a table that does
  not log whole rows: logical decoding sends no value for it then.
  `ALTER TABLE orders REPLICA IDENTITY FULL;` avoids that, and also makes a delete
  carry the whole row; without it a deleted row arrives with its key alone, the
  other columns NULL.
- The change feed is read as a statement's FROM, or the CTE it starts with, not
  as a joined table.

## Source clauses

These follow the source, in any order. The rules for what a SQL source is asked
to do, and the deprecated `PUSHDOWN`, are on [Pushdown](pushdown.md).

- **`PAGINATE BY page|offset|cursor (param = 'page', size = 100,
  total = 'count', field = 'next', start = 2, max = 50)`** — REST pagination.
  Friendly keys map to the engine hints (`param`→`page_param`/`cursor_param`,
  `size`→`page_size`, `total`→`total_field`, `total_rows`→`total_rows_field`,
  `field`→`cursor_field`,
  `start`→`start_page`, `max`→`max_pages`); any other key is a REST option's
  own name. A
  cursor that is a full `http(s)://` URL is fetched as given; any other is
  URL-encoded into the cursor parameter.
- **`RETRY n [ON (404, 501)]`** — retries + extra retryable statuses. 408, 429
  and every 5xx except 501, 505 and 511 are retried without being listed; `ON`
  adds codes an API misuses for an outage.
- **`WITH (delimiter = ';', encoding = 'latin1')`** — the CSV dialect. The
  delimiter is one character, or the word `tab`; the encodings are `utf8`
  (default), `latin1` / `iso-8859-1`, and `cp1252` / `windows-1252`. Non-UTF-8
  input is decoded to UTF-8 as it is read, so everything downstream — comparisons,
  `length()`, a parquet sink — sees proper text. The five bytes cp1252 leaves
  unassigned (0x81, 0x8D, 0x8F, 0x90, 0x9D) read as the C1 controls U+0081 …
  U+009D, as Windows' code page 1252 best-fit table, the WHATWG Encoding standard
  and MySQL read them, so no byte is lost and the
  text writes back to the same bytes. A local file read with a dialect
  still fans out over byte-range chunks under `-j`, each lane decoding its chunk
  in that dialect. Multi-byte encodings are not supported: they would break that
  chunking. Read as `utf8`, a cell that is not valid UTF-8 fails the read, naming
  the file, line and column. `latin1` and `cp1252` decode every byte, so a file
  whose bytes are not what you claimed does not fail there, it just yields
  mojibake: prefer the publisher's stated encoding over guessing. The delimiter
  is also accepted on a `LOAD INTO` file target; `encoding` is not — a CSV sink
  always writes UTF-8, and being told otherwise is an error rather than ignored.
- **`WITH (format = 'csv' | 'parquet' | 'arrow' | 'xlsx')`** — read or write a
  path as this format whatever its extension says. Needed
  for a file named `.dat` or `.txt`, and for a URL that serves CSV from an
  extensionless path. Without it, an extension basalt does not know is refused
  at plan time rather than parsed as CSV, which would read a binary file's
  bytes as rows.
- **`WITH (k = v, flag, ...)`** — every other option of a read, listed in full
  below.

## WITH options

A read's `WITH (...)` takes `key = value` pairs and bare flags. `PAGINATE BY` and
`RETRY` (above) are shorthands that set some of the REST ones. A key the source
does not read — misspelled, or one of another kind of source — is a plan-time
error rather than ignored, and so is a REST option's value the read cannot use: a
method, body type or paging mode that is none of the names below, text where a
number goes, a flag that is neither `true` nor `false`, a body on a GET. Names are
matched in any case; a value holding a loop's `${...}` is checked once filled in.

**Files**

| option | meaning |
|---|---|
| `format` | `csv`, `parquet`, `arrow`, `xlsx`, `json` or `ndjson` (`jsonl`), whatever the extension says |
| `delimiter` | a CSV's separator: one character, or `tab` (also on a `LOAD INTO` file) |
| `encoding` | a CSV's text encoding: `utf8` (default), `latin1` / `iso-8859-1`, `cp1252` / `windows-1252` |
| `sheet` | an Excel worksheet by name (default the first) |
| `range` | an Excel block such as `'B3:F200'`, or `'B3:F'` to the last row |
| `header` | `false` reads an Excel sheet's first row as data, the columns named `A`, `B`, … |
| `buffer` | a flag: drain the source fully before opening the sink |

**REST sources** — `HTTP(...)`, `conn.GET(...)` and `conn.POST(...)`

| option | default | meaning |
|---|---|---|
| `items` | the response | dotted path to the array of rows when the response (or a JSON file) nests it, as `'data.rows'` |
| `method` | `get` | `get`, `post`, `put`, `patch` or `delete` |
| `body` | none | the request body, sent by `post`, `put` or `patch` (on another method it is an error); when paginating, the page values go into it (see `paging_in`) |
| `body_type` | `form` | `form` sends the body as `application/x-www-form-urlencoded`, `json` as `application/json` |
| `header` | none | an extra header, `'Name: value'`; repeat it for more (up to 16), a later one replacing an earlier of the same name, so a read's overrides its connection's. A `User-Agent` or `Accept` given this way replaces basalt's |
| `bearer`, `bearer_env` | none | a bearer token, or the environment variable holding one |
| `auth`, `auth_env` | none | an `Authorization` value sent verbatim, or the variable holding it |
| `user_env`, `pass_env` | none | variables holding a basic-auth user and password |
| `paginate` | none | `page`, `offset` or `cursor` (`PAGINATE BY`) |
| `paging_in` | the body when the method sends one, else the query string | where the page number, offset, size and a cursor go: `query` (the URL's query string) or `body`. In a JSON body each is that key of the object, a dotted `page_param` such as `'paging.page'` a nested one; a key already in the body keeps its kind, so `"page": "1"` goes on as `"page": "2"`, and a missing one is added as a number. In a form body each is appended as `&name=value`, the value URL-encoded |
| `page_param` | `page` | the parameter, or JSON key, that carries the page number or offset (`param` in `PAGINATE`) |
| `start_page` | `1` | the first page number (`start`) |
| `start_offset` | `0` | the first offset, in `offset` mode |
| `size_param` | none | the parameter, or JSON key, that carries the page size, as OData's `$top` |
| `page_size` | `100` | rows per page, sent in `size_param` and the step of `offset` mode (`size`) |
| `cursor_param` | `cursor` | the parameter, or JSON key, that carries a cursor that is not a URL (`param` in cursor mode) |
| `cursor_field` | `next` | dotted path to the next cursor or URL in each response (`field`) |
| `total_field` | none | dotted path to a page count in the first response, for APIs that never return an empty page (`total`); a number, or a numeric string as `"12"` |
| `total_rows_field` | none | dotted path to a row count instead: the pages are that count over the first page's rows, so a server that sends fewer per page than `page_size` asks is still read to the end (`total_rows`). A read takes one of the two; a value that is no whole number fails it |
| `max_pages` | `10000` | a cap on pages fetched (`max`) |
| `stop_short` | off | a flag (bare, or `= true` / `= false`): stop after a page shorter than `page_size` |
| `prefetch` | `1` | pages kept in flight at once, in `page` and `offset` modes |
| `retries` | `2` | retries of a transient failure (`RETRY n`) |
| `batch`, `concurrency` | none, `4` | of a table function called per row only: rows per request, and requests in flight ([HTTP APIs](../connectors/http.md#calling-a-function-per-row)) |
| `retry_statuses` | none | extra HTTP codes treated as transient, as `'404,501'`, on top of 408, 429 and every 5xx but 501, 505 and 511 (`RETRY n ON (...)`) |
| `retry_base_ms` | `500` | the first retry's back-off, doubled each time, ±30% jitter |
| `timeout_ms` | `300000` | the limit on one page's fetch or one login, retries included |
| `progress_ms` | `30000` | how often a long fetch logs its progress |

A value the API wants computed — a token hashed from a password and the date, a
date window — is a LET, put into any option's text with `${...}`. The holes are
filled in once, before the first request, from the script's PARAMs and LETs (a
LET may read `env()`, see [Parameters](parameters.md)), and the option is then
checked as filled in, so a `page_size = '${n}'` that comes out `0` fails before any
request:

```sql
LET token = sha256(env('API_PASS') || strftime(now(), '%d/%m/%Y'));
SELECT * FROM HTTP('https://api.example.com/v1/items')
  PAGINATE BY page
  WITH (method = 'post', body_type = 'json', body = '{"user": "etl", "token": "${token}"}',
        header = 'X-Token: ${token}');
```

A connection's options take the same, and a `$name` alone ([Connections](connections.md)).

A page or login reply that decodes past 256 MiB stops the read (`HttpBodyTooLarge`),
so a compressed bomb cannot exhaust memory; ask the endpoint for smaller pages.

**Joins** — on the `JOIN` clause: `max_build = '16GB'` sets the right side's
memory ceiling, past which the join spills to disk ([Joins](joins.md));
`join_order = 'written'` (default `'auto'`) keeps a join's right side in memory
and its place in a chain, whatever the size estimates say.

`WHERE` on a REST source runs in basalt after the fetch; on a SQL table it is
pushdown. Same word, different plan — `EXPLAIN` shows which.

## Reading a REST API

A complete REST read, for orientation:

```sql
CREATE CONNECTION crates TYPE http OPTIONS (base_url = 'https://crates.io/api/v1')
  RETRY 2;

SELECT id AS crate, downloads, json_get(links, 'owners') AS owners
FROM crates.GET('/crates', sort = 'downloads', per_page = 100)
  PAGINATE BY page (param = 'page', size = 100, total = 'meta.total', max = 5)
  WITH (items = 'crates')
WHERE downloads > 0;
```

The rows are the array at `items` (or the response itself); a single object —
a detail endpoint's answer — is one row. Columns are typed as a JSON file's are
([JSON and NDJSON](#json-and-ndjson)), from the first page's rows: numbers,
booleans and strings as themselves, nested objects and arrays as JSON text. A
later row's value is read into its column as `CAST` would read it, and one that
cannot be — `"yes"` in a `BOOL` column, `1e300` in an `INT` — is `NULL` rather
than a guess. `json_get` and `JSON_EACH` ([JSON functions](../reference/json.md), and `UNNEST` in [Queries](queries.md)) take
the nested JSON text apart.
