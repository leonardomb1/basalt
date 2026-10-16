# Sources

A query reads from one source, named after `FROM`, and joins may read more
([Joins](joins.md)).

| source | syntax |
|--------|--------|
| SQL table | `FROM erp.dbo.SC5010` |
| SQL table (per-row name) | `FROM erp.dbo.IDENTIFIER($name)` ([dynamic names](for-each.md#dynamic-names--var-identifier-)) — still a table read |
| raw query | `FROM erp.QUERY($$SELECT ...$$)` (no dialect translation) |
| file — CSV, Parquet, Arrow IPC or Excel | `FROM 'path.csv'` / `FROM 'path.parquet'` / `FROM 'path.arrow'` (also `.feather`, `.ipc`, `.arrows`) / `FROM 'path.xlsx'` (also `.xlsm`) — the extension picks the reader; local or HTTPS URL (Arrow IPC: local only). Any other extension is a plan-time error unless `WITH (format = 'csv' \| 'parquet' \| 'arrow' \| 'xlsx' \| 'json' \| 'ndjson')` names one |
| file — JSON or NDJSON | `FROM 'events.ndjson'` (also `.jsonl`) / `FROM 'rows.json'` — read only; see [JSON and NDJSON](#json-and-ndjson) |
| standard input | `FROM STDIN` — CSV, or `WITH (format = 'json' \| 'ndjson')`; see [Standard input](#standard-input) |
| compressed file | `FROM 'path.csv.gz'` / `.csv.zst` / `.ndjson.gz` — the inner name picks the reader |
| file inside a zip | `FROM 'archive.zip :: inner.csv'`, or just `FROM 'archive.zip'` when it holds one file |
| folder | `FROM 'sales/'` — a trailing `/`, local or remote, reads every Parquet file under it (subfolders too, as Spark's `year=2026/` layout) as one table, or every `.csv`/`.tsv`/`.txt` when it holds CSVs; see below |
| object storage | `FROM 'az://account/container/path.parquet'` or `FROM 's3://bucket/key.parquet'`; a trailing `/` reads the prefix as a folder |
| SFTP | `FROM 'sftp://bank/retorno/2026-10.csv'` — `bank` a `CREATE CONNECTION bank TYPE sftp` (below), or `sftp://user@host[:port]/path`; any file format, a trailing `/` a folder; `LOAD INTO 'sftp://…'` writes |
| FTP | `FROM 'ftp://ftp.example.org/dados/arquivo.csv'` — anonymous, `ftp://user:pass@host/path`, or `ftp://name/path` through a `CREATE CONNECTION name TYPE ftp`; downloaded once per run, then read like a local file in any format; a trailing `/` a folder; read only ([FTP](../connectors/ftp.md)) |
| Windows share (SMB) | `FROM 'smb://fs/share/2026-10.xlsx'` — `fs` a `CREATE CONNECTION fs TYPE smb` (below), or `smb://[domain;]user@host/share/path`; any file format, a trailing `/` a folder; `LOAD INTO 'smb://…'` writes |
| REST (connection) | `FROM crm.GET('/v1/customers', status = 'open')` — path on the conn's base URL; each `name = value` is a URL-encoded query param (`name = NULL`, or a value that is `NULL`, such as a `PARAM` defaulting to `NULL`, leaves it out), and path and values are expressions (`'/v1/customers/' \|\| $id`). `crm.POST('/search', body = $$...$$)` sends a body. `crm.'/v1/customers'` is the older spelling of a bare GET |
| REST resource | `FROM crm.customers` — an endpoint named with `CREATE RESOURCE` ([HTTP APIs](../connectors/http.md)) |
| REST (raw URL) | `FROM HTTP('https://host/api/x')` — the URL exactly as written, the way `QUERY()` is raw SQL |
| discovered union | `FROM EACH TABLE OF (...)` ([UNION ALL BY NAME](set-operations.md)) |
| generated integers | `FROM RANGE(10)` / `FROM RANGE(2, 5)` — `lo..hi-1` as a `range` column; bounds are int literals or params |
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
wrong name. Each file still skips the columns and row groups the query does
not need. CSV files must repeat the first one's header. JSON files take the
first file's columns, as a REST read's later pages do.

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
`CAST(d AS DATE)`. A later row is read into those columns as `CAST` reads a
value: a missing key is `NULL`, a key not among the columns is dropped, and a
value its column cannot read is `NULL` rather than a guess.

Rows stream: an NDJSON file or an array is never held whole, only `items` reads
its document whole to find the path. A local NDJSON file splits across lanes at
line ends as a CSV does; a JSON array, a compressed or remote file reads on one.
Every location a CSV reads from works the same way: a path, a URL, `s3://`,
`az://`, SFTP, SMB, FTP, a `.gz`/`.zst` suffix, a zip member, a folder. basalt
reads JSON but does not write it; `--format json` prints a `SELECT` as NDJSON.

## Standard input

`FROM STDIN` reads the process's stdin as a CSV, or as JSON or NDJSON with
`WITH (format = 'json' | 'ndjson')`, taking a CSV's `delimiter` and `encoding`
or JSON's `items`:

```sh
curl -s https://api.example.org/export.ndjson | basalt run -c "
  LOAD INTO wh.events AS SELECT * FROM STDIN WITH (format = 'ndjson');"
```

Stdin must be free for data: a script read from stdin (`basalt run -`), the
REPL and the notebook kernel already use it, and `FROM STDIN` is refused there.
Parquet, Arrow and Excel need random access and are read from a path.

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

## Source clauses

These follow the source, in any order. `PUSHDOWN` and the rules for what a SQL
source is asked to do are on [Pushdown](pushdown.md).

- **`PAGINATE BY page|offset|cursor (param = 'page', size = 100,
  total = 'count', field = 'next', start = 2, max = 50)`** — REST pagination.
  Friendly keys map to the engine hints (`param`→`page_param`/`cursor_param`,
  `size`→`page_size`, `total`→`total_field`, `field`→`cursor_field`,
  `start`→`start_page`, `max`→`max_pages`); any other key is a REST option's
  own name. A
  cursor that is a full `http(s)://` URL is fetched as given; any other is
  URL-encoded into the cursor parameter.
- **`RETRY n [ON (429, 503)]`** — retries + retryable statuses.
- **`WITH (delimiter = ';', encoding = 'latin1')`** — the CSV dialect. The
  delimiter is one character, or the word `tab`; the encodings are `utf8`
  (default), `latin1` / `iso-8859-1`, and `cp1252` / `windows-1252`. Non-UTF-8
  input is decoded to UTF-8 as it is read, so everything downstream — comparisons,
  `length()`, a parquet sink — sees proper text. A local file read with a dialect
  still fans out over byte-range chunks under `-j`, each lane decoding its chunk
  in that dialect. Multi-byte encodings are not supported: they would break that
  chunking. A file whose bytes are not what you claimed does not fail, it just yields
  mojibake, so prefer the publisher's stated encoding over guessing. The delimiter
  is also accepted on a `LOAD INTO` file target; `encoding` is not — a CSV sink
  always writes UTF-8, and being told otherwise is an error rather than ignored.
- **`WITH (format = 'csv' | 'parquet' | 'arrow' | 'xlsx')`** — read or write a
  path as this format whatever its extension says (`xlsx` reads only). Needed
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
error rather than ignored.

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

**REST sources** — `HTTP(...)`, `conn.GET(...)`, `conn.POST(...)` and resources

| option | default | meaning |
|---|---|---|
| `items` | the response | dotted path to the array of rows when the response (or a JSON file) nests it, as `'data.rows'` |
| `method` | `get` | `post` sends `body` |
| `body` | none | the request body; the page parameter is added to it when paginating a POST |
| `body_type` | `form` | `json` sends the body as `application/json`, anything else as a form |
| `header` | none | one extra header, `'Name: value'`; a `User-Agent` given this way replaces basalt's |
| `bearer`, `bearer_env` | none | a bearer token, or the environment variable holding one |
| `auth`, `auth_env` | none | an `Authorization` value sent verbatim, or the variable holding it |
| `user_env`, `pass_env` | none | variables holding a basic-auth user and password |
| `paginate` | none | `page`, `offset` or `cursor` (`PAGINATE BY`) |
| `page_param` | `page` | the query parameter that carries the page number or offset (`param` in `PAGINATE`) |
| `start_page` | `1` | the first page number (`start`) |
| `start_offset` | `0` | the first offset, in `offset` mode |
| `size_param` | none | the query parameter that carries the page size, as OData's `$top` |
| `page_size` | `100` | rows per page, sent in `size_param` and the step of `offset` mode (`size`) |
| `cursor_param` | `cursor` | the query parameter that carries a cursor (`param` in cursor mode) |
| `cursor_field` | `next` | dotted path to the next cursor or URL in each response (`field`) |
| `total_field` | none | dotted path to a page count, for APIs that never return an empty page (`total`) |
| `max_pages` | `10000` | a cap on pages fetched (`max`) |
| `stop_short` | off | a flag: stop after a page shorter than `page_size` |
| `prefetch` | `1` | pages kept in flight at once, in `page` and `offset` modes |
| `retries` | `2` | retries of a transient failure (`RETRY n`) |
| `retry_statuses` | none | extra HTTP codes treated as transient, as `'404,408'` (`RETRY n ON (...)`) |
| `retry_base_ms` | `500` | the first retry's back-off, doubled each time, ±30% jitter |
| `timeout_ms` | `300000` | the limit on one page's fetch |
| `progress_ms` | `30000` | how often a long fetch logs its progress |

**Joins** — on the `JOIN` clause: `max_build = '16GB'` sets the right side's
memory ceiling, past which the join spills to disk ([Joins](joins.md));
`join_order = 'written'` (default `'auto'`) keeps an inner join's right side in
memory and its place in a chain, whatever the size estimates say.

`WHERE` on a REST source runs in basalt after the fetch; on a SQL table it is
pushdown. Same word, different plan — `EXPLAIN` shows which.

## Reading a REST API

A complete REST read, for orientation:

```sql
CREATE CONNECTION crates TYPE http OPTIONS (base_url = 'https://crates.io/api/v1')
  RETRY 2 ON (429, 503);

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
