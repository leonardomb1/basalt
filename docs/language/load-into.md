# LOAD INTO

```sql ignore
LOAD INTO sr.silver.pedidos            -- conn[.schema].table, or a quoted path
  USING stream_load                    -- load path; optional, starrocks/doris only
  UPSERT ON (empresa, num_pedido)      -- disposition (below)
  SPLIT BY (num_pedido) JOBS 4         -- key-range parallel load
  WITH (label_prefix = 'noturno')      -- overrides the connection's label_prefix
AS
<query>;
```

- File target by quoted path — the extension picks the writer:
  `LOAD INTO '/out/x.csv'`, `LOAD INTO '/out/x.parquet'`,
  `LOAD INTO '/out/x.arrow'` (Arrow IPC file; also `.feather`, `.ipc`, and
  `.arrows` for the stream format, local paths only), or an object-store
  path `LOAD INTO 'az://account/container/bronze/x.parquet'` or
  `LOAD INTO 's3://bucket/bronze/x.parquet'`.
- `USING stream_load` names the one load path StarRocks and Doris have, and
  may be left out; on any other target, or with another name, it is an error.
- A per-row dynamic target uses `IDENTIFIER(<string-expr>)` over loop vars
  ([FOR EACH](for-each.md)): `LOAD INTO sr.IDENTIFIER('crm_' || lower($name)) ...`. A file target
  built this way must end in a literal extension, which picks the writer.
- `LOAD INTO '/out/x.xlsx'` writes an Excel workbook of one sheet, `WITH (sheet =
  'Name')` naming it ([Writing Excel](formats.md#writing-excel)); `REPLACE` only.
- `LOAD INTO '/out/x.avro'` writes an Avro object container file, each column a
  nullable field ([Avro](formats.md#avro)); `REPLACE` only.
- A folder target (`LOAD INTO 's3://lake/sales/'`, ending in `/`) is a Delta
  table, written as a table target: `APPEND` by default, `REPLACE`, or `UPSERT`
  (copy-on-write, in one commit); `WITH (format = 'delta')` creates it when the
  folder holds no `_delta_log/`, and `partition_by = 'c1, c2'` partitions the
  table it creates ([Writing Delta](formats.md#writing-delta), where `PARTIAL
  COLS` and repeated or NULL keys are spelled out).
- `UPSERT` needs a table to merge into; on a file target it is a plan-time error.
- `WITH (...)` on a `LOAD INTO` takes `delimiter` (or `delim`) and `format`
  (a file), `sheet` (an Excel file), `compression` (a Parquet file or a Delta
  table's data files: `snappy`, `gzip` or `none`; an Avro file's blocks:
  `deflate`, `snappy` or `none`), `partition_by` (a Delta table
  a write creates), `array` (a JSON file: one top-level array instead of a row
  a line), `root` and `row` (an XML file's element names), `newline` and
  `encoding` (a `format = 'records'` file, [Records](formats.md#records)) and
  `label_prefix` (StarRocks, Doris), `cutover` (a REPLACE into PostgreSQL, MySQL
  or SQL Server); a key the target does not read, or any
  other key, is a plan-time error rather than ignored.
- Dispositions on a **table target**: `APPEND` (default, omissible) · `REPLACE`
  (overwrite) · `UPSERT ON (k1, k2)` · `UPSERT ON (id) PARTIAL COLS (a, b)` ·
  bare `UPSERT` (infer the PK from the source table's metadata at plan time —
  needs a table read on a SQL source that exposes it). An empty/unresolved
  upsert key is an error, never a silent no-op.
- `REPLACE` into PostgreSQL, MySQL or SQL Server loads a staging table
  (`_basalt_stage_<table>`, in the target's schema) from every lane, then in one
  transaction clears the target and copies the staged rows in, and drops the
  staging table. The target stays the same table — grants, views, foreign keys,
  triggers and indexes stay — readers see the old rows until the commit, and a
  failed load leaves the target as it was. `WITH (cutover = 'delete')` clears
  with `DELETE` instead of the default `TRUNCATE`, for a login without the right
  to truncate or a deferrable foreign key; MySQL always deletes, as its
  `TRUNCATE` commits on its own. Running two REPLACEs into one target at once is
  the caller's to prevent: they share the staging table.
- Dispositions on a **file target** — the omissible default is *not* `APPEND`:
  a bare `LOAD INTO 'x.csv'`, like `REPLACE`, replaces the file, so a rerun
  replaces it. A local file is written whole: the rows go to `x.csv.part`,
  renamed over `x.csv` once the write completes and deleted if it fails or is
  interrupted, so a reader never sees a torn file and a failed `LOAD` leaves the
  previous one in place. The `.part` takes an existing file's permission bits
  and, where the user may set them, its owner and group; ACLs and extended
  attributes are not carried over. A target that is a device, a FIFO or a
  symlink, has more than one hard link (so every name sees the new rows), or
  sits in a directory where `x.csv.part` cannot be created is instead truncated
  and written in place, keeping its inode, links and attributes but not the
  guarantee: a reader may see it half written, and a failed `LOAD` leaves it
  truncated. Explicit `APPEND` accumulates for CSV and NDJSON only (an NDJSON
  file gets a newline first when its last line has none, and `WITH (array =
  true)` is refused): a CSV file is
  opened without truncating and the header row is written only when it was
  absent or empty (a `.csv.gz` whose content is empty counts as empty). A file
  that already has a header must have this load's columns — the same names, in
  the same order — or the load fails naming both headers before writing a byte;
  appending `b, a` under `a, b` would file every value under the wrong column. A
  file whose last line has no newline gets one first, so the first new row never
  joins the last old one. Explicit `APPEND` is a plan-time error for `.parquet`,
  `.xml` (its root closes on the last line) and Arrow IPC, whether named by extension or by `WITH (format = ...)` (the footer
  indexes every row group or batch and is written last,
  so appending means rewriting the file), for `az://` / `s3://` (an object is
  replaced on write, never extended), and for `sftp://` / `smb://` (the file is
  written beside the target and renamed over it) — use `REPLACE`, or a per-run
  path.
- `SPLIT BY (col)` parallelizes a load from a SQL table (or a `UNION` of
  them) by ranges of `col`; `JOBS n` fixes the lane count (otherwise the CLI
  `-j` applies). A file source fans out on its own, so `SPLIT BY` over one is
  a plan-time error, as is `JOBS 0`.

**stdout is not syntax**: a terminal `SELECT ...;` statement prints the result
as an aligned table — `basalt run -c "SELECT * FROM 'x.csv'"` works as a
mini-DuckDB.
