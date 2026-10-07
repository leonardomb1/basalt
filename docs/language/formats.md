# File formats

A path's extension picks its reader and writer — `.csv`, `.parquet`, Arrow IPC
and Excel — unless `WITH (format = ...)` names one ([Sources](sources.md#source-clauses)).

## CSV

A header name in double quotes loses its quotes (`""` inside is one quote) and
may hold the delimiter: `"Valor Total"` is the column `Valor Total`.

A CSV column's type is sniffed from the first 1024 rows: int ⊂ float ⊂ string,
and a column whose every non-empty, unquoted cell is an ISO `YYYY-MM-DD` reads as
a `DATE` — so `WHERE day >= '2026-01-01'`, `date_add`, `date_diff` and `EXTRACT`
work on it directly, and a parquet sink stores it as a date. A string function
on it (`substr(day, 1, 7)`, `day LIKE '2026%'`) still sees the ISO text. A cell
past the sample that does not parse as the inferred type is an error, never a
silent coercion. Timestamps are not sniffed; `CAST(ts AS TIMESTAMP)` as before.

## Parquet

Parquet reads use column projection, row-group skipping from statistics, and
ranged reads — only the footer and the chunks a query needs are fetched. A
remote `.parquet` (`https://...`, `az://...`, `s3://...`) is read the same way, by HTTP
range request, so a projected query transfers only the chunks it decodes; a
server that ignores `Range` falls back to one whole-object fetch. Parquet writes
store `DECIMAL` in the narrowest physical type its precision allows — INT32 to 9
digits, INT64 to 18, FIXED_LEN_BYTE_ARRAY up to 38; past 38 digits (the engine's
own ceiling) a value is refused rather than silently truncated.

## Excel

An Excel workbook — `.xlsx` or `.xlsm`, local, by URL or in object storage — reads
its first worksheet; `WITH (sheet = 'Vendas')` names another (case does not
matter, and a name that is not there is an error listing the ones that are).
The first non-empty row is the header — an empty header cell is named by its
column letter, a repeated one numbered (`total_2`) — or, with `header = false`,
every row is data and the columns are `A`, `B`, …. `range = 'B3:F200'` reads
that block, its first row the header, for a sheet with a title above its table;
`B3:F` reads to the last row. Excel is read, never written.

A workbook's cells carry their own types, so a column's type is decided from
every row, not a sample (a CSV's first 1024): numbers are `INT` when each is an
integer below 2^53 and `FLOAT` otherwise; numbers in a date or time format are
`DATE`, `TIMESTAMP` or `TIME` (to the millisecond, as Excel keeps them; a
1904-based workbook is read as such, and serials before 1 March 1900 are a day
late, as in Excel); booleans are `BOOL`; and any text in a column makes it
`STRING`, a number there keeping the digits the file stored — so a "Total" row
of text at the bottom turns its column to text instead of failing the load.
Empty cells and error values (`#N/A`, `#DIV/0!`) are null; a formula reads as
the value Excel last saved, and a merged cell only in its first cell. Rows
stream; what stays in memory is the workbook's shared-string table (up to 512
MB). The old binary `.xls`, `.xlsb` and `.ods` are not read.

## Arrow IPC

Arrow IPC — `.arrow`, `.feather`, `.ipc` (the file format, Feather v2) or
`.arrows` (the stream format) — is how a dataframe reaches basalt fastest: the
file is memory-mapped and its columns copied out, so polars' `write_ipc` or
pyarrow's `write_feather` hands a frame over with no encode or decode step.
Both formats read, uncompressed or with LZ4-frame or ZSTD buffers, and only
the columns a query uses are converted. Every integer width reads as `int` (a
`UInt64` above 2^63−1 is an error, never a wraparound), floats as `float`,
all three string layouts (`utf8`, `large_utf8`, polars' default `utf8_view`)
as `string` and the binary ones as `bytes`, `decimal128` as `decimal`, and
dates, times, timestamps and durations in any unit as basalt's own
(nanoseconds floor to the microsecond; a zoned timestamp is its UTC
wall-clock). Dictionary-encoded columns — a polars `Categorical` — read as
their values. Lists, structs and maps read as JSON text, so `json_get` and
`CROSS JOIN UNNEST(JSON_EACH(col))` reach inside them. Unions and run-end or list-view
encodings are refused with the column named. Only the first stream of a
`.arrows` holding several is read. An Arrow read is one lane; `-j` does not
split it yet.

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
converted twins do; a `UUID` stays `bytes`.

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
output is not supported, and Parquet and Arrow compress their own pages.

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
  splittable under Hadoop. `EXPLAIN` reports `physical: serial` for both, and a
  loose CSV is the faster shape if you have the disk for it.
- **Parquet cannot be read through either.** It needs to seek — footer first, then
  the chunks the query wants — and neither a codec nor an archive member offers
  that. Both are refused at plan time rather than quietly expanded somewhere.

Reading an archive over HTTP is not supported yet: a zip's index sits at the end of
the file, so it needs a ranged fetch before anything else can happen.
