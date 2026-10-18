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
range request, so a projected query transfers only the chunks it decodes; a
server that ignores `Range` falls back to one whole-object fetch. Parquet writes
store `DECIMAL` in the narrowest physical type its precision allows — INT32 to 9
digits, INT64 to 18, FIXED_LEN_BYTE_ARRAY up to 38; past 38 digits (the engine's
own ceiling) a value is refused rather than silently truncated.

Pages are compressed with Snappy unless the sink says otherwise:
`LOAD INTO 'orders.parquet' WITH (compression = 'gzip') AS ...` writes GZIP
pages, smaller than Snappy's for somewhat more CPU (a fast DEFLATE level), and
`compression = 'none'` leaves them uncompressed. `compression` is a Parquet
sink's option only; any other target refuses it at plan time.

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
the columns a query uses are converted. Every integer width reads as `int` (a
`UInt64` above 2^63−1 is an error, never a wraparound), floats as `float`,
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
`UINT64`) reads as its unsigned value, and a `UINT64` above 2^63−1 is an error
naming the column, never a wraparound.

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
