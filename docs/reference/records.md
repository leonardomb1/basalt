# Record functions

Functions over raw bytes, for reading and writing record files a format library
describes in SQL: fixed-width bank and tax files, mainframe extracts, binary
layouts. A record is `BYTES`. Decoders cut a field out of it and type it;
encoders turn a value back into a field's bytes, and `||` joins fields into a
record.

Byte positions count from **0**: a bank layout's "posição 063–070" is
`pos = 62, len = 8`. (`substr` on text keeps counting characters from 1, as SQL
does.) A field outside the record, or one that does not read as its kind, is an
error naming the function and the field's bytes, never a guess; a field of only
spaces decodes as `NULL`.

## Bytes and charsets

| Function | Returns |
|---|---|
| `x'0D0A'` | a `BYTES` literal, in hex |
| `byte_len(b)` | its length in bytes |
| `byte_slice(b, pos, len)` | `BYTES` from `pos`, `len` long |
| `byte_at(b, pos)` | one byte as an `INT` 0–255 |
| `byte_find(b, needle[, from])` | where `needle` starts, from 0, or `NULL` |
| `hex(b)` / `unhex(t)` | `BYTES` as hex text, and back |
| `decode(b, charset)` / `encode(t, charset)` | `BYTES` in a charset as text, and back |
| `a \|\| b` | `BYTES` when either side is `BYTES` |

Charsets are `utf8`, `latin1` (`iso-8859-1`), `cp1252` (`windows-1252`) and
`cp037` (`ebcdic`). cp1252's five unassigned bytes (0x81, 0x8D, 0x8F, 0x90, 0x9D)
decode to the C1 controls of the same number and encode back, so every byte of a
single-byte charset survives `encode(decode(b, cs), cs)`. `decode` and `text_at`
refuse bytes read as `utf8` that are not valid UTF-8, naming the first bad byte,
rather than repair them; `encode` refuses a character the charset cannot hold.

## Reading a file by range

| Function | Returns |
|---|---|
| `read_bytes(path, offset, len)` | `len` bytes of the file from byte `offset` (from 0), as `BYTES` |
| `file_size(path)` | the file's size in bytes |

The path is any file basalt reads: local, `https://`, `s3://`, `az://`, SFTP or
SMB, read by range requests, so only the bytes asked for are fetched. A range
past the end of the file is an error naming it. A `LET` over them is read when
the script runs (`LET header = read_bytes($file, 0, 32)`); `basalt check` reads
nothing, so it types such a `LET` without reading it. For a whole file of
records, `fixed()`, `lines()`, `split()` and `chunks()` stream it instead
([Sources](../language/sources.md)).

## Decoding fields

| Function | Returns |
|---|---|
| `text_at(rec, pos, len[, charset])` | text, trailing spaces trimmed |
| `int_at(rec, pos, len)` | an `INT`: digits with an optional sign, spaces around it allowed, leading zeros fine |
| `decimal_at(rec, pos, len, scale[, separator])` | `DECIMAL(len, scale)`: with no separator the last `scale` digits are the places (`0000012345` → `123.45`); with `','` or `'.'`, that is the decimal point and the other a grouping mark (`1.234,50`) |
| `date_at(rec, pos, len, pattern[, null marker])` | a `DATE`; `time_at` and `timestamp_at` likewise |
| `zoned_at(rec, pos, len, scale)` | a zoned decimal, ASCII overpunch (`0001234}`) or EBCDIC zones |
| `packed_at(rec, pos, len, scale)` | a packed (COMP-3) decimal |
| `u8_at(rec, pos)`, `u16le_at`, `i32be_at`, … `u64be_at` | binary integers, little or big endian |
| `f32le_at(rec, pos)` … `f64be_at` | binary floats |

A pattern uses `YYYY`, `YY`, `MM`, `DD`, `hh`, `mm`, `ss` and literal punctuation
(`'DDMMYY'`, `'YYYY-MM-DD'`, `'DD/MM/YYYY hh:mm:ss'`), and must be as long as the
field. `YY` reads 69–99 as 19xx and 00–68 as 20xx. A field equal to the null
marker (`'000000'`) is `NULL`, as an all-blank one is.

## Encoding fields

| Function | Returns `BYTES` |
|---|---|
| `fmt_text(t, width[, charset])` | padded with spaces; too long is an error |
| `fmt_int(n, width)` | zero-padded, `-` first when negative |
| `fmt_decimal(d, width, scale[, separator])` | implied places, or a separator |
| `fmt_date(d, pattern)`, `fmt_time`, `fmt_timestamp` | in the pattern; `YY` only for 1969–2068 |
| `fmt_zoned(d, width, scale)`, `fmt_packed(d, width, scale)` | mainframe numbers |
| `fmt_u32le(n)` … `fmt_f64be(x)` | binary numbers; out of range is an error |

A `NULL` is written as spaces (binary encoders return `NULL`), and a value is
never rounded: a decimal with more places than its field, or too wide for it, is
an error.

```sql
SELECT int_at(x'3030313233', 0, 5) AS n,
       decimal_at('0000012345', 0, 10, 2) AS amount,
       date_at('150325', 0, 6, 'DDMMYY') AS due,
       fmt_int(42, 6) || fmt_text('ACME', 10) || fmt_date(today(), 'DDMMYY') AS rec;
```
