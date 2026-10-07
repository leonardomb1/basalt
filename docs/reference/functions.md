# Scalar functions

Function names are case-insensitive. A function given a null argument answers
null unless its row says otherwise. Aggregates (`COUNT`, `SUM`, `MEDIAN`, …) are
listed under [Queries](../language/queries.md#operators), window functions under
[Window functions](../language/windows.md), the JSON functions on
[their own page](json.md), and `CAST`, `TRY_CAST`, `IF`, `CASE` and the
operators under [Expressions and types](expressions.md).

## Strings

`length()`, `substr()`, `upper()`/`lower()`, the functions in this table and
`LIKE`'s `_` count characters, as Postgres and DuckDB do; `strlen()` counts
bytes. A byte that is not UTF-8 counts as one character, so mis-encoded text
never fails a load.

| function | returns |
|---|---|
| `length(s)` | the number of characters |
| `strlen(s)` | the number of bytes |
| `lower(s)`, `upper(s)` | the text in one case; Latin, Greek and Cyrillic map one character to one (`ß` stays) |
| `trim(s)` | the text without leading and trailing spaces |
| `substr(s, start[, length])` | the characters from `start` (from 1) on, or `length` of them |
| `left(s, n)`, `right(s, n)` | the first or last `n` characters |
| `lpad(s, length[, fill])`, `rpad(s, length[, fill])` | the text padded on the left or right to `length` with `fill` (a space by default); longer text is cut to `length` |
| `replace(s, from, to)` | every `from` replaced by `to` |
| `translate(s, from, to)` | each character of `from` mapped to the one at the same place in `to`, deleted when `to` is shorter: `translate(cnpj, './-', '')` keeps the digits |
| `concat(a, …)` | the values joined; null when any value is (`a \|\| b` is the same) |
| `concat_ws(sep, a, …)` | the values joined with `sep`, skipping nulls |
| `split_part(s, delimiter, n)` | the `n`-th piece (from 1), `''` past the last |
| `strpos(s, sub)` | where `sub` first starts (from 1), `0` when it does not occur |
| `starts_with(s, prefix)`, `ends_with(s, suffix)`, `contains(s, sub)` | whether the text starts with, ends with or holds the other |
| `like(s, pattern)` | `s LIKE pattern` as a call |
| `repeat(s, n)` | the text `n` times over |
| `reverse(s)` | the characters in reverse order |
| `initcap(s)` | each run of letters and digits capitalized |
| `unaccent(s)`, `strip_accents(s)` | the accents of Latin letters dropped: `São` → `Sao`, `Æ` → `AE`, `ß` → `ss` |
| `ascii(s)` | the first character's code point |
| `chr(n)` | the character with code point `n` |

## Regular expressions

The pattern syntax is on [Regular expressions](regex.md). A literal pattern is
compiled at plan time, so a malformed one fails `check`.

| function | returns |
|---|---|
| `regexp_matches(s, pattern)` | whether the pattern matches anywhere; anchor with `^…$` for the whole string |
| `regexp_extract(s, pattern[, group])` | the match, or the numbered group; null where nothing matches |
| `regexp_replace(s, pattern, replacement)` | the text with the first match replaced; `\1`…`\9` in the replacement expand to captured groups and `\0` to the whole match |

## Hashes and encodings

The hashes are over the value's text. For a row's change key, write
`md5(concat_ws('|', a, b, c))`: `concat` would make it null whenever a column is.

| function | returns |
|---|---|
| `md5(s)`, `sha256(s)` | the digest as lowercase hex |
| `xxhash64(s)` | the hash as a BIGINT |
| `to_base64(s)` | the Base64 text |
| `from_base64(s)` | the decoded BYTES; `CAST(… AS STRING)` for text |
| `url_encode(s)`, `url_decode(s)` | percent-encoding as RFC 3986 has it; `+` is left alone |

## Numbers

| function | returns |
|---|---|
| `abs(x)` | the absolute value |
| `floor(x)`, `ceil(x)` | rounded down or up to a whole number |
| `round(x[, digits])` | rounded half away from zero, to `digits` decimal places; deliberately engine-side, never pushed down. On a DECIMAL it is exact, and a literal `digits` answers `DECIMAL(p, digits)` |
| `mod(a, b)` | the remainder, with the dividend's sign: `mod(-7, 3)` is `-1` |
| `power(base, exponent)` | `base` raised to `exponent` |
| `sqrt(x)` | the square root |
| `sign(x)` | `-1`, `0` or `1` |

## Nulls and comparison

| function | returns |
|---|---|
| `coalesce(a, …)` | the first value that is not null (`a ?? b` is the same) |
| `nullif(a, b)` | null when `a = b`, else `a` |
| `greatest(a, b, …)`, `least(a, b, …)` | the largest or smallest value; null arguments are ignored, as in Postgres |

## Dates and times

The units are `year`, `month`, `week`, `day`, `hour`, `minute` and `second`.
`week` is the ISO week: it starts on Monday, `extract` numbers it 1–53 with
week 1 holding the year's first Thursday, and `date_diff` counts the Mondays
crossed. `check` rejects an unknown unit even over a SQL table whose columns it
has not seen.

| function | returns |
|---|---|
| `now()` | the current timestamp |
| `today()` | the current date |
| `date_trunc(unit, ts)` | the time cut to the start of its unit; `date_trunc('week', …)` is that Monday at 00:00 |
| `extract(unit FROM ts)` | the unit's number; `extract(unit, ts)` is accepted too |
| `date_add(unit, n, ts)` | the time moved by `n` units; month and year arithmetic clamps the day of the month (`2026-01-31` plus a month is `2026-02-28`) |
| `date_diff(unit, start, end)` | the number of units from `start` to `end` |
| `make_date(year, month, day)` | the date |
| `epoch(ts)` | seconds since 1970-01-01 00:00:00 |
| `to_timestamp(seconds)` | the timestamp that many seconds after 1970-01-01 00:00:00 |
| `strftime(ts, format)` | the time as text |
| `strptime(text, format)` | the timestamp read out of text: `strptime(dt, '%d/%m/%Y')` |
| `try_strptime(text, format)` | the same, but null where the text does not fit |

The format codes are `%Y %m %d %H %M %S %y %%`. `strptime` takes a number
shorter than its width (`3/1/2026`) and pivots `%y` at 69. Text that does not
fit the format, or a day that does not exist, is an error that stops the
statement, as a failed `CAST` does — one bad date in a file loads nothing;
`try_strptime` makes it null instead and the load goes on.

## Bits

The bitwise operators `& | ^ << >> ~` are under
[Expressions and types](expressions.md).

| function | returns |
|---|---|
| `bit_count(n)` | the number of set bits |
| `to_hex(n)` | the integer as lowercase hex |
| `from_hex(s)` | the integer the hex text spells |
