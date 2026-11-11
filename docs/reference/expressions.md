# Expressions and types

SQL-ish, Pratt-parsed. Precedence (high→low): unary `- NOT ~` → `* / %` →
`+ - ||` → `<< >>` → `&` → `^` → `|` → comparisons
`= == != <> < <= > >= LIKE IN IS` → `??` → `AND` → `OR`.

**Nulls** follow SQL's three-valued logic: a comparison with a null is null,
which a `WHERE`, an `ON` or a `CASE WHEN` treats as false — so a null join key
never matches, under any join kind, and `x NOT IN (1, NULL)` keeps no row. `AND`
and `OR` are null only when the other side does not decide them (`false AND
null` is false, `true OR null` true).

**Scope rule:** `$name` is script/environment scope — a PARAM, a LET, or (in a
`FOR EACH ROW OF` / statement-function body) a loop variable, resolved at plan
time. Bare names are row/local scope — columns, `LET … IN` bindings, aliases —
and a PARAM, LET or loop variable never stands in for one: with `PARAM region`,
`WHERE region = 'West'` filters on the column and `WHERE region = $region` on
the parameter. Among `$` names the innermost binding wins: loop var >
LET/PARAM. A `$name` that nothing binds is a plan-time error naming it — never
the column it happens to spell — and `check` reports it even over a table whose
columns it has not seen.

- `$name` — see the scope rule above. `$job.a?.b` navigates a JSON param.
- Bitwise (INT only, engine-side — never pushed down): `& | ^ << >>`, unary
  `~`. `^` is xor. `>>` is arithmetic; shift counts `< 0` or `>= 64` yield 0
  (`-1` for `>>` of a negative). Companions: `bit_count() to_hex() from_hex()`.
- `a || b` — string concat (ANSI), sugar for `concat(a, b)`.
- `IDENTIFIER(<string-expr>)` — treat a computed string as a name ([dynamic names](../language/for-each.md#dynamic-names--var-identifier-)): a table
  or path in `FROM`/`LOAD INTO`, an upsert key, or a column wherever a column
  may be named.
- `CASE` expression, both forms:
  `CASE status WHEN 'paid', 'ok' THEN 'done' ELSE 'open' END` ·
  `CASE WHEN amount >= 1000 THEN 'gold' WHEN amount >= 100 THEN 'silver' ELSE 'std' END`
- `IF(c, a, b)` kept as sugar.
- `x IS [NOT] NULL` · `x IS [NOT] EMPTY` (true when null **or** `''`; string
  operands only — handy for loop values).
- `a IS [NOT] DISTINCT FROM b` — the null-safe `<>` (`=`): two nulls are not
  distinct, a null and a value are, and the answer is never null.
  `ON l.k IS NOT DISTINCT FROM r.k` pairs null keys too (as a join without a
  key). The operands compare as `=` would, a quoted literal included; it pushes
  down to PostgreSQL as written and to MySQL, StarRocks and Doris as `<=>`, and
  stays in the engine on SQL Server.
- `a ?? b` — null-coalesce (sugar for `COALESCE`).
- **Numeric literals** are typed as the standard types them: `42` is an `INT`
  (one past INT's range, up to 38 digits, is an exact `DECIMAL(p, 0)`); a
  literal with a point and no exponent — `2.5`, `.5`, `10.00` — is an exact
  `DECIMAL(p, s)` with the digits as written (`10.00` is `DECIMAL(4,2)`, `0.05`
  is `DECIMAL(2,2)`); one with an exponent — `1e3`, `2.5E-1` — is a `FLOAT`
  (PostgreSQL reads it as `numeric` too; basalt follows the standard's
  approximate literal). So `0.1 + 0.2 = 0.3` is true, `x * 0.5` on an INT
  column is a `DECIMAL`, and `0e0` (or `CAST(0 AS DOUBLE)`) is the way to write
  a float zero.
- `CAST(x AS INT)` / `CAST(x AS DECIMAL(18,2))` / `CAST(x AS DATE)` /
  `CAST(x AS TIMESTAMP)` — implicit widening is int→decimal→float only: an int
  beside a decimal is a decimal, a decimal beside a float a float, as
  PostgreSQL resolves `numeric` beside `float8` (`coalesce(f, 0.0)` over a
  FLOAT `f` is a FLOAT). Text
  parses as `YYYY-MM-DD[ HH:MM:SS]`, and a day the month does not have
  (`'2023-02-29'`, `'2024-04-31'`) fails rather than rolling into the next month.
  The year is what a date prints as: four to seven digits and `-` before year 0
  (`'-0044-03-15'` is 45 BC, `'12345-01-01'` past 9999), so every date reads
  back from its own text; one past what a `DATE` holds fails. A PostgreSQL sink
  is sent a year before 1 its own way, `'0045-03-15 BC'`. A CAST or literal
  also takes a zero-led year past four digits (`'01234-05-06'` is year 1234)
  and `'-0000'` as year 0, which a date never prints as. A CSV column of
  five- to seven-digit years with no leading zero (`'12345-01-02'`) is sniffed
  as `DATE`, which prints each back as it was written; a zero-led one
  (`'02024-01-05'`) stays text.
  Text to a number is **strict**: surrounding spaces aside, an optional sign,
  digits and at most one `.` (an `INT` takes no `.`), and anything else — `1_000`,
  `0x10`, `1 000` — fails rather than being guessed at. A `FLOAT` also reads an
  exponent (`1.5e-05`, as spreadsheets write small numbers) and the words `nan`,
  `inf` and `infinity`; the same rules decide whether a CSV column is sniffed as a
  number. That matters wherever money is written the Brazilian or European way
  — `'1000,00'` is not a number basalt will read, rather than a guess that might
  be 100000.00. Strip the separator first: `CAST(replace(v, ',', '.') AS
  DECIMAL(18,2))`, or reach for `TRY_CAST` to turn unreadable values into nulls.
- **A decimal that loses digits rounds half away from zero**, as PostgreSQL and
  SQL Server round: `CAST('12.345' AS DECIMAL(10,2))` is `12.35` and `-12.345`
  is `-12.35`. The rule is the same for a cast, for a value written to a file
  column of smaller scale (Parquet, Arrow), and for an aggregate's result — one
  answer whichever path a value takes. A database sink is sent the value whole
  and its column does its own rounding. A float converts through its
  15 significant digits, as PostgreSQL converts `float8` to `numeric`: the
  double nearest `2.675` is `2.67499…`, and it still becomes `2.68`. A float too
  large for 38 digits fails the cast.
- **`DECIMAL(p, s)` holds at most 38 digits**: `p` is 1 to 38, `s` at most `p`,
  and `DECIMAL(p)` is `DECIMAL(p, 0)`. A cast whose value (after rounding to `s`)
  has more than `p` digits fails — `CAST('1234.5' AS DECIMAL(4,1))` is an error,
  and `TRY_CAST` gives null. `CAST(x AS INT)` of a float rounds half to even,
  as PostgreSQL rounds a `float8` (`2.5` is `2`, `3.5` is `4`, `-2.7` is `-3`),
  while of a decimal it rounds half away from zero (`2.5` is `3`), as PostgreSQL
  rounds a `numeric`; a value past the INT range, `nan` or `inf` fails. Decimals compare exactly — `=`, `<`, `GROUP BY`, `DISTINCT`,
  joins, `MIN`/`MAX` and sorting tell `1234567890.12345678` from
  `1234567890.12345679` — and against an int exactly too; against a float
  they compare as floats.
- **Nested types**: `CAST(x AS STRUCT(city STRING, zip INT NOT NULL))`,
  `CAST(x AS LIST(INT))`, `CAST(x AS MAP(STRING, DECIMAL(10,2)))`, and any
  nesting of them. `x` is JSON text — a string column, what `json_object` or
  `json_get` returns — or a nested value. A part is nullable unless `NOT NULL`
  follows it; a map's key never is, and is a scalar type. The cast reads the
  JSON against the type: a struct's fields come out in the type's order, a field
  the JSON lacks is null and one it has beyond the type is dropped; a list's
  elements and a map's entries keep the order written; every scalar is read as
  its type, as a CAST of its text would read it (`BYTES` from base64, as JSON
  writes them). `'{"zip":"01310","city":"SP"}'` as the struct above is
  `{"city":"SP","zip":1310}`. A value that does not read as its type, a null or
  missing `NOT NULL` part, or an object where a list belongs fails the cast;
  `TRY_CAST` gives null. JSON `null` is SQL `NULL`. A nested value is shown and
  written as its JSON: in a JSON file as the object or array itself, in CSV and
  database columns as its text, in a Parquet file as Parquet's own
  `LIST`, `MAP` and nested groups, laid out as the format's spec has them, in
  an Arrow file as Arrow's `list`, `struct` and `map`, and in an Avro file as
  its arrays, records and maps (a map keyed by `string` only). An Excel output
  refuses a nested column.
- **Taking nested values apart**, each read from the column's own child
  columns, nothing parsed:
  - `s.city` is a struct's field, `t.s.city` with its table named, to any depth
    (`s.addr.zip`). A name is a table's column first: `s.city` is the struct
    `s`'s field only when no table of the FROM is named or aliased `s`. The
    column it gives is named for the field (`city`). Inside a subquery a bare
    `s.city` is read as the query around it naming its table `s`, since that is
    settled before either query's columns are known: name the subquery's own
    table there (`b.s.city`).
  - `l[i]` is a list's element, counted from 1; `m['k']` a map's value for a key;
    `s['city']` a struct's field. Past the end, or with no such key, it is NULL;
    `l[1].name` reaches into an element.
  - `cardinality(x)` is a list's or a map's length.
  - `list_filter(l, x -> cond)`, `list_transform(l, x -> value)`,
    `list_any(l, x -> cond)`, `list_all(l, x -> cond)` and
    `list_reduce(l, initial, (acc, x) -> value)` work on a list's elements in
    place, as the `json_*` lambdas do on a JSON array. The body sees the element
    as `x` (`x.name` for a struct's field), its position from 1 as an optional
    second parameter (`(x, i) -> …`), and the row's columns; a parameter
    shadows a column of its name. A condition that is not true (false or NULL)
    does not hold, and an empty list is false for `list_any`, true for
    `list_all`. The body runs once over all of a batch's elements, not row by
    row; `list_reduce` alone folds them one at a time.
  - `list_cosine_similarity(a, b)`, `list_cosine_distance(a, b)`,
    `list_distance(a, b)` (Euclidean), `list_inner_product(a, b)` and
    `list_negative_inner_product(a, b)` compare two lists of numbers, such as
    embeddings, as DuckDB's functions of those names do: elements of any numeric
    type (a Parquet `list<float>`, an Arrow fixed-size list) are taken as
    doubles, and the result is a `DOUBLE`. A NULL list gives NULL; a NULL element,
    or lists of different lengths, fail the run, naming the function (a `CASE`
    can keep such rows out). Two empty lists are 0 apart; a cosine with an empty
    or all-zero side is NULL, not NaN, which sorts above every number. A query
    vector is a cast from its JSON text, often a parameter (`PARAM q STRING;`,
    `-p 'q=[0.12, ...]'`); the nearest ten are
    `SELECT id, list_cosine_similarity(embedding, CAST($q AS LIST(DOUBLE))) AS s
    FROM 'docs.parquet' ORDER BY s DESC NULLS LAST LIMIT 10`, a parallel top-N
    over the file's row groups. `NULLS LAST` matters: a descending order puts
    NULLs first.
  - `chunks(text, size [, overlap])` cuts a text into pieces of at most `size`
    characters for embedding, as a `LIST` of `STRUCT(no, start, text)`: `no`
    counts them from 1 and `start` is a piece's first character's position from
    1, so `substr(text, start, length(piece))` is the piece. A piece is an exact
    substring, never trimmed; it ends after the last space, tab or line break in
    its `size` characters when one lies in their second half, else at `size`
    characters or the text's end. The next piece starts `overlap` characters
    (default 0, below `size`) before that end, moved forward to the next word
    when that lands inside one. Characters are UTF-8 code points; an empty text
    has no pieces and a NULL text gives NULL. The rule will not change, so a
    piece's hash stays a key to its embedding:
    `SELECT d.id, c.no, c.text FROM docs d CROSS JOIN UNNEST(chunks(d.body, 1000, 200)) AS c`.
  - `s.*` is a struct's fields as columns of their own.
  - `CROSS JOIN UNNEST(l) AS e` gives a row per element of a list (a map's
    entries as `e.key`, `e.value`), the row's other columns repeated; a NULL or
    empty list gives no rows.
  - A struct's field can be a `GROUP BY`, `ORDER BY` or join key
    (`GROUP BY s.city`, `ON a.s.id = b.id`), and so can the nested value
    itself, which also goes into `MIN`, `MAX`, `DISTINCT`, `PARTITION BY`, `=`,
    `<` and `IN`. Nested values compare part by part: a struct field by field in
    its type's order, a list element by element and then the shorter first, a
    map entry by entry (key, then value) in its stored order. A NULL part sorts
    after every value whatever the `NULLS FIRST` or `LAST`, which places only a
    NULL value itself, and floats inside compare as numbers (`-0.0` is `0.0`,
    which a group or DISTINCT keeps). A join key's two sides must be the same
    nested type.
- **DECIMAL arithmetic is exact.** `+`, `-` and `%` over two decimals (or a
  decimal and an int) answer a decimal at the wider operand's scale, `*` one at
  the summed scale — `1.1 + 0.3` is `1.4`, not `1.4000000000000001`, so a `SUM`
  cast to `DECIMAL` stays exact when this month's total is subtracted from last
  month's. A product's scale stops at 38, rounding half away from zero past it.
  A result (or a `SUM`'s final total) of more than 38 digits fails with
  `decimal overflow: more than 38 digits` — cast an operand to a smaller scale
  first. Anything
  with a float operand is a float. `round(x, n)` on a decimal is exact too, and
  with a literal `n` answers `DECIMAL(p, n)`; `-x`, `abs`, `floor`, `ceil`,
  `sign` and `mod` stay decimals, and `CAST(x AS INT)` rounds half away from
  zero. `%` takes the dividend's sign for every numeric kind: `-5.5 % 2` is
  `-1.5`.
- **Division follows the operands' types**, as PostgreSQL divides. `INT / INT`
  is an `INT`, truncated toward zero: `7 / 2` is `3`, `-7 / 2` is `-3`, and so
  `SUM(x) / COUNT(*)` over an INT column truncates — write `SUM(x) * 1.0 /
  COUNT(*)` or `AVG(x)` for the fraction. A decimal divided by a decimal or an
  int is an exact decimal at PostgreSQL's numeric scale: at least 16
  significant digits and no fewer decimals than either operand, rounded half
  away from zero — `1 / 3.0` is `0.33333333333333333333`, `7.0 / 2` is
  `3.5000000000000000`, `10 / 3.0` is `3.3333333333333333`. The 38-digit limit
  caps that scale where PostgreSQL would go on. Its column type is `DECIMAL(p,
  s)` with `s` the operands' larger scale but at least 16 while the whole
  digits leave room, so a file column gets that many decimals. A float divides
  as a float. A zero divisor — in `/`, `%` or `mod`, for every numeric kind,
  a float's included — fails the run with `division by zero`, as in
  PostgreSQL.
- A `DATE`/`TIMESTAMP` column compares directly against an ISO string literal
  (`WHERE d >= '2013-07-01'`). The literal is coerced to the column's type,
  never the reverse, and it is validated at plan time — so `'2013-13-01'` and
  `'01/07/2013'` are errors from `check`, not silent text comparisons. A quoted
  literal compared with a number or a bool is read the same way, as SQL reads
  an untyped literal by the other operand: `amount < '100000'` compares
  numbers (a decimal one exactly, at the literal's own scale), `id IN ('1',
  '2')` ints, `active = 'yes'` bools (`true`/`false`, `t`/`f`, `yes`/`no`,
  `y`/`n`, `1`/`0`, any case — the words `CAST(x AS BOOL)` and a `BOOL` PARAM
  read too, the ones DuckDB and PostgreSQL share; PostgreSQL's `on`/`off` and
  prefixes such as `'tr'` are refused). Surrounding spaces are ignored; anything else
  that does not read as the type — `'1,5'`, or `'1.5'` against an INT — is an
  error from `check`. Pushed down to a SQL source whose column types are known,
  it is sent as that number or bool; where they are not yet, PostgreSQL and SQL
  Server get the quoted literal, which they read the same way, and MySQL,
  StarRocks and Doris, which would compare a decimal with it as doubles, leave
  a number to the engine. A text *column* against a number still needs a `CAST`, as it
  does in PostgreSQL.
- `"Valor Total"` — a quoted column name ([Scripts](../language/scripts.md)), valid anywhere a bare name is:
  `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, an alias (`AS "Total Geral"`), and
  after a qualifier (`t."Valor Total"`).
- `x LIKE 'a%'`, `x IN (1, 2, 3)` (expands to an OR-chain),
  `x [NOT] BETWEEN a AND b` (inclusive; expands to `x >= a AND x <= b`, so it
  pushes down like any other pair of comparisons).
- `LET x = <val> IN <body>` — local binding, inlined at plan time.

- `TRY_CAST(x AS T)` — CAST that yields null instead of failing on a bad value;
  the workhorse for dirty inputs. Never pushed down.
- `CAST(x AS TIME)` takes `'HH:MM:SS[.ffffff]'` or `'HH:MM'` text, or a
  timestamp (its time of day). Fraction digits past the microsecond are
  dropped, for a `TIME` as for a `TIMESTAMP`, in a CAST and in a file alike,
  as DuckDB does for a `TIME` and StarRocks for a `DATETIME`:
  `'12:00:00.1234567'` is `12:00:00.123456`, never rounded up into the next
  second or day.
- Scalar functions are listed on [Scalar functions](functions.md), and the JSON
  ones on [JSON functions](json.md).
