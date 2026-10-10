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
- `CAST(x AS INT)` / `CAST(x AS DECIMAL(18,2))` / `CAST(x AS DATE)` /
  `CAST(x AS TIMESTAMP)` — implicit widening is int→float/decimal only. Text
  parses as `YYYY-MM-DD[ HH:MM:SS]`, and a day the month does not have
  (`'2023-02-29'`, `'2024-04-31'`) fails rather than rolling into the next month.
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
- **DECIMAL arithmetic is exact.** `+`, `-` and `%` over two decimals (or a
  decimal and an int) answer a decimal at the wider operand's scale, `*` one at
  the summed scale — `CAST(1.1 AS DECIMAL(18,2)) + CAST(0.3 AS DECIMAL(18,2))` is
  `1.40`, not `1.4000000000000001`, so a `SUM` cast to `DECIMAL` stays exact
  when this month's total is subtracted from last month's. A product's scale
  stops at 38, rounding half away from zero past it. A result (or a `SUM`) of
  more than 38 digits fails with `decimal overflow: more than 38 digits` — cast
  an operand to a smaller scale first. `/` has no finite
  scale and stays float, as does anything with a float operand (a literal like
  `0.3` is a float). `round(x, n)` on a decimal is exact too, and with a literal
  `n` answers `DECIMAL(p, n)`; `-x` stays a decimal, and `CAST(x AS INT)` rounds
  half away from zero. `%` takes the dividend's sign for every numeric kind:
  `-5.5 % 2` is `-1.5`.
- A `DATE`/`TIMESTAMP` column compares directly against an ISO string literal
  (`WHERE d >= '2013-07-01'`). The literal is coerced to the column's type,
  never the reverse, and it is validated at plan time — so `'2013-13-01'` and
  `'01/07/2013'` are errors from `check`, not silent text comparisons. A quoted
  literal compared with a number or a bool is read the same way, as SQL reads
  an untyped literal by the other operand: `amount < '100000'` compares
  numbers (a decimal one exactly, at the literal's own scale), `id IN ('1',
  '2')` ints, `active = 'yes'` bools (`true`/`false`, `t`/`f`, `yes`/`no`,
  `on`/`off`, `1`/`0`, any case). Surrounding spaces are ignored; anything else
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
  timestamp (its time of day).
- Scalar functions are listed on [Scalar functions](functions.md), and the JSON
  ones on [JSON functions](json.md).
