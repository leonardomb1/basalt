# Expressions and types

SQL-ish, Pratt-parsed. Precedence (high→low): unary `- NOT ~` → `* / %` →
`+ - ||` → `<< >>` → `&` → `^` → `|` → comparisons
`= == != <> < <= > >= LIKE IN IS` → `??` → `AND` → `OR`.

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
- `a ?? b` — null-coalesce (sugar for `COALESCE`).
- `CAST(x AS INT)` / `CAST(x AS DECIMAL(18,2))` / `CAST(x AS DATE)` /
  `CAST(x AS TIMESTAMP)` — implicit widening is int→float/decimal only. Text
  parses as `YYYY-MM-DD[ HH:MM:SS]`. Text to a number is **strict**: an optional
  sign, digits and at most one `.`, and anything else fails rather than being
  guessed at. That matters wherever money is written the Brazilian or European way
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
- **DECIMAL arithmetic is exact.** `+`, `-` and `%` over two decimals (or a
  decimal and an int) answer a decimal at the wider operand's scale, `*` one at
  the summed scale — `CAST(1.1 AS DECIMAL(18,2)) + CAST(0.3 AS DECIMAL(18,2))` is
  `1.40`, not `1.4000000000000001`, so a `SUM` cast to `DECIMAL` stays exact
  when this month's total is subtracted from last month's. `/` has no finite
  scale and stays float, as does anything with a float operand (a literal like
  `0.3` is a float). `round(x, n)` on a decimal is exact too, and with a literal
  `n` answers `DECIMAL(p, n)`; `-x` stays a decimal, and `CAST(x AS INT)` rounds
  half away from zero. `%` takes the dividend's sign for every numeric kind:
  `-5.5 % 2` is `-1.5`.
- A `DATE`/`TIMESTAMP` column compares directly against an ISO string literal
  (`WHERE d >= '2013-07-01'`). The literal is coerced to the column's type,
  never the reverse, and it is validated at plan time — so `'2013-13-01'` and
  `'01/07/2013'` are errors from `check`, not silent text comparisons.
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
