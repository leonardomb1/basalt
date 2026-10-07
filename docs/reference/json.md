# JSON functions

JSON documents travel as text: a `JSON` column, a nested Parquet or Arrow column,
a REST response's nested object. These functions build and take them apart;
`CROSS JOIN UNNEST(JSON_EACH(col))` turns an array into rows (see
[Queries](../language/queries.md#operators)).

## Building a document

`JSON_OBJECT(k1, v1, …)`, `JSON_ARRAY(v1, …)` — build a JSON document, for a
request body or a nested column: numbers and booleans as themselves, text
as a JSON string, a null as `null` (the result itself is never null), and a
NaN or infinity as `null` too, since JSON has neither. Text
that is a JSON object or array — another `json_object`, or what `json_get`
returned — goes in as that object or array, so
`json_object('id', id, 'tags', json_array(a, b))` nests. A null key fails
the statement.

## Reading a value

`JSON_GET(doc, path)` — one value out of a JSON document, as text: `path` is
`a.b`, `a[0].b` or `a.0.b` (a leading `$.` is allowed). Strings come back
unquoted, objects and arrays as JSON text; a missing key, an index past the
end or a JSON `null` is null. A cell that is not JSON is an error, not a
null. `CAST(json_get(doc, 'n') AS INT)` for a typed value.

## Lambdas over an array

`JSON_FILTER(arr, x -> cond)`, `JSON_TRANSFORM(arr, x -> value)`,
`JSON_ANY(arr, x -> cond)`, `JSON_ALL(arr, x -> cond)` — work on a JSON array
in place, without `UNNEST` and re-aggregating: the lambda's body is evaluated
once per element with its parameter bound to it. `json_filter` keeps the
elements whose condition is true, as written; `json_transform` returns the
array of the body's values; `json_any` / `json_all` whether the condition is
true for some / every element (an empty array: false / true). A nested
Parquet list reads as exactly such an array.

```sql
SELECT id,
       json_filter(tags, t -> t LIKE 'vip%')                    AS vip_tags,
       json_transform(items, i -> json_get(i, 'sku'))           AS skus,
       json_any(items, i -> CAST(json_get(i, 'qty') AS INT) = 0) AS has_empty_line
FROM 'orders.parquet'
WHERE json_any(tags, t -> t = 'new');
```

An element is the parameter as itself — a number is a number (`x > 5`), a
string a string, `true`/`false` a BOOL — and an object or array is its JSON
text, which `json_get` and these functions take apart (lambdas nest). The body
may name the row's columns too (`t -> t = region`); the parameter shadows a
column of its name. In `json_transform`'s result, text that is a JSON object
or array (what `json_get` returns for one) goes in as that object or array,
any other text as a string. An element the body cannot compare — a number
against text, in an array that mixes kinds — counts as null (the condition is
not true; `json_transform` puts `null`); a failing `CAST` still fails the run.
A null cell is null; a cell that is JSON but not an array is an error. A
lambda is an argument of these functions and `json_reduce` only, and never
descends to a SQL source — the row is filtered here, while the rest of its
WHERE still descends. Written `(x, i) -> …`, the lambda also gets the
element's position, from 0 as in a `json_get` path.

## Folding an array

`JSON_REDUCE(arr, initial, (acc, x) -> value)` — a fold: `acc` starts at
`initial` and becomes the body's value at each element in turn; the result is
the last one, `initial` for an empty array and null for a null cell.
`(acc, x, i) -> …` adds the position. The accumulator keeps `initial`'s type,
widened to what the body returns from it (a DECIMAL total stays a DECIMAL, a
date a date); a FLOAT element arriving in an INT or DECIMAL total stops the
statement rather than being cut short — start from `0.0` to sum floats. An
element the body cannot compare stops it too, where `json_transform` would
put null: a null mid-fold would quietly undo everything folded before it.

## Arrays

`CHARS(s)` is a string's characters as a JSON array, `JSON_RANGE([start,]
stop)` the integers `start` (default 0) up to but not including `stop`,
`JSON_LENGTH(arr)` an array's element count, `JSON_SLICE(arr, start[, stop])`
its elements from `start` to before `stop` (from 0; a negative bound counts
from the end, as in Python) and `JSON_CONCAT(a, b, …)` the arrays joined (null
when any is). Together with `json_reduce` they make a per-character algorithm
a function — a CNPJ check digit, letters included (the July 2026
alphanumeric CNPJ counts a character as its code point minus 48):

```sql
CREATE FUNCTION cnpj_dv(base, weights) AS
  LET s = json_reduce(chars(base), 0, (acc, c, i) ->
            acc + (ascii(c) - 48) * CAST(json_get(weights, CAST(i AS STRING)) AS INT))
  IN CASE WHEN s % 11 < 2 THEN 0 ELSE 11 - s % 11 END;

CREATE FUNCTION cnpj_valid(raw) AS
  LET d = translate(upper(raw), './-', '')
  IN length(d) = 14
     AND cnpj_dv(left(d, 12), '[5,4,3,2,9,8,7,6,5,4,3,2]') = ascii(substr(d, 13, 1)) - 48
     AND cnpj_dv(left(d, 13), '[6,5,4,3,2,9,8,7,6,5,4,3,2]') = ascii(substr(d, 14, 1)) - 48;
-- cnpj_valid('11.222.333/0001-81') and cnpj_valid('12.ABC.345/01DE-35') are true
```
