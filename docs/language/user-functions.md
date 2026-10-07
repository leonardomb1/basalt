# User functions

`CREATE FUNCTION` names an expression, a block of statements or a query, so a
script — or a library it `@include`s — can reuse it.

## Scalar and statement functions

`CREATE [OR REPLACE] FUNCTION nome(a [TYPE] [DEFAULT <expr>], ...)` — two body
forms. `AS <expr>;` is a scalar function, inlined at plan time; recursion and
arity mismatches are compile errors, declared types are checked against
literal arguments at the call site, defaults fill omitted trailing arguments.
A body starting with `LOAD`/`FOR`/`CALL`/`SELECT`/`WITH`/`PRINT`/`THROW`, or
with the statement `CASE` (the one closed by `END CASE`), is a **statement
function** terminated by `END;` and invoked with `CALL nome(args);` — its
params bind like loop variables (`$name`, `IDENTIFIER($name)`, `PUSHDOWN($f)`,
`${name}` in strings), rendered per call through the same machinery as a `FOR
EACH ROW OF` body. CALL nesting is depth-guarded (16); a statement function is
not atomic — a mid-body failure leaves earlier loads committed, exactly as if
the statements were inline. Plain re-declaration of a name is an error; `OR
REPLACE` is the sanctioned overwrite.

## Table functions

`CREATE [OR REPLACE] FUNCTION nome(a [TYPE] [DEFAULT <expr>], ...) RETURNS
TABLE AS <query>;` is a **table function**: a query with parameters, read
wherever a CTE could be — `FROM nome(args) [alias]`, or the right side of a
`JOIN`. Each call is the body with every `$a` replaced by its argument,
lowered to a derived table at plan time, so it behaves exactly as if the query
were written inline; two calls in one query (even of one function) are
independent, `WITH` clauses inside the body included. Without an alias, the
function's name qualifies its columns (`paid.id`). Arguments are plan-time
constants — literals, `$params`, loop variables, and expressions over them —
except under `JOIN LATERAL` (below). Arity, defaults and literal-argument
types are checked as for a scalar function, the body is checked where it is
declared, and an `@include`d table function is called like a local one. A body
may call table functions declared before it; a call that reaches its own
function (possible only through `OR REPLACE`) stops at 16 levels. A discovery
query — `FOR EACH ROW OF (...)`, `EACH TABLE OF (SELECT ...)` — may call one
too, as it may read a derived table or open with `WITH`: their bindings run
ahead of the loop.

```sql
CREATE FUNCTION paid_orders(since DATE, branch STRING DEFAULT '01') RETURNS TABLE AS
  SELECT C5_NUM AS num, C5_CLIENTE AS cliente, C5_EMISSAO AS emissao
  FROM erp.dbo.SC5010 PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
  WHERE C5_FILIAL = $branch AND C5_EMISSAO >= $since;

SELECT num, cliente FROM paid_orders($desde) WHERE cliente <> '000001';
```

## JOIN LATERAL

`CROSS JOIN LATERAL f(o.col) x` (or `JOIN LATERAL`, or `LEFT JOIN LATERAL …
ON TRUE` to keep rows with no match) passes each row's column. It is not a
query per row: where the body says `column = $param`, that conjunct leaves the
body and the call becomes an ordinary hash join `ON o.col = x.column` — the
table is read once, its other conditions still reach the source, and lanes
apply as to any join. So the body has to be one the join can stand for: a
read, joins, filters and a SELECT list, with the parameter only in `column =
$param` conjuncts of its WHERE (or on its own as a SELECT item, which is then
that column). A `LIMIT`, `DISTINCT`, `GROUP BY`, window or set operation, a
parameter compared any other way, or one never compared at all is refused at
plan time, naming it: a join could not apply them per row. A body that does
not select the column joins on it under the parameter's name, which `x.*`
then shows. Constant arguments in the same call stay constants.

```sql cont
CREATE FUNCTION itens(pedido) RETURNS TABLE AS
  SELECT C6_ITEM AS item, C6_PRODUTO AS produto, C6_QTDVEN AS qtd
  FROM erp.dbo.SC6010 PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
  WHERE C6_NUM = $pedido;

-- one read of SC6010, joined on C6_NUM = p.num
SELECT p.num, p.cliente, i.produto, i.qtd
FROM paid_orders($desde) p CROSS JOIN LATERAL itens(p.num) i;
```
