# Basalt SQL — Documento de Migração de Linguagem

**Status:** proposta de design · **Alvo:** basalt v0.2.x · **Origem:** BSL (`.bsl`) v0.1.13

Este documento compila o redesenho completo da linguagem do basalt de um DSL próprio
(BSL) para um dialeto SQL — **Basalt SQL** — preservando a semântica do plano de
execução atual (plan-time static, streaming pull, pushdown, stream load) e trocando
apenas a superfície sintática. Cada construção nova cita o precedente de mercado que
a ancora (DuckDB, Trino, Snowflake, Databricks, Postgres, T-SQL).

**O BSL é removido por completo em v0.2.0.** Não há período de convivência de
dialetos: o lexer/parser `.bsl` sai da árvore, e todo script existente é reescrito
em Basalt SQL na mesma entrega. Consequência estrutural: nenhuma construção pode
"ficar no BSL até haver proposta boa" — tudo que o engine faz hoje precisa de
grafia SQL definida neste documento (ver §11, que deixa de ser lista de pendências
e vira escopo obrigatório).

---

## 1. Motivação

1. **Contratar e treinar.** A stack (StarRocks, Windmill, basalt) é deliberadamente
   fora do padrão do mercado brasileiro; o custo dela é organizacional, não técnico.
   Uma superfície SQL reduz o bus factor: analista lê `LOAD INTO ... AS SELECT` no
   primeiro dia, sem manual.
2. **O BSL já é quase SQL.** `select * except (...)` é o `EXCLUDE` do DuckDB,
   `aggregate ... by` é `GROUP BY`, o pipeline com `|` é a pipe syntax do
   GoogleSQL/Databricks. A migração é curta porque a distância é curta.
3. **Menos conceitos, não mais.** A migração *remove* vocabulário: `write stdout`
   desaparece (SELECT terminal imprime), o hint `@[where]` desaparece como hint —
   o caso comum vira `WHERE` traduzido automaticamente (pushdown implícito) e o
   caso dialeto-específico vira a cláusula `PUSHDOWN($$...$$)` (§7), `env()`/
   `secret()` desaparecem (convenção de credencial), a tag do union desaparece
   (é uma coluna literal).

### Princípio central de design

> **Se uma opção aparece em mais de um terço dos scripts, ela merece cláusula
> própria com palavra própria. Senão, fica no `WITH (...)` residual.**

É como o SQL real evoluiu: `PARTITION BY`, `ON CONFLICT` e `QUALIFY` nasceram como
configuração e viraram gramática quando o uso provou frequência. Aplicado aqui:
disposição de escrita, chave de upsert, paralelismo, paginação e retry viram
cláusulas; `label_prefix`, `buckets`, `prefetch`, `timeout_ms` ficam no saco.

---

## 2. Estrutura do programa

```
[CREATE ENDPOINT ...;]        -- apenas modo HTTP; ausência = batch
PARAM ...;                    -- entradas (CLI / request)
CREATE CONNECTION ...;        -- endpoints de dados nomeados
LOAD INTO ... AS <query>;     -- pipeline(s) de saída
<query terminal>;             -- SELECT solto = imprime no stdout
```

### Batch é o default silencioso

`@batch` deixa de existir como obrigação. Um script sem cabeçalho roda uma vez até
o fim — que é o que qualquer leitor de SQL assume de um script. Todos os exemplos
deste documento são completos como estão.

### HTTP é a declaração explícita

`@http(path = "/x", doc = "...")` vira primeira instrução SQL, com precedente
direto no T-SQL (`CREATE ENDPOINT ... AS HTTP`, SQL Server 2005):

```sql
CREATE ENDPOINT '/ingest/pedidos'
  DOC 'Ingere pedidos por empresa; corpo JSON';
```

O `basalt serve <dir>` continua roteando cada arquivo pelo path declarado; `DOC`
alimenta o banner.

---

## 3. Parâmetros

```sql
PARAM dias   INT DEFAULT 7;             -- escalar; batch: -p dias=3 | http: query string
PARAM desde  TIMESTAMP;                 -- sem default = obrigatório
PARAM job    JSON FROM BODY;            -- corpo da request como documento JSON
PARAM tenant STRING FROM HEADER('X-Tenant');
```

- Referência no corpo da query com `$`: `$dias`, `$desde`.
- Navegação JSON por caminho pontilhado: `$job.tables`, `$job.source.host`.
- Navegação segura: `$job.filtro?.uf` — intermediário ausente resolve o caminho
  inteiro para `null` (regra idêntica ao `?.` do BSL; só em caminhos JSON).
- Defaults de origem: escalar ← query string, `JSON` ← corpo. `FROM BODY`,
  `FROM QUERY`, `FROM HEADER(...)` explicitam o incomum.

---

## 4. Conexões

### Grafia

`CREATE CONNECTION` (precedente: Databricks Lakehouse Federation; o ancestral ANSI
é o SQL/MED `CREATE SERVER`), com `chave = valor` em tudo:

```sql
CREATE CONNECTION erp TYPE sqlserver OPTIONS (
  host     = 'sql.internal',
  port     = 1433,
  database = 'totvs',
  tls      = 'require'
);

CREATE CONNECTION sr TYPE starrocks OPTIONS (
  fe_host  = '10.0.0.7',
  be_url   = 'http://10.0.0.10:8040',
  database = 'bronze'
);
```

### Credenciais por convenção — `env()`/`secret()` são removidos

Conexão `erp` resolve `ERP_USER`/`ERP_PASS` do ambiente ao conectar; `sr` resolve
`SR_USER`/`SR_PASS`. O caso comum custa **zero caracteres** no script. O
`basalt check` avisa em plan-time quais variáveis o script espera.

Válvula de escape (credencial compartilhada, nome imposto pelo orquestrador): o
namespace `env` na interpolação que a linguagem já tem —

```sql
  user     = '${env.SVC_TOTVS_USER}',
  password = '${env.SVC_TOTVS_PASS}'
```

Segredo nunca é literal no script; sempre indireção para o ambiente.

### Onde as conexões moram

| camada | mecanismo | semântica |
|---|---|---|
| no script | `CREATE CONNECTION` inline | escopo de sessão (modelo `ATTACH` do DuckDB); autocontido, ideal p/ Windmill |
| compartilhada | `connections.sql` — só `CREATE CONNECTION`s | `basalt serve <dir>` lê `<dir>/connections.sql` por convenção; batch: `--use connections.sql`. Mesma gramática, zero YAML |
| sobreposição | `CREATE OR REPLACE CONNECTION` no script | modo estrito: redeclarar sem `OR REPLACE` é erro de plano |

Sem metastore: o "catálogo" é o repositório git — conexão tem diff, blame e review.

---

## 5. Sink — `LOAD INTO`

```sql
LOAD INTO sr.silver.pedidos_obra          -- destino primeiro
  USING stream_load                       -- adaptador físico
  UPSERT ON (empresa, num_pedido)         -- disposição como verbo
  SPLIT BY (num_pedido) JOBS 4            -- paralelismo de carga
AS                                        -- dobradiça: acima mecânica, abaixo query
<query>;
```

Decisões, uma a uma:

- **`LOAD INTO`**, não `INSERT INTO`: insert é semântica de linha; isto é carga em
  massa. Ecoa o `LOAD LABEL` do StarRocks e o `COPY INTO` do Snowflake.
- **`USING <forma>`** é o mecanismo físico: `stream_load`, `insert`, futuramente
  `clickhouse_http`. Precedente: `USING parquet` (Spark/Databricks).
- **Disposição vira gramática** (precedente `ON CONFLICT` do Postgres — a chave
  muda a semântica, não é config):

  | Basalt SQL | BSL v0.1 |
  |---|---|
  | `APPEND` (default, omissível) | `append` / default |
  | `REPLACE` | `overwrite` |
  | `UPSERT ON (a, b)` | `upsert on a, b` |
  | `UPSERT ON (id) PARTIAL COLS (a, b)` | `upsert on id partial cols (a,b)` |
  | `UPSERT` (infere PK da fonte) | `upsert` bare |
  | `CREATE OR APPEND` | `auto_create` |

  Chave vazia/não-resolvida continua sendo **erro**, nunca no-op silencioso.
- **`SPLIT BY (col) JOBS n`**: particiona a carga por faixas de chave (o
  `@[split = col]`), absorvendo o `-j` da CLI quando o autor quer fixar. Sem
  `JOBS`, vale a linha de comando.
- **`AS` como delimitador**, não `BEGIN/END`: `BEGIN` já tem dono em SQL
  (transação/bloco procedural — leitura errada para o público SQL Server). O
  idioma consagrado para "objeto definido pela query a seguir" é `AS`
  (`CREATE TABLE AS`, `CREATE VIEW AS`, `EXPORT DATA AS SELECT`). Bônus de
  parser: antes do `AS`, `WITH` só pode ser opção; depois, só pode ser CTE.
- **`MERGE INTO`** completo fica disponível como forma canônica ANSI do upsert,
  mas `UPSERT ON` permanece como açúcar do caso de 99%.
- **`WITH (...)` residual** para knobs raros de conector:
  `WITH (label_prefix = 'noturno', buckets = 16)`.
- **Alvo-arquivo pela extensão:** `LOAD INTO '/out/pedidos.csv' AS SELECT ...`.

### stdout não é sintaxe

Um `SELECT` terminal imprime a tabela (formato alinhado atual). `write stdout`
deixa de existir. Efeito colateral: `basalt run -c "SELECT ... FROM 'x.csv'"` vira
um mini-DuckDB de linha de comando.

---

## 6. Fontes

| fonte | Basalt SQL | precedente | observação |
|---|---|---|---|
| tabela SQL | `FROM erp.dbo.SC5010` | Trino (catalog.schema.table) | pushdown implícito (§7) |
| tabela + predicado cru | `FROM erp.dbo.SC5010 PUSHDOWN($$...$$)` | — | fragmento SQL verbatim na fonte (§7); o antigo `@[where]` |
| query crua | `FROM erp.QUERY($$SELECT ...$$)` | — | sem tradução de dialeto, como hoje; literal dollar-quoted (§7) |
| CSV | `FROM 'dados.csv'` | DuckDB | inferência de tipos idêntica à atual (1024 linhas) |
| CSV c/ opções | `FROM CSV('arq.txt', sep = ';')` | DuckDB `read_csv` | forma-função p/ o raro |
| REST/JSON | `FROM crm.'/v1/customers' PAGINATE ...` | — | §8 |
| REST sem conexão | `FROM HTTP('https://host/api/x') PAGINATE ...` | — | o `read http "<url>"` do BSL; forma-função, mesmas cláusulas da fonte REST |
| corpo HTTP | `FROM BODY (schema)` | — | §9 |
| buffer WAL | `FROM BUFFER 'nome'` | — | §10 |
| binding | CTE (`WITH x AS (...)`) | ANSI | substitui `let` |

### Fonte REST — as duas invenções necessárias

Autenticação mora na conexão (com `CRM_TOKEN` por convenção). Paginação e retry
são promovidos a cláusula — são *a* característica definidora de ingestão REST:

```sql
FROM crm.'/v1/customers'
  PAGINATE BY page   (param = 'page', size = 100, total = 'count')
  -- ou: PAGINATE BY offset (...)
  -- ou: PAGINATE BY cursor (field = 'next', param = 'after')
  RETRY 3 ON (429, 503)
  WITH (prefetch = 4, timeout_ms = 30000, method = 'post', body = '{...}')
```

`RETRY n ON (códigos)` funde `retries` + `retry_statuses`, que sempre andam
juntos. Os demais hints de §9 do BSL (`stop_short`, `progress_ms`, auth avançada
`login_json`/`oauth2`...) ficam no `WITH` residual da fonte.

**Semântica a documentar:** `WHERE` sobre fonte REST roda **no basalt, após a
busca** (REST não aceita pushdown); sobre tabela SQL, desce para a fonte. Mesmo
símbolo, plano diferente — `check -s` mostra a diferença.

---

## 7. Query, pushdown e join

```sql
LOAD INTO sr.silver.pedidos_obra
  USING stream_load
  UPSERT ON (empresa, num_pedido)
  SPLIT BY (num_pedido) JOBS 4
AS
WITH pedidos AS (                        -- roda NO SQL Server (pushdown)
  SELECT filial, num, cliente, obra, emissao, valor
  FROM erp.dbo.SC5010
  WHERE D_E_L_E_T_ <> '*'
    AND emissao >= today() - $dias
),
obras AS (                               -- roda NO Postgres (pushdown)
  SELECT codigo_obra, nome_obra, gestor, centro_custo
  FROM campo.public.obras
  WHERE ativo
),
enriquecido AS (                         -- roda AQUI, na memória do basalt
  SELECT SUBSTR(p.filial, 1, 2) AS empresa,
         p.num AS num_pedido,
         p.cliente, p.emissao, p.valor,
         o.nome_obra, o.gestor, o.centro_custo
  FROM pedidos p
  LEFT JOIN obras o ON p.obra = o.codigo_obra
)
SELECT * FROM enriquecido;
```

### Pushdown implícito e explícito

Dois níveis, mesmo plano:

- **Implícito (o default): pushdown por conexão é invisível.** CTE que referencia
  tabelas de *uma* conexão e é inteiramente traduzível desce como query para a
  fonte (modelo Trino). `WHERE`/projeção na tabela-fonte são o pushdown; o que o
  tradutor não sabe emitir naquele dialeto fica no basalt (filtro pós-fetch),
  nunca erro. `check -s` mostra a query final enviada à fonte — a linha de corte
  entre "desceu" e "ficou" é sempre inspecionável.
- **Explícito: `PUSHDOWN($$<fragmento SQL>$$)` na tabela-fonte.** O fragmento
  vai **verbatim** (sem tradução, sem parse) para o `WHERE` da query gerada
  contra a fonte — é o sucessor do `@[where = "..."]`, agora cláusula em vez de
  hint. Uso: predicado dialeto-específico que o tradutor não cobre (função
  T-SQL, collation, subquery correlacionada no dialeto da fonte):

  ```sql
  FROM erp.dbo.SC5010
    PUSHDOWN($$D_E_L_E_T_ <> '*' AND R_E_C_N_O_ > ISNULL((SELECT ...), 0)$$)
  WHERE valor > 0                       -- implícito: ANDado se traduzível
  ```

  Regras: o fragmento é composto por `AND` com o que o pushdown implícito
  derivar; erro de sintaxe só aparece na fonte, em runtime (é o preço do
  verbatim — erro permanente, exit 1); `${...}` interpola normalmente dentro do
  literal; literal vazio = sem cláusula (paridade com o `@[where = ""]` de
  hoje). Para a *query inteira* crua, `QUERY($$SELECT ...$$)` continua sendo
  a forma — `PUSHDOWN` é fragmento de predicado, `QUERY` é a fonte toda.

### O literal de SQL cru — dollar-quoting

`PUSHDOWN(...)` e `QUERY(...)` recebem um **literal de SQL cru** com
**dollar-quoting do Postgres**: `$$...$$`, ou a forma etiquetada
`$tag$...$tag$` quando o corpo precisa conter `$$`. Racional: o conteúdo é SQL,
e SQL é cheio de aspas simples — com `'...'` o caso comum exigiria dobrar
(`''*''`); com `$$` o caso comum fica limpo, e o delimitador não colide com
nenhum dialeto de fonte (crase foi descartada por ser quoting de identificador
em MySQL/StarRocks).

- **Sem escapes** dentro do literal (nem `\n` nem dobra de aspas) — o texto vai
  como está até o delimitador de fechamento. Corpo contendo `$$`: use uma tag
  (`$sql$ ... $$ ... $sql$`), regra idêntica ao Postgres.
- **`${...}` interpola** com as regras C#-style de sempre (brace-balanced,
  string-aware). Não colide com o dollar-quoting: `${` abre interpolação, `$$`
  e `$tag$` delimitam o literal, `$nome` fora de literal é referência a `PARAM`.
- `'...'` comum também é aceito nas duas formas (com dobra `''`) — o
  dollar-quoting é o idioma preferido, não a única grafia.

Demais regras do planner:
- **A fronteira multi-conexão é o breaker.** O join entre conexões roda no
  basalt: lado menor materializa (build do hash join), lado maior faz streaming
  — a mecânica atual de `let` + `join <binding>`. Escolha do lado de build por
  estimativa, com escape `WITH (build = obras)`.
- Kinds de join: `INNER | LEFT | RIGHT | FULL | CROSS | SEMI | ANTI` — paridade
  total com o BSL v0.1 (semi/anti via `WHERE [NOT] EXISTS` na grafia ANSI, ou
  palavras diretas como extensão; `CROSS JOIN` também é a base do `explode` via
  `UNNEST`).
- O hint `@[buffer]` (drenar fonte antes de abrir o sink — Dataverse) vira
  `WITH (buffer)` na CTE/fonte.

### Mapeamento de operadores

| BSL v0.1 | Basalt SQL | nota |
|---|---|---|
| `filter <expr>` | `WHERE` | |
| `select a, x = expr` | `SELECT a, expr AS x` | |
| `select * except (a)` | `SELECT * EXCLUDE (a)` | DuckDB; não-ANSI, mantido |
| `select * rename (a as b)` | `SELECT * RENAME (a AS b)` | DuckDB |
| `aggregate s = sum(x) by k` | `GROUP BY k` (+ `GROUP BY ALL` açúcar) | DuckDB |
| `sort a desc, b` | `ORDER BY a DESC, b` | |
| `limit 100 offset 20` | `LIMIT 100 OFFSET 20` | **`LIMIT`, não `FETCH FIRST`** — ANSI de facto (DuckDB, Trino, StarRocks, Databricks, Snowflake) |
| `distinct` / `distinct on a` | `DISTINCT` / `DISTINCT ON (a)` | `ON` é extensão Postgres/DuckDB, mantida (a alternativa ANSI `ROW_NUMBER()` é 3 linhas para 2 palavras) |
| `explode tags as t on ","` | `CROSS JOIN UNNEST(SPLIT(tags, ',')) AS t` | Trino; menos legível, mas padrão |
| `if(c, a, b)` | `CASE WHEN c THEN a ELSE b END` (e `IF()` mantido como açúcar) | |
| `match ... => ... end` (expressão) | `CASE` (subject e guard forms) | sem perda |
| `a ?? b` | `COALESCE(a, b)` (e `??` mantido) | |
| `x is empty` | `x IS EMPTY` (extensão; = null ou `''`) | não há equivalente ANSI de 2 palavras |
| `cast(x as int)` | `CAST(x AS INT)` | idêntico |
| `let x = e in body` (expr) | `LET x = e IN body` mantido em expressão | ANSI não tem binding local em expressão |
| `fn nome(a) = expr` | `CREATE FUNCTION nome(a) AS expr` | inlinada em plan-time como hoje |
| `let nome = <pipeline>` | CTE | |
| funções escalares (`substr`, `lower`, `concat`, ...) | idênticas | já são SQL |

---

## 8. UNION — reconciliação por nome

Semântica a preservar (o `UNION ALL` ANSI é o oposto — posicional e estrito):
alinhamento **por nome**, NULL-fill no ausente, descarte do excedente, cast nas
diferenças, autoridade canônica, coluna de tag.

Âncora: **`UNION ALL BY NAME`** (DuckDB). A tag **deixa de ser conceito** — é uma
coluna literal, como qualquer pessoa de SQL faria:

```sql
-- forma explícita (o caso CT2 dos testes)
LOAD INTO sr.bronze.CT2_UNIFIED
  USING stream_load
  UPSERT ON (CT2_EMPRESA, R_E_C_N_O_)
  SPLIT BY (R_E_C_N_O_)
AS
SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
UNION ALL BY NAME
SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
ANCHOR SCHEMA erp.dbo.CT2010;        -- o `canon`; opcional (default: união de
                                     -- nomes, tipos alargados — regra DuckDB)
```

```sql
-- forma discovered (dezenas de tabelas por empresa, padrão Protheus)
LOAD INTO sr.bronze.CT2_UNIFIED
  USING stream_load
  UPSERT ON (CT2_EMPRESA, R_E_C_N_O_)
AS
SELECT *
FROM EACH TABLE OF (erp.QUERY($$
  SELECT name, SUBSTRING(name, 4, 2) FROM sys.tables WHERE name LIKE 'CT2%'
$$))
  AS (table_name, CT2_EMPRESA)              -- 1ª coluna = tabela, 2ª = tag
  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)           -- predicado cru em TODOS os ramos
  ANCHOR SCHEMA erp.dbo.CT2010;
```

Regra do `EACH TABLE OF (...)`: uma linha por ramo. **v0.2**: a descoberta é uma
query crua (`<conn>.QUERY($$...$$)`) de duas colunas — (tabela, tag) — e a
cláusula `AS (table_name, <col>)` nomeia a coluna de tag na saída. A forma
generalizada (SELECT traduzível como descoberta, N colunas extras injetadas por
ramo) fica para quando existir tradução query→SQL da fonte; a grafia já a
comporta. A forma `json` do BSL cai no mesmo buraco sem construção nova:
`EACH TABLE OF` aceita um **caminho de PARAM JSON** (ou uma string interpolada)
no lugar da query, com a conexão dos ramos explícita — `FROM EACH TABLE OF
($corpo.tables) IN erp AS (table_name, tag)`. Elementos do array: objetos com
`table`/`name` + `tag`/`emp` (remapeáveis via `WITH (table_field = ...,
tag_field = ..., tag_substr = '4,2')`).

Vocabulário novo total do capítulo: `EACH TABLE OF` e `ANCHOR SCHEMA`.

---

## 9. Modo HTTP

### Ingestão configurada por request (corpo como *parâmetro*)

```sql
CREATE ENDPOINT '/ingest/pedidos'
  DOC 'Ingere pedidos por empresa; corpo JSON';

PARAM job JSON FROM BODY;
PARAM dry BOOL DEFAULT false;

LOAD INTO sr.bronze.pedidos
  USING stream_load
  UPSERT ON (empresa, num_pedido)
AS
SELECT SUBSTR(filial, 1, 2) AS empresa, num AS num_pedido, cliente, emissao, valor
FROM erp.dbo.SC5010
WHERE D_E_L_E_T_ <> '*'
  AND emissao >= $job.desde
LIMIT CASE WHEN $dry THEN 100 ELSE NULL END;
```

### Recepção de dados (corpo como *linhas*) — modo mensageria

Nova fonte `FROM BODY (schema)`. Schema **declarado, não inferido** (o corpo não
existe em plan-time) — e a declaração é o contrato do endpoint:

```sql
CREATE ENDPOINT '/eventos'
  DOC 'Recebe eventos de telemetria e grava no StarRocks';

LOAD INTO sr.bronze.eventos
  USING stream_load
AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, tipo, payload, now() AS recebido_em
FROM BODY (
  device_id STRING NOT NULL,
  ts        STRING,
  tipo      STRING,
  payload   JSON
)
WHERE tipo IN ('leitura', 'alarme');
```

Aceita array JSON ou NDJSON pelo Content-Type. Linha que viola o schema ⇒ `422`
apontando a linha. O `serve` pode gerar a documentação da rota a partir do schema.

### Contrato de status — exit codes viram HTTP

| exit (batch) | HTTP (serve) | significado |
|---|---|---|
| `0` | `200` + summary JSON | sucesso |
| `1` | `422` | erro permanente (script/dados) — não re-tentar |
| `75` (`EX_TEMPFAIL`) | `503` + `Retry-After` | transiente — re-tentar |

### Idempotência via label

Cliente envia `X-Basalt-Label`; o basalt usa como label do stream load. Re-envio
com o mesmo label ⇒ StarRocks recusa a duplicata ⇒ basalt responde `200`
idempotente. **Retry do cliente + label = entrega efetivamente-única**, sem
código novo de deduplicação.

Limite honesto do modo síncrono: não há buffer — StarRocks fora ⇒ `503` e o
cliente é a fila. Para webhook interno e telemetria, suficiente; para absorver
rajadas ou produtores sem retry, ver §10.

---

## 10. Buffer durável (WAL em JSONL)

Transforma o endpoint em fila: **`200` passa a significar "aceito duravelmente"**
(ack após fsync), e a carga vira assíncrona.

```sql
CREATE ENDPOINT '/eventos'
  DOC 'Recebe telemetria; ack após persistir em disco'
  ACCEPT BODY (
    device_id STRING NOT NULL,
    ts        STRING,
    tipo      STRING,
    payload   JSON
  )
  INTO BUFFER 'eventos'
    AT '/var/lib/basalt/wal'
    SEGMENT 16 MB
    RETAIN UNTIL LOADED;          -- ou: RETAIN 24 HOURS (permite reprocesso)

LOAD INTO sr.bronze.eventos
  USING stream_load
FROM BUFFER 'eventos'
  FLUSH EVERY 5 SECONDS OR 50000 ROWS
AS
SELECT device_id, CAST(ts AS TIMESTAMP) AS ts, tipo, payload, now() AS recebido_em
WHERE tipo IN ('leitura', 'alarme');
```

Mecânica:

- **Segmentos append-only** JSONL com nome determinístico
  (`eventos-000042.jsonl`), rotação por tamanho.
- **Label derivado do segmento** (`eventos-000042`): crash entre "carregou" e
  "marcou concluído" ⇒ replay re-envia o mesmo label ⇒ StarRocks deduplica ⇒
  exatamente-uma-vez sem two-phase commit.
- **Group commit**: acumula requests ~5–10 ms, um fsync, ack conjunto (técnica
  Postgres/Kafka).
- **Backpressure**: disco no limite configurado ⇒ `503 + Retry-After`. Queda de
  20 min do StarRocks vira não-evento: WAL acumula, flusher re-tenta com
  backoff, drena na volta.
- `FROM BUFFER` é **só mais uma fonte** — um `basalt run` batch pode reprocessar
  um buffer `RETAIN 24 HOURS`. A fila entrou na linguagem sem subsistema novo.

**Custo honesto:** o `serve` vira estateful — o diretório do WAL precisa de
volume persistente, e a durabilidade é a do disco do nó (não replicado). Para
telemetria/webhook, aceitável; para dado financeiro crítico, é o limite.

**Roadmap (v2):** roteamento por conteúdo —
`LOAD INTO sr.bronze.'eventos_${tipo}'` particionando o batch por coluna, cada
grupo num stream load próprio (o endpoint vira um pequeno exchange de tópicos).

---

## 11. Construções sem grafia natural em SQL — design obrigatório

Com a remoção total do BSL (não há dialeto de fallback), as construções abaixo
**precisam** de grafia SQL fechada dentro do escopo v0.2.0 — a alternativa é
remover a capacidade do engine, o que os scripts de produção existentes (fan-out
Protheus/CRM) não permitem.

| construção BSL | grafia Basalt SQL | nota |
|---|---|---|
| `for v,... in <fonte>` (fan-out plan-time) | `FOR EACH ROW OF (query \| $json.path) AS (v1, v2:INT, ...) [PARALLEL] [ON ERROR CONTINUE \| STOP] <corpo> END FOR` | corpo = um ou mais statements (`LOAD INTO`/SELECT terminal); `${v}` interpola em alvos/strings como hoje. `EACH TABLE OF` (§8) continua cobrindo o caso union — este é o caso geral |
| `match` *statement* (dispatch de pipeline) | `CASE WHEN <guard> THEN <statements> [WHEN ...] [ELSE <statements>] END CASE` — statement-level, plan-time, tipicamente dentro do `FOR EACH ROW OF` | precedente: `CASE` procedural do PL/pgSQL / SQL/PSM (`END CASE` desambigua do `CASE` expressão). Substitui o corpo-`match` do `for`; a resposta do mercado (templating Jinja/dbt) segue sendo a doença a evitar |
| interpolação `${...}` em alvos | mantida integralmente | targets templateados (`'crm_${lower(name)}'`), chaves de upsert e strings; regras C#-style inalteradas |
| hints HTTP avançados (`login_json`, `oauth2`, `stop_short`...) | `WITH (...)` da fonte REST | sem promoção a cláusula até provarem frequência |

Exemplo do caso de produção (fan-out CRM com dispatch por tipo):

```sql
FOR EACH ROW OF (SELECT name, cols, filtro, kind FROM crm.QUERY($$...$$))
  AS (name, cols, filtro, kind)
  PARALLEL ON ERROR CONTINUE
  CASE
    WHEN kind = 'view' THEN
      LOAD INTO sr.bronze.'crm_${lower(name)}' USING stream_load AS
      SELECT * FROM crm.QUERY($$SELECT ${cols} FROM ${name} WHERE ${filtro}$$);
    ELSE
      LOAD INTO sr.bronze.'crm_${lower(name)}' USING stream_load
        UPSERT ON ('${lower(name)}id') AS
      SELECT *, now() AS extraction_timestamp
      FROM crm.QUERY($$SELECT ${cols} FROM ${name} WHERE ${filtro}$$);
  END CASE
END FOR;
```

O `CASE` statement também tem a **forma com sujeito** (precedente idêntico no
PL/pgSQL), cobrindo o `match <valor>` do BSL — `,` alterna valores num mesmo
`WHEN`, como no PL/pgSQL:

```sql
CASE $env
  WHEN 'prod', 'staging' THEN
    LOAD INTO sr.bronze.orders USING stream_load AS
    SELECT * FROM erp.dbo.orders;
  ELSE
    SELECT * FROM 'sample.csv';          -- SELECT terminal ⇒ stdout
END CASE;
```

> **Por que não `MATCH`:** todo significado consagrado de `MATCH` em SQL é
> outra coisa — `MATCH FULL/PARTIAL/SIMPLE` (FK, SQL-92), `MATCH_RECOGNIZE`
> (padrões sobre sequências de linhas, SQL:2016), `MATCH` de grafos (SQL/PGQ,
> SQL:2023), `MATCH ... AGAINST` (full-text MySQL). Dispatch estrutural em SQL
> sempre foi `CASE`; usar `MATCH` colidiria com a leitura esperada do público
> SQL e queimaria a palavra para um eventual `MATCH_RECOGNIZE` futuro.

---

## 12. Estratégia de migração

**Um parser, um plano.** O IR pós-parse (planner, `analyze`, `pushdown`,
executor) não muda — a migração é exclusivamente de superfície. O parser SQL
**substitui** o parser BSL: `src/lang/lexer.zig` + `parser.zig` são reescritos
para o dialeto novo, e o suporte a `.bsl` é removido da árvore na mesma entrega.
Não há período de dois dialetos — todo script existente é reescrito em Basalt SQL
como parte da migração (a distância curta do §1 é o que torna isso viável).

Sequência dentro da entrega única — **v0.2.0**:

1. **Congelar o critério de equivalência.** Antes de tocar no parser: reescrever
   `examples/` e os scripts de produção representativos em Basalt SQL e capturar
   os planos BSL atuais (`check -s`) como golden files. O parser velho só morre
   depois que o novo reproduz cada plano.
- **Núcleo batch:** `PARAM`, `CREATE CONNECTION` (com convenção de credencial),
  `LOAD INTO ... AS`, CTEs, pushdown implícito, operadores da tabela do §7,
  `LIMIT`, fontes CSV por caminho, SELECT terminal = stdout.
- **Fan-out e dispatch (§11):** `FOR EACH ROW OF`, `CASE ... END CASE`
  statement — obrigatórios nesta entrega, já que o BSL não sobrevive como
  fallback.
- **Union e REST:** `UNION ALL BY NAME`, `ANCHOR SCHEMA`, `EACH TABLE OF`,
  fonte REST com `PAGINATE BY` / `RETRY ON`.
- **HTTP:** `CREATE ENDPOINT`, `FROM BODY`, mapeamento de status, label de
  idempotência.
- **Buffer:** `ACCEPT ... INTO BUFFER`, `FROM BUFFER`, flusher, group commit,
  backpressure.
- **Remoção:** lexer/parser BSL, `language.md` substituído pela referência do
  dialeto novo, exemplos e deploys atualizados (`.bsl` → `.sql`).

Critério de pronto: os golden files do passo 1 — cada script reescrito produz
**plano idêntico** (`check -s`) ao que o BSL produzia; a partir daí o BSL sai.

> **Status (2026-07-23):** gate cumprido (17/17 planos idênticos; `ingest` e
> `bad` com os deltas documentados em `examples/golden/README.md`) e o parser
> BSL foi **removido** — um parser, um plano. Falta do escopo v0.2.0: o buffer
> durável (§10) — ver §13.

---

## 13. Plano de implementação — o que falta do escopo

Ordem sugerida; cada item é independente dos demais.

### 13.1 Buffer durável (§10) — o único subsistema novo

> **Status:** IMPLEMENTADO (fases 1–4; ver `language.md` §"Durable buffer").
> Pendências menores: purge por idade (`RETAIN n HOURS` retém mas não expira),
> knob de gramática para o limite de backpressure (default 1 GiB), e teste de
> integração serve+socket (o ciclo accept→WAL→drain é testado sem socket).

Fases incrementais, cada uma útil sozinha:

1. **WAL writer** (`src/connect/wal.zig`): segmentos JSONL append-only
   (`<nome>-<seq:06>.jsonl`), rotação por tamanho (`SEGMENT n MB`), fsync no
   ack, group commit (janela ~5–10 ms acumulando requests → um fsync). Estado
   do consumo em um arquivo-manifesto (`<nome>.state`: último segmento
   carregado). Sem dependências novas.
2. **Gramática**: `CREATE ENDPOINT ... ACCEPT BODY (schema) INTO BUFFER 'nome'
   AT '<dir>' SEGMENT n MB RETAIN UNTIL LOADED | RETAIN n HOURS;` — estende
   `parseCreate`; `FROM BUFFER 'nome' [FLUSH EVERY n SECONDS OR n ROWS]` como
   fonte em `parseFromSource` (novo `ReadForm.buffer`). A validação de schema
   reusa `types.BodyCol` + `request.validateBody` (já enforced no §9).
3. **Serve**: endpoint com buffer responde 200 **após fsync** (o corpo não
   passa pelo pipeline na request); backpressure = disco no limite → `503 +
   Retry-After`. O flusher é uma thread do serve: drena segmentos completos
   a cada `FLUSH EVERY`, com label do stream load = nome do segmento
   (`eventos-000042`) — crash entre carregar e marcar ⇒ replay com o mesmo
   label ⇒ StarRocks deduplica ⇒ efetivamente-única sem 2PC.
4. **Batch replay**: `basalt run` com `FROM BUFFER` lê segmentos retidos
   (`RETAIN n HOURS`) como fonte comum — nenhum código novo além da fonte.

Riscos a validar cedo: fsync custo por request (medir; group commit resolve),
formato do manifesto sob crash (escrever-e-rename atômico), interação do
flusher com SIGHUP/reload do serve.

### 13.2 Tradutor expr→SQL e pushdown implícito

> **Status:** PARCIAL (implementado o essencial). O tradutor `ast.Expr` → SQL
> da fonte existe (`runtime/pushdown.zig` `translateExpr`): comparações,
> `AND/OR/NOT`, `IS NULL/EMPTY`, `IN`, `LIKE`, `CASE/IF`, `CAST`, e funções
> escalares portáveis (`lower upper length trim substr replace concat coalesce
> starts_with ends_with contains`). O pushdown implícito de um único read está
> ligado: o prefixo de `filter` logo após um `read` SQL desce para o `WHERE`
> da query (`serialWhere` em run.zig; preview em `analyze` → linha `pushdown:`
> no `check -s`). O filtro é sempre MANTIDO (regra superset), então o resultado
> nunca muda — só o volume na rede.
>
> **Falta:** colapsar uma CTE inteiramente de UMA conexão numa única query
> descida (o modelo Trino completo do §7); o join cross-conexão ainda
> materializa o lado menor no engine. O `EACH TABLE OF (SELECT ...)`
> generalizado cai no mesmo bloco. `conn.QUERY($$...$$)` cobre o caso cru
> até lá.

### 13.3 Miudezas

- `FROM HEADER('X-Name')`: hoje o nome é parseado e descartado — ligar ao
  binding de header (campo novo em `ast.Param`).
- União json-form: corpus entry quando houver runtime de teste com corpo
  (o `check` offline não renderiza o param).
- Auth HTTP avançada (`login_json`, `oauth2`): passam por `WITH (...)`;
  validar contra um IdP real e promover a cláusula se a frequência provar.
- `jsonToString` em `request.zig` perde objetos/arrays (`payload JSON` vira
  `""`): serializar com `std.json.Stringify.valueAlloc` para colunas JSON.

---

## Apêndice A — folha de referência rápida

```sql
-- batch é default; http declara:
CREATE ENDPOINT '/rota' DOC '...';

PARAM x INT DEFAULT 7;                    -- $x no corpo; $j.a?.b p/ JSON
PARAM j JSON FROM BODY;

CREATE CONNECTION c TYPE sqlserver OPTIONS (host = '...', database = '...');
                                          -- credencial: C_USER / C_PASS do ambiente

LOAD INTO sr.db.tabela                    -- ou 'arquivo.csv'
  USING stream_load                       -- adaptador físico
  UPSERT ON (k1, k2)                      -- APPEND | REPLACE | CREATE OR APPEND
  SPLIT BY (k1) JOBS 4                    -- paralelismo de carga
  WITH (label_prefix = 'x')               -- knobs raros
AS
WITH etapa AS (
  SELECT * EXCLUDE (lixo), SUBSTR(a,1,2) AS e
  FROM erp.dbo.T                          -- pushdown implícito
    PUSHDOWN($$D_E_L_E_T_ <> '*'$$)       -- explícito: fragmento verbatim na fonte
  WHERE ativo                             -- traduzível ⇒ desce ANDado; senão roda aqui
)
SELECT * FROM etapa
UNION ALL BY NAME                         -- reconciliação por nome
SELECT * FROM 'extra.csv'                 -- CSV por caminho (DuckDB)
ANCHOR SCHEMA erp.dbo.T                   -- autoridade de schema (opcional)
LIMIT 1000;

SELECT 1;                                 -- SELECT terminal ⇒ stdout
```
