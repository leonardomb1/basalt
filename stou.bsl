@batch

param catalog json

connection stou = http
  base_url = "https://awstou.ifractal.com.br"
  auth = "header"
  header_name = "token"
  header_value = env("IFR_TOKEN")

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("SR_USER") password = secret("SR_PASSWORD")
  database = "bronze"

for endpoint, key, p_from, p_to, dtde, dtate in catalog @[mode = parallel, on_error = continue]
  read stou "/ripbr/rest/" @[method = post, header = "user: integracao",
      body = "pag=${endpoint}&cmd=get&${p_from}=${dtde}&${p_to}=${dtate}&start=1",
      items = "itens", paginate = page, page_param = "page",
      total_field = "totalCount", prefetch = 16, timeout_ms = 60000,
      retries = 6, retry_base_ms = 2000, retry_statuses = "404",
      max_pages = 200000]
    | select *, extraction_timestamp = now()
    | write sr stream_load "stou_${endpoint}" upsert on "${key}"
