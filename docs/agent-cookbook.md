# flume-lib — cookbook for AI agents

**Audience:** an AI coding agent asked to write a **Microsoft Fabric Python notebook** (non-Spark) that loads a REST or GraphQL API into Delta tables with flume-lib. Everything needed to do it is on this page, in decision order. Humans looking for an overview want the [README](../README.md).

**Trust model:** every statement here was taken from the code or from a test of this repository, and every complete config below is validated by `tests/test_docs.py` on each CI run. Where this page and your memory of other libraries disagree, this page wins. The key reference in [section 9](#9-key-reference-generated) is generated from the validator.

---

## 0. Rules

1. **Never invent a config key.** Validation is strict: an unknown key raises `ConfigError` with a "did you mean…" suggestion. The only keys that exist are in [section 9](#9-key-reference-generated).
2. **Call `validate_config(config)` before anything else.** It raises `ConfigError` and nothing else, so one `except ConfigError` covers it.
3. **Never write a secret in the config.** A credential is a reference — `{"env_var": "NAME"}` or `{"keyvault_url": "https://<vault>.vault.azure.net", "secret_name": "<name>"}` — never a literal. A literal in a secret field is refused by validation. Public identifiers (`client_id`, `username`, `consumer_key`, `tenant_id`) may be literals.
4. **Run `run_source(config, dry_run=True)` before the first real run.** It calls the API for real (credentials, pagination, response shape) and writes nothing: no data, no watermark, no `log_runs` row.
5. **`run_source` never raises on a source problem.** A failure is a `RunResult` with `status == "failed"` and a message in `error_message`. **Check `status`, and read `warnings` on a `success`.** (It does re-raise `KeyboardInterrupt` and `SystemExit`, after logging the run.)
6. **A notebook that ignores `status` always ends green.** After the loop, raise if any source failed, so the Fabric pipeline sees the failure.
7. **Give every source a stable `name`.** The watermark and `log_runs` are keyed on it; a renamed source restarts from `initial_value`.
8. **State the reload strategy explicitly.** The default is **append**: rerunning a source duplicates its rows. For a table that mirrors the source use `"write": {"mode": "overwrite"}`; for a changed-object feed use `merge` ([section 4.5](#45-reload-strategy)).
9. **Do not assume an API's pagination or auth — read its documentation or ask.** [Section 4.1](#41-questions-to-ask-the-human-when-the-answer-is-not-in-the-api-documentation) lists what to ask.
10. **Do not reach for code where a key exists.** There is no per-API code in this library: auth, pagination, incremental loading, GraphQL and SQL-over-REST are all configuration.

---

## 1. What the library does

`run_source(config)` makes the HTTP calls described by one source config, locates the JSON records in each response, and writes them to **one Delta table** in the notebook's default lakehouse, in batches. It records the run in a technical table. That is all it does.

**Input:** a `dict` (usually one entry of a JSON file such as `Files/conf/sources.json`).
**Output:** a `RunResult` ([section 6.1](#61-runresult)); rows in `<target_schema>.<target_table>`; one row in `<log_schema>.log_runs`; with `incremental`, a row in `<log_schema>.watermark`.

Facts that shape the code you write:

- **JSON only.** One HTTP call returns one page of JSON. Bulk-export APIs (submit a job, poll, download a CSV/JSONL file), XML, SOAP and CSV responses are not supported.
- **Schema-enabled lakehouse required.** `target_schema` is mandatory (`bronze`, …). Names (`target_schema`, `target_table`) accept letters, digits and underscore only, not starting with a digit.
- **Typing:** each column is typed from all the values of a batch — integers → `bigint`, floats (or ints and floats) → `double`, booleans → `boolean`, anything else → `string`. Nested objects and lists are stored as **one JSON string column**; the library never flattens. Dates and timestamps stay strings unless `write.column_types` declares them. Once a table exists, its column types win: a batch that does not fit fails the run rather than being coerced. A column that cannot be typed is written as text and reported in `warnings`.
- **Every written row carries `_flume_run_id` and `_flume_ingested_at`** (lineage; do not name an API field like that).
- **At-least-once.** Rows are committed by batches of `batch_size` (default 50 000). A run that fails midway leaves its earlier batches in the table. Data is committed **before** its watermark.
- **Technical tables** are created in `log_schema` (default `flume`): `watermark` (`source_name`, `last_value`, `updated_ts`), `log_runs` (`run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — it does **not** carry `warnings`), `run_locks` (only with `single_instance`).
- **Not exactly-once.** An appending source needs de-duplication on `_flume_run_id` by the consumer.

### Public API

```python
from flume_lib import (
    run_source, run_sources, run_source_windows, validate_config,
    read_watermark, write_watermark, reset_watermark,
    compact_technical_tables, retype_columns, resolve_lakehouse_tables_path,
    RunResult, SourcesResult, ConfigError, WindowRunError, RunLockedError, RunSourcesError,
)
```

| Name | Signature | Returns / raises |
|---|---|---|
| `run_source` | `(config, lakehouse_tables_path="/lakehouse/default/Tables", storage_options=None, log_schema="flume", dry_run=False, on_progress=None)` | `RunResult`. Never raises on a source problem. `on_progress(pages, rows)` is called after each page; if it raises it is dropped and `warnings` says so. |
| `validate_config` | `(config)` | `None`. Raises only `ConfigError`. |
| `run_sources` | `(configs, lakehouse_tables_path=…, storage_options=None, log_schema="flume", dry_run=False, max_workers=1, stop_on_failure=False, on_progress=None)` | `SourcesResult`: `.results` (input order), `.succeeded`, `.failed`, `.skipped`, `.rows_loaded`, `.ok`, `.summary()`, `.raise_for_failures()` (raises `RunSourcesError`). `ValueError` before any run on a bad list, a missing or duplicate `name`, or two sources on one table with `max_workers > 1`. A failing source is a failed `RunResult`, never an exception. `on_progress(source_name, pages, rows)`. |
| `run_source_windows` | `(config_factory, until, max_span_days, max_iterations=1000, lakehouse_tables_path=…, storage_options=None, log_schema="flume", on_progress=None)` | `list[RunResult]`. Raises `WindowRunError` (`.results` holds the runs so far) or `ValueError` on bad arguments. |
| `read_watermark` | `(lakehouse_tables_path=…, source_name=None, schema="flume", storage_options=None)` | `str \| None`. `source_name` is required: **call by keyword**. |
| `write_watermark` | `(lakehouse_tables_path=…, source_name=None, last_value=None, schema="flume", storage_options=None)` | `None`. Appends a row. |
| `reset_watermark` | `(source_name, schema="flume", storage_options=None, lakehouse_tables_path=…)` | `int`, rows deleted for that source only. |
| `compact_technical_tables` | `(lakehouse_tables_path=…, schema="flume", storage_options=None, vacuum=False)` | `dict` per table: `rows_pruned`, `files_removed`, `files_added`. |
| `retype_columns` | `(target_schema, target_table, column_types, lakehouse_tables_path=…, storage_options=None)` | `int`, rows rewritten. Changes the type of existing columns (`double`, `string`, `decimal(p,s)`, `date`); every value is checked first, nothing is written if one does not fit. Raises `DeltaWriteError`. |
| `resolve_lakehouse_tables_path` | `(path)` | `str`; in Fabric, the ABFSS URI of the default lakehouse. |

`lakehouse_tables_path` defaults to the notebook's default lakehouse and is resolved to its OneLake ABFSS URI automatically; `storage_options` is only for other storage. Leave both alone in a Fabric notebook.

---

## 2. Fabric notebook facts

- The notebook is a **Python notebook (non-Spark)**, kernel 3.10, 3.11 or 3.12, with a **schema-enabled lakehouse attached as default**. There is no `spark` session and none is needed.
- **Install** from wheels uploaded to the lakehouse, with the version pinned (a notebook cell of its own, run before the imports):

  ```text
  %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.19.0
  ```

  The library is not on PyPI. If the user has not uploaded the wheels, point them to [install.md](install.md); do not write `pip install flume-lib`.
- **Secrets:** use Key Vault references; in Fabric they resolve through `notebookutils` with the notebook identity, which needs the **Get** permission on the vault's secrets. `{"env_var": …}` reads `os.environ`. Resolved values are stripped of surrounding whitespace.
- **Config location:** either a Python dict in the notebook, or a JSON list read from `/lakehouse/default/Files/conf/sources.json`. They are the same thing.
- **Outside Fabric** (local run, tests): pass `lakehouse_tables_path` as a local folder and the Key Vault extra is `flume-lib[azure]`.

---

## 3. Notebook skeleton

Cell 1 — install (see above). Then:

```python
from flume_lib import ConfigError, run_source, run_sources, validate_config

KEYVAULT = "https://<vault-name>.vault.azure.net"

SOURCES = [
    {
        "name": "vendor_clients",
        "base_url": "https://api.vendor.com/v1/clients",
        "auth": {
            "type": "bearer_token",
            "token": {"keyvault_url": KEYVAULT, "secret_name": "vendor-api-token"},
        },
        "pagination": {"type": "offset", "limit": 100, "items_field": "data"},
        "target_schema": "bronze",
        "target_table": "clients",
        "write": {"mode": "overwrite"},
    },
]

# Cell 2 — validate everything before calling anything.
for source in SOURCES:
    try:
        validate_config(source)
    except ConfigError as exc:
        raise SystemExit(f"{source.get('name')}: {exc}")
```

```python
# Cell 3 — dry run: real HTTP calls, nothing written.
for source in SOURCES:
    probe = run_source(source, dry_run=True)
    print(probe.source_name, probe.status, probe.rows_loaded, probe.error_message)
    print(probe.sample)          # first 3 raw records
```

```python
# Cell 4 — the real run, then fail loudly.
outcome = run_sources(SOURCES)
print(outcome)                   # a total, then one line per source
for result in outcome.results:
    for warning in result.warnings:
        print(result.source_name, "warning:", warning)

outcome.raise_for_failures()     # raises RunSourcesError if a source failed or was skipped
```

`run_sources` is the loop around `run_source`, written once: results in the order given, a failing source never stops the others (unless `stop_on_failure=True`), `max_workers` runs sources in parallel, and `raise_for_failures()` fails the notebook — which is what a pipeline sees. Duplicate or missing `name`s are refused before anything runs; with `max_workers > 1` so are two sources writing the same table. For one source, call `run_source` directly.

A dry run walks the **whole** pagination: while testing a large source, add `"max_pages": 2` to `pagination` (reaching it fails the run, which is acceptable for a probe) or test against a narrow filter.

---

## 4. Writing a source config

### 4.1 Questions to ask the human when the answer is not in the API documentation

1. What is the exact endpoint URL, and is it `GET` or `POST`?
2. How does the API authenticate (header, OAuth2 client credentials, a login call, OAuth 1.0a, nothing)? **Where are the secrets stored** — which Key Vault and secret names, or which environment variables?
3. How does the API paginate, and how does a client know the last page?
4. Which field of the response holds the list of records?
5. Should the table be a full reload, an append-only stream, or "only what changed since last time" — and which response field says when a record changed?
6. Which lakehouse schema and table name should receive the data?
7. For money or dates: should they be typed (`decimal`, `date`) or left as text?

Do not guess 2 or 5: a wrong auth fails visibly, a wrong reload strategy silently duplicates or loses data.

### 4.2 Minimal config

Three keys are required: `base_url`, `target_schema`, `target_table`. Everything else has a default.

```json config
{
  "name": "currencies",
  "base_url": "https://api.example.com/v1/reference/currencies",
  "target_schema": "bronze",
  "target_table": "currencies"
}
```

With no `auth` the request is unauthenticated; with no `pagination` it is **one call**; with no `write` it **appends**.

### 4.3 Authentication — match the vendor's wording

| The vendor says | `auth` block |
|---|---|
| "Send `Authorization: Bearer <token>`" | `{"type": "bearer_token", "token": <ref>}` |
| "Send your token in the `X-Whatever` header" | `{"type": "api_key_header", "header_name": "X-Whatever", "key": <ref>}` (default header: `X-API-Key`) |
| `Authorization: token <t>` or another prefix | `bearer_token` + `"value_prefix": "token "` (the trailing space is part of the prefix; `""` sends the bare token) |
| Azure APIM "subscription key" | `{"type": "api_key_header", "header_name": "Ocp-Apim-Subscription-Key", "key": <ref>}` |
| HTTP Basic | `{"type": "basic", "username": "<literal or ref>", "password": <ref>}` |
| "POST your credentials to `/login`, use the token returned" | `{"type": "token_endpoint", "token_url": …, "body": {…}, "token_json_path": …}` |
| OAuth2 "client credentials" / client id + secret | `{"type": "oauth2_client_credentials", "token_url": …, "client_id": …, "client_secret": <ref>}` |
| Any Microsoft API (Graph, Fabric, Business Central) | same, with `"tenant_id"` instead of `token_url`, and `"scope": "<resource>/.default"` |
| The vendor's token `curl` uses `--user '<key>:<secret>'` | the OAuth2 block **plus** `"client_auth": "basic"` — without it the token call answers 401 |
| "OAuth 1.0a", "TBA", "token-based authentication" | `{"type": "oauth1", "consumer_key": …, "consumer_secret": …, "token": …, "token_secret": …, "realm": …}` |
| Nothing | no `auth`, or `{"type": "none"}` |

`<ref>` is `{"keyvault_url": …, "secret_name": …}` or `{"env_var": …}`. An API key that must go in the **query string** has no auth type: ask for the header form (a key in a URL lands in every proxy log); `params` would work but puts the secret in the config, which validation does not stop for `params` — do not do it.

Tokens from `oauth2_client_credentials` and `token_endpoint` are renewed during a run: ahead of expiry when the endpoint announces a lifetime (`expires_in`, or `expires_in_json_path`), otherwise once per page on a 401. Static credentials are never renewed: a 401 there is a configuration error.

A `token_endpoint` with `"method": "GET"` and a secret reference in `body` is refused (the values would travel in the URL). Use POST.

### 4.4 Records and pagination — match the response

First, **where are the records?** `items_field` is a dotted path to the list (`data.orders.edges`). Left out, a top-level JSON list is used as is; otherwise the keys `data`, `items`, `results`, `value` are probed. **Name it anyway**: a vendor who renames the envelope then fails the run instead of loading nothing. `record_field` unwraps each item (`"node"` for GraphQL `{cursor, node}` edges).

| What you observe | `pagination` block |
|---|---|
| `?limit=100&offset=200` (also `$top`/`$skip`, `count`/`start`) | `{"type": "offset", "limit": 100, "limit_param": "limit", "offset_param": "offset"}` |
| `?page=3`, stop on an empty page | `{"type": "page", "page_param": "page"}` |
| `?page=3`, total pages in a **response header** | `{"type": "page", "page_param": "page", "total_pages_header": "X-Total-Pages"}` |
| `?page=3`, total pages in the **body** | `{"type": "page", "page_param": "page", "total_pages_field": "pagination.total_pages"}` (mutually exclusive with the header form) |
| The response holds the **full URL** of the next page | `{"type": "next_link", "next_field": "next"}` — `next_field` is a top-level key, not a dotted path (`"@odata.nextLink"` works because that is the key) |
| The response holds an **opaque cursor** | `{"type": "cursor", "cursor_param": …, "cursor_field": …}` + `"has_more_field"` when the API has a boolean |
| "Pass the last id you saw", or an offset ceiling (some APIs stop at 100 000) | `{"type": "keyset", "key_field": "id", "key_param": "since_id"}` — the source **must be sorted by that key, with unique values** |
| One response (reference data) | `{"type": "none"}` or no block |

Rules the library enforces — design around them:

- **Set `has_more_field` (cursor) or `total_pages_header`/`total_pages_field` (page) whenever the API gives one.** An API that filters server-side after cutting the page returns short or empty pages mid-result; every stop-on-empty rule reads that as the end and the run succeeds with a fraction of the data.
- `size_param` and `page_size` go together (neither has an effect alone).
- A page shorter than the requested size ends pagination, unless no full page was seen yet: one more request is made, and if the API caps the page size below `limit`, pagination continues and a **warning** says which size the API serves.
- `max_pages` / `max_rows` are safety bounds: **reaching one fails the run** (never truncates silently). A page identical to the previous one also fails the run.
- `next_link` only follows URLs on the origin of `base_url` (same scheme, host, port). A legitimate second host goes in `"allowed_hosts": ["cdn.example.com"]`.
- `params` (top level) are sent on **every** call as query parameters. With `next_link` they are sent on the first call only, because the next URL carries its own.

**POST and GraphQL:** set `"method": "POST"` and `"body"`. `pagination.params_in` says where pagination parameters go: `"query"` (default), `"body"` (merged into the JSON body; `params_path` says under which key — GraphQL wants `"variables"`), or `"body_template"` (each parameter whose `{placeholder}` appears in `body` is substituted there, the rest go to the query string). `body_template` needs `body`. A `GET` carrying a `body` is refused unless `"allow_body_on_get": true`.

**Braces in a body** (a compact GraphQL query): list the branches that carry `{placeholders}` in `"template_paths": ["variables"]`; the rest of `body` is sent verbatim. Without it the whole body is scanned and `{id}` fails the run.

**Application errors in a 200** (GraphQL always): add `"errors": {"path": "errors", "retryable_codes": ["THROTTLED"]}`. Without it, an `errors` entry next to valid `data` is reported `success` with part of the data missing. `code_field` defaults to `extensions.code`, `message_field` to `message`.

### 4.5 Reload strategy

| Intent | Config |
|---|---|
| Table mirrors the source (reference data) | `"write": {"mode": "overwrite"}`, no `incremental` |
| Append-only stream (events, logs) | nothing — `append` is the default |
| Only what changed since the last run | `"incremental": {"enabled": true, "field": "<record field>", "param_name": "<query param>"}` |
| …the filter lives in the request body (SQL-over-REST, GraphQL variables) | `"inject": "body_template"`, `"initial_value": …`, `"value_format": "iso_datetime"` (or `numeric`, `iso_date`), with `{watermark}` in `body` |
| …the API dates records with a local offset but filters in UTC | add `"normalize": "utc_iso"` |
| A rerunnable backfill of one window | `"write": {"mode": "replace_where", "replace_where": "<SQL over target columns>"}` |
| An object that comes back changed; keep its latest version | `"write": {"mode": "merge", "merge_keys": ["<id>"]}` |
| The API filters on a date window | `incremental.window` + `advance` + `run_source_windows` ([recipe I](#i-date-window-api-with-merge)) |

How the **watermark** works (`incremental.enabled`): the stored value (or `initial_value` on the first run) is sent as `param_name` — or substituted into `{watermark}` — and after a **successful** run the largest `field` of the loaded records becomes the new watermark. It only moves forward (`allow_regression` opts out). `"checkpoint": true` commits it after each batch so an interrupted run resumes, but **the source must then return rows sorted by `field`**; the library fails the run if a batch goes backwards. `checkpoint` is incompatible with `overwrite` and `replace_where`, compatible with `merge`.

Constraints validation enforces:

- `incremental.enabled` requires `field` and either `param_name` or `"inject": "body_template"`.
- `inject: "body_template"` requires an explicit `value_format` (not `any`) and a `body` — the watermark lands inside a query, and a bare placeholder would otherwise accept `0 OR 1=1`. The value must also contain none of `'`, `"`, `;`, `--`, `/*`, a backslash or a newline.
- `replace_where` is not templated: no `{…}` in it. Build the string in Python, one window per run, from the same bounds as the request. It can only reference columns already in the table and `_flume_*`. Rows outside the predicate make delta-rs refuse the commit.
- **A run that loads zero rows replaces nothing** under `overwrite` and `replace_where` (the target keeps its content and `warnings` says so — an API that is down also answers "0 rows"); `merge` and `append` write nothing, and warn only when `incremental` is enabled. `"fail_on_empty": true` turns an empty run into a failure before the watermark moves.
- Use `merge` rather than `append` for objects that reappear; `merge` collapses duplicate keys inside a batch (last wins), fails on a record missing a key, creates the table on the first batch, and keeps the table's column types. Its cost follows the size of the **table**, not the batch.
- `partition_by` is fixed when the table is created.

### 4.6 Types

`"write": {"column_types": {"amount": "decimal(18,4)", "invoice_date": "date", "account_id": "string"}}`

Only `decimal(precision,scale)` (precision ≤ 38), `double`, `date` and `string` exist; there is **no `timestamp`**. A value that does not fit **fails the run** (more decimals than the scale, too many integer digits, non-numeric text, invalid date, an integer beyond 2^53 for a `double`) — it is never rounded or nulled. Declare types **before the first run**: Delta cannot change a column's type by writing. Declare `double` for any amount that may start with round values (`100`, `250`): otherwise the first batch creates an integer column, and a later `1234.56` fails the run instead of being stored as `1234`. Identifiers are `string`, never `double`.

**The existing table's types are a contract.** A batch that does not fit them fails the run with `DeltaWriteError` before anything is written (floats into a `bigint` column, text into a number…); integers into a `double` column and anything into a `string` column are accepted. To fix a table that froze the wrong type, call `retype_columns("bronze", "table", {"amount": "double"})` between runs.

### 4.7 Limits and safety

| Key | Default | Effect |
|---|---|---|
| `timeout_seconds` | 60 | per HTTP request (silence between bytes, not total time) |
| `retry.max_attempts` | 3 | network errors, 429, 5xx, and `errors.retryable_codes`; other 4xx fail at once. A `Retry-After` header wins over backoff (capped by `retry.max_retry_after_seconds`, 300) |
| `max_response_bytes` | 104 857 600 | cuts one response body off; the run fails |
| `max_run_seconds` | none | wall-clock limit, checked before each request and between chunks |
| `max_requests_per_second` | none | at most that many requests start per second (replays included); set it from the API's documented quota, below it, to avoid the 429 rather than handle it. `retry.jitter` (default true) disperses the backoff |
| `batch_size` | 50 000 | rows per Delta commit; lower it if memory runs out |
| `single_instance` | false | refuses a second run of the same source while one is going (advisory lock in `run_locks`); `lock_ttl_seconds` (default 21 600) must exceed `max_run_seconds` |
| `allow_insecure_http`, `allow_literal_secrets` | false | escape hatches for a trusted network / a throwaway test. **Do not set them** unless the human asks. |

`base_url` and `auth.token_url` must be `https://` URLs without embedded credentials (`localhost` may use `http://`).

---

## 5. Recipes

Each config below is complete and valid; replace URLs, field names and secret names. `<ref>`-style secrets are Key Vault references on a placeholder vault.

### A. Bearer token, offset pagination, incremental by query parameter

```json config
{
  "name": "example_source",
  "base_url": "https://api.example.com/v1/items",
  "auth": {
    "type": "bearer_token",
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "src-token"}
  },
  "pagination": {"type": "offset", "limit": 100, "items_field": "data"},
  "incremental": {"enabled": true, "field": "updated_at", "param_name": "updated_since"},
  "target_schema": "bronze",
  "target_table": "example_source",
  "retry": {"max_attempts": 3, "backoff_multiplier": 1}
}
```

### B. Page numbers with the total in a header, full reload

```json config
{
  "name": "vendor_clients",
  "base_url": "https://api.vendor.com/v1/clients",
  "auth": {
    "type": "bearer_token",
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "vendor-api-token"}
  },
  "pagination": {
    "type": "page",
    "page_param": "page",
    "total_pages_header": "X-Total-Pages",
    "items_field": "data"
  },
  "target_schema": "bronze",
  "target_table": "clients",
  "write": {"mode": "overwrite"}
}
```

### C. OAuth2 service principal against Microsoft Graph (OData `next_link`)

```json config
{
  "name": "graph_users",
  "base_url": "https://graph.microsoft.com/v1.0/users",
  "auth": {
    "type": "oauth2_client_credentials",
    "tenant_id": "00000000-0000-0000-0000-000000000000",
    "client_id": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "graph-sp-client-id"},
    "client_secret": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "graph-sp-client-secret"},
    "scope": "https://graph.microsoft.com/.default"
  },
  "params": {"$select": "id,displayName,mail,userPrincipalName", "$top": 999},
  "pagination": {"type": "next_link", "next_field": "@odata.nextLink", "items_field": "value"},
  "retry": {"max_attempts": 5, "backoff_multiplier": 2},
  "target_schema": "bronze",
  "target_table": "users",
  "write": {"mode": "overwrite"}
}
```

Name the columns with `$select`: without it Graph returns a default property set it may change, and each added property becomes a new column. An OData `$filter` cannot carry a watermark (`query_param` sends the whole value of one parameter): reload fully, or compute a bounded window in the notebook.

### D. Opaque cursor in a flat REST response

```json config
{
  "name": "example_transactions",
  "base_url": "https://api.example.com/v1/transactions",
  "auth": {
    "type": "api_key_header",
    "header_name": "Ocp-Apim-Subscription-Key",
    "key": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "apim-key"}
  },
  "pagination": {
    "type": "cursor",
    "cursor_param": "cursor",
    "cursor_field": "paging.next_cursor",
    "has_more_field": "paging.has_more",
    "limit": 1000,
    "limit_param": "page_size",
    "items_field": "data"
  },
  "target_schema": "bronze",
  "target_table": "transactions"
}
```

### E. GraphQL with a cursor, incremental, throttling in the body

```json config
{
  "name": "commerce_orders",
  "base_url": "https://api.example.com/admin/2026-07/graphql.json",
  "method": "POST",
  "auth": {
    "type": "api_key_header",
    "header_name": "X-Api-Access-Token",
    "key": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "graphql-admin-token"}
  },
  "body": {
    "query": "query Orders($first: Int!, $after: String, $q: String) { orders(first: $first, after: $after, query: $q, sortKey: UPDATED_AT) { edges { node { id name updatedAt } } pageInfo { hasNextPage endCursor } } }",
    "variables": {"q": "updated_at:>'{watermark}'"}
  },
  "template_paths": ["variables"],
  "pagination": {
    "type": "cursor",
    "items_field": "data.orders.edges",
    "record_field": "node",
    "cursor_field": "data.orders.pageInfo.endCursor",
    "has_more_field": "data.orders.pageInfo.hasNextPage",
    "cursor_param": "after",
    "limit": 250,
    "limit_param": "first",
    "params_in": "body",
    "params_path": "variables"
  },
  "incremental": {
    "enabled": true,
    "field": "updatedAt",
    "inject": "body_template",
    "initial_value": "1970-01-01T00:00:00Z",
    "value_format": "iso_datetime"
  },
  "errors": {"path": "errors", "retryable_codes": ["THROTTLED"]},
  "retry": {"max_attempts": 6, "backoff_multiplier": 2},
  "target_schema": "commerce",
  "target_table": "orders"
}
```

The query must declare the variables the pagination injects: `$first: Int!` and `$after: String` (nullable — the first request has no cursor). The library never edits the query text. Nested selections land as JSON string columns.

### F. SQL over REST: OAuth 1.0a, watermark in the SQL, resumable

```json config
{
  "name": "erp_transactions",
  "base_url": "https://api.example.com/services/rest/query/v1/sql",
  "method": "POST",
  "headers": {"Prefer": "transient"},
  "auth": {
    "type": "oauth1",
    "realm": "1234567",
    "signature_method": "HMAC-SHA256",
    "consumer_key": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "api-consumer-key"},
    "consumer_secret": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "api-consumer-secret"},
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "api-token-id"},
    "token_secret": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "api-token-secret"}
  },
  "body": {
    "q": "SELECT id, tranid, TO_CHAR(lastmodifieddate, 'YYYY-MM-DD HH24:MI:SS') AS lastmodified FROM transaction WHERE lastmodifieddate >= TO_TIMESTAMP('{watermark}', 'YYYY-MM-DD HH24:MI:SS') ORDER BY lastmodifieddate"
  },
  "pagination": {"type": "offset", "limit": 1000, "items_field": "items"},
  "incremental": {
    "enabled": true,
    "field": "lastmodified",
    "inject": "body_template",
    "initial_value": "1970-01-01 00:00:00",
    "value_format": "iso_datetime",
    "checkpoint": true
  },
  "timeout_seconds": 180,
  "target_schema": "erp",
  "target_table": "transactions"
}
```

`headers` takes literal, non-secret strings only. The `ORDER BY` on the watermark column is what makes `checkpoint` safe. Project dates through an ISO-ordered format: the watermark is compared in Python and `replace_where` compares strings. For a JSON request body OAuth 1.0a does not sign the body (RFC 5849); a `form` body is signed. Point `base_url` at the final URL — a redirect is not re-signed.

### G. Backfill one month, rerunnable

```json config
{
  "name": "erp_transactions_backfill_2026_03",
  "base_url": "https://api.example.com/services/rest/query/v1/sql",
  "method": "POST",
  "headers": {"Prefer": "transient"},
  "auth": {
    "type": "oauth1",
    "realm": "1234567",
    "consumer_key": {"env_var": "ERP_CONSUMER_KEY"},
    "consumer_secret": {"env_var": "ERP_CONSUMER_SECRET"},
    "token": {"env_var": "ERP_TOKEN_ID"},
    "token_secret": {"env_var": "ERP_TOKEN_SECRET"}
  },
  "body": {
    "q": "SELECT id, TO_CHAR(trandate, 'YYYY-MM-DD') AS trandate_iso FROM transaction WHERE trandate >= TO_DATE('2026-03-01', 'YYYY-MM-DD') AND trandate < TO_DATE('2026-04-01', 'YYYY-MM-DD') ORDER BY id"
  },
  "pagination": {"type": "offset", "limit": 1000, "items_field": "items"},
  "write": {
    "mode": "replace_where",
    "replace_where": "trandate_iso >= '2026-03-01' AND trandate_iso < '2026-04-01'"
  },
  "target_schema": "erp",
  "target_table": "transactions"
}
```

Rerunning leaves exactly one copy of March. Derive the request bounds and the predicate from the same values. Do not combine with `incremental.checkpoint`.

### H. Walk a huge table by keyset (offset capped by the API)

```json config
{
  "name": "erp_transactions_backfill",
  "base_url": "https://api.example.com/services/rest/query/v1/sql",
  "method": "POST",
  "auth": {
    "type": "oauth1",
    "realm": "1234567",
    "consumer_key": {"env_var": "ERP_CONSUMER_KEY"},
    "consumer_secret": {"env_var": "ERP_CONSUMER_SECRET"},
    "token": {"env_var": "ERP_TOKEN_ID"},
    "token_secret": {"env_var": "ERP_TOKEN_SECRET"}
  },
  "body": {"q": "SELECT id, amount FROM transaction WHERE id > {last_id} ORDER BY id"},
  "pagination": {
    "type": "keyset",
    "key_field": "id",
    "key_param": "last_id",
    "params_in": "body_template",
    "value_format": "numeric",
    "initial_value": 0,
    "items_field": "items",
    "limit": 1000,
    "limit_param": "limit",
    "max_pages": 5000,
    "max_rows": 10000000
  },
  "batch_size": 50000,
  "target_schema": "erp",
  "target_table": "transactions"
}
```

With `body_template`, `key_param` names the placeholder, and `value_format` is mandatory (the key comes from the API and lands in a query). `limit` goes to the query string while the key goes into the SQL.

### I. Date-window API with merge

The endpoint takes `Date_From`/`Date_To`, caps the span, and filters on a date that is not a field of the response. `field_max` is unsafe there (an outlier row beyond the window carries the watermark past never-requested days), so the watermark follows the **requested window** (`advance: upper_bound`), `lookback` re-requests late-posted events, and `merge` absorbs the overlap.

```json config
{
  "name": "reports_by_company_acme",
  "base_url": "https://api.example.com/reports/invoices",
  "params": {"format": "json", "Company": "ACME", "Date_To": "2026-09-28"},
  "pagination": {"type": "none", "items_field": "Report_Entry"},
  "incremental": {
    "enabled": true,
    "field": "Last_Activity_Date",
    "param_name": "Date_From",
    "initial_value": "2024-09-01",
    "lookback": 7,
    "window": {"upper_param": "Date_To", "upper_value": "2026-09-28"},
    "advance": "upper_bound"
  },
  "write": {"mode": "merge", "merge_keys": ["Invoice_ID"]},
  "target_schema": "bronze",
  "target_table": "invoices"
}
```

To walk a long period, wrap the config in a function of the window end and let `run_source_windows` loop:

```python
from datetime import date

from flume_lib import WindowRunError, run_source_windows


def config_for(window_end: str) -> dict:
    return {
        "name": "reports_by_company_acme",
        "base_url": "https://api.example.com/reports/invoices",
        "params": {"format": "json", "Company": "ACME", "Date_To": window_end},
        "pagination": {"type": "none", "items_field": "Report_Entry"},
        "incremental": {
            "enabled": True,
            "field": "Last_Activity_Date",
            "param_name": "Date_From",
            "initial_value": "2024-09-01",
            "lookback": 7,
            "window": {"upper_param": "Date_To", "upper_value": window_end},
            "advance": "upper_bound",
        },
        "write": {"mode": "merge", "merge_keys": ["Invoice_ID"]},
        "target_schema": "bronze",
        "target_table": "invoices",
    }


try:
    results = run_source_windows(config_for, until=date.today().isoformat(), max_span_days=31)
except WindowRunError as exc:
    print(exc, [r.error_message for r in exc.results])
    raise
```

Constraints: `window.upper_param` must be a key of `params`, and `upper_value` must equal its value; any `advance` other than `field_max` requires `window`; `lookback` requires a `yyyy-MM-dd` watermark and does not shift `initial_value`. `run_source_windows` raises `WindowRunError` when a run fails, when a run succeeds without advancing the watermark, or after `max_iterations` windows; a rerun resumes from the stored watermark. The config returned by the factory must carry a `name`.

### J. A vendor's own login call

```json config
{
  "name": "vendor_orders",
  "base_url": "https://api.vendor.com/v1/orders",
  "auth": {
    "type": "token_endpoint",
    "token_url": "https://api.vendor.com/v1/auth/login",
    "method": "POST",
    "body": {
      "clientId": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "vendor-client-id"},
      "clientSecret": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "vendor-client-secret"}
    },
    "body_format": "json",
    "token_json_path": "data.jwt",
    "expires_in_json_path": "data.expiresIn",
    "header_name": "Authorization",
    "value_prefix": "Bearer "
  },
  "pagination": {"type": "page", "page_param": "page", "total_pages_field": "meta.total_pages", "items_field": "data"},
  "target_schema": "bronze",
  "target_table": "orders"
}
```

Values of `body` and `headers` in `token_endpoint` are literals **or** references; a field whose name looks like a credential (contains `secret`, `passw`, `token`, `api-key`, `credential`, `authorization`, `signature`) must be a reference.

### K. A GET endpoint that reads its filter from a body

```json config
{
  "name": "incidents",
  "base_url": "https://api.example.com/incidents",
  "method": "GET",
  "allow_body_on_get": true,
  "auth": {
    "type": "bearer_token",
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "api-token"}
  },
  "body": {"updated_from": "{watermark}"},
  "pagination": {"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 100},
  "incremental": {
    "enabled": true,
    "field": "updated_at",
    "inject": "body_template",
    "placeholder": "watermark",
    "initial_value": "2020-01-01T00:00:00Z",
    "value_format": "iso_datetime",
    "normalize": "utc_iso"
  },
  "target_schema": "bronze",
  "target_table": "incidents"
}
```

### L. Typed money and dates

```json config
{
  "name": "ledger_entries",
  "base_url": "https://api.example.com/v1/ledger",
  "auth": {
    "type": "bearer_token",
    "token": {"env_var": "LEDGER_TOKEN"}
  },
  "pagination": {"type": "offset", "limit": 500, "items_field": "entries"},
  "write": {
    "mode": "merge",
    "merge_keys": ["entry_id"],
    "column_types": {"amount": "decimal(18,4)", "booked_on": "date", "entry_id": "string"}
  },
  "target_schema": "finance",
  "target_table": "ledger_entries"
}
```

### M. A scheduled source with every guard on

```json config
{
  "name": "nightly_orders",
  "base_url": "https://api.example.com/v1/orders",
  "auth": {
    "type": "bearer_token",
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "orders-token"}
  },
  "pagination": {
    "type": "page",
    "page_param": "page",
    "total_pages_header": "X-Total-Pages",
    "items_field": "data",
    "max_pages": 2000
  },
  "incremental": {"enabled": true, "field": "updated_at", "param_name": "updated_since", "checkpoint": true},
  "write": {"mode": "merge", "merge_keys": ["id"]},
  "fail_on_empty": true,
  "single_instance": true,
  "lock_ttl_seconds": 14400,
  "max_run_seconds": 7200,
  "max_response_bytes": 52428800,
  "target_schema": "bronze",
  "target_table": "orders"
}
```

`checkpoint` is only correct if the API returns the rows sorted by `updated_at`; if it does not, drop `checkpoint`.

---

## 6. Reading the outcome

### 6.1 `RunResult`

| Field | Meaning |
|---|---|
| `source_name`, `run_id` (UUID), `start_ts`, `end_ts` (ISO 8601 UTC) | identity and timing; `run_id` is also in `_flume_run_id` and in `log_runs` |
| `status` | `"success"` or `"failed"` |
| `rows_loaded` | rows **actually written**, also on a failed run (and rows counted in a dry run) |
| `error_message` | `None` on success; `ExceptionClassName: message` on failure, secrets redacted, at most 2 000 characters |
| `warnings` | `list[str]`: degradations that did not fail the run. **Read it on a `success`.** Also stored in `log_runs.warnings`. |
| `sample` | dry run only: the first 3 raw records |
| `watermark_before`, `watermark_after` | with `incremental`: where the run started and where the next one will start. **Equal means the source did not advance**, whatever `rows_loaded` says |
| `window_end` | with `incremental.window`: the upper bound sent |
| `rows_inserted`, `rows_updated` | `merge` only; `rows_loaded` stays "rows processed" |
| `pages`, `retries`, `token_renewals`, `duration_seconds` | what the run cost: data responses received, requests replayed, tokens obtained again, seconds. All are also columns of `log_runs`, with `target_table`, `write_mode` and `lib_version` |

### 6.2 `error_message` prefixes

| Prefix | Cause | Action |
|---|---|---|
| `ConfigError:` | invalid config; the network was not touched | fix the key it names |
| `AuthError:` / `SecretResolutionError:` | the credential could not be **obtained**: env var unset, Key Vault name wrong, token call refused (the vendor's reply is quoted) | check names and permissions; a 401 from the token call with valid credentials usually means `client_auth` should be `"basic"` |
| `HTTPError: HTTP 401` | credential obtained and refused | token value, `header_name`, `value_prefix` |
| `HTTPError: HTTP 403` | accepted, not authorised | vendor-side scope or role |
| `HTTPError: HTTP 4xx` | the API refused the request; its reply is quoted (500 characters) | fix the request; never retried |
| `RetryableHTTPError:` | 429 or 5xx persisted over `retry.max_attempts` | raise `max_attempts`; a `Retry-After` is already honoured |
| `ConnectionError:`, `Timeout:`, `ReadTimeout:`, `ConnectTimeout:` | network | retried up to `retry.max_attempts`; check the URL and `timeout_seconds` |
| `PaginationError:` | the loop lost its footing: records not found (`items_field`), `Field '…' missing`, cursor not advancing, key not advancing, repeated page, `max_pages`/`max_rows` reached, `next_link` outside the origin of `base_url` | read the message; usually `items_field`, `cursor_field`, `total_pages_*`, or an unsorted source for `keyset` |
| `APIError:` / `RetryableAPIError:` | an error inside a 200 matched your `errors` block | the API's message is quoted |
| `IncrementalError:` | the watermark cannot be computed or advanced: mixed types, unsorted batch with `checkpoint`, bad `lookback` | `incremental.field`, sort order, `value_format` |
| `TemplateError:` | a `{placeholder}` has no variable, or a value is forbidden or does not match `value_format` | placeholders, `template_paths`, `value_format` |
| `DeltaWriteError:` | Delta refused the write: predicate not satisfied by the rows, missing column, partition mismatch, merge key missing from a record, a `column_types` value that does not fit, a batch that does not fit the type the existing table holds for a column | message names the cause; for a type conflict, declare the column in `write.column_types` or call `retype_columns` |
| `EmptyResultError:` | zero rows with `fail_on_empty` | expected if the source was empty |
| `RunLockedError:` | another run of the source holds the lock (`single_instance`) | wait, or rerun; it expires after `lock_ttl_seconds` |
| `ResponseTooLargeError:` | a body exceeded `max_response_bytes` | paginate, or raise the cap |
| `RunTimeoutError:` | `max_run_seconds` exceeded, or `max_requests_per_second` too slow to reach the end of the run within it | raise it or narrow the source; written batches stay |
| `KeyboardInterrupt:` / `SystemExit:` … `run interrupted before completion` | the notebook was cancelled; the exception is re-raised | none |

Plus, appended after ` | `: `écriture log_runs impossible — …` when the `log_runs` row itself could not be written.

### 6.3 Frequent warnings

- *the source returned no rows, nothing was replaced / nothing was written* — an empty source under a replacing mode, or an empty incremental run.
- *the source serves pages of N rows while 'limit' asks for M* — set the limit to N.
- *column '…': text according to a previous batch…* / *values not representable as …, written as text* — a column was frozen to or degraded to text; the typing section of [configuration.md](configuration.md#column-types-in-delta) explains.
- *lakehouse path '…' was not resolved to an ABFSS URI* — pass `lakehouse_tables_path` as `abfss://…`.
- *the run lock could not be released* — it expires on its own.
- *incremental: the new watermark … cannot be compared with the stored one* — fix the stored value or set `allow_regression`.

---

## 7. Operating

```python
from flume_lib import compact_technical_tables, read_watermark, reset_watermark, write_watermark

read_watermark(source_name="my_source")                       # str | None — call by keyword
write_watermark(source_name="my_source", last_value="2026-01-01T00:00:00Z")
reset_watermark("my_source")                                  # rows deleted; the next run restarts from initial_value
compact_technical_tables()                                    # weekly, between runs
```

- `source_name` must equal `config["name"]` exactly; a typo creates an unrelated entry. Do these between runs, not during one.
- A **rerun appends** by default. To reload a window use `replace_where` or `merge`; otherwise de-duplicate on `_flume_run_id`. Rows of a failed run can be removed with `DELETE … WHERE _flume_run_id = '<run_id>'`.
- `compact_technical_tables()` keeps the latest watermark per source, drops long-expired lock rows, merges the small files of `watermark`, `log_runs` and `run_locks`, and never deletes from `log_runs`.
- `log_runs`, `watermark` and `run_locks` are ordinary Delta tables in `log_schema`.

---

## 8. What not to promise

- No XML/CSV/SOAP, no asynchronous export APIs, no exactly-once.
- No `timestamp` column type; no flattening of nested JSON.
- `single_instance` is advisory: it only holds if every run of the source asks for it, and two simultaneous starts may both be refused.
- Reaching `max_pages`/`max_rows` fails the run by design.
- An API whose credential must be in the URL query string cannot be configured safely.

More depth: [configuration.md](configuration.md) (every option, with the reasoning), [cookbook.md](cookbook.md) (watermark operations, windows), [security.md](security.md), runnable notebooks in [`examples/`](../examples/).

---

## 9. Key reference (generated)

Every key the validator accepts, its block, whether it is required, and its default. Written by `scripts/gen_key_index.py` and checked in CI. `Required` reads: `yes` — always; `one of` — at least one key of its group; `conditional` — required by another key of the same block; `—` — optional.

<!-- BEGIN GENERATED KEY INDEX -->

<!-- Généré par scripts/gen_key_index.py — ne pas éditer à la main. -->

#### Top level

| Key | Required | Default | Note |
|---|---|---|---|
| `base_url` | yes | — |  |
| `target_schema` | yes | — |  |
| `target_table` | yes | — |  |
| `name` | — | `"<sans_nom>"` | identity in `log_runs` and `watermark` |
| `params` | — | `{}` | fixed query params, sent on every call beside the pagination params |
| `auth` | — | — |  |
| `pagination` | — | — |  |
| `incremental` | — | — |  |
| `retry` | — | — |  |
| `timeout_seconds` | — | `60` |  |
| `method` | — | `"GET"` |  |
| `body` | — | `{}` | rejected on GET — set `"method": "POST"` |
| `body_format` | — | `"json"` |  |
| `headers` | — | `{}` | literal strings only — a credential belongs in `auth` |
| `errors` | — | — |  |
| `template_paths` | — | — | restricts body templating to these branches (GraphQL braces) |
| `batch_size` | — | `50 000` | rows per Delta commit; caps the memory a run uses |
| `write` | — | — |  |
| `allow_body_on_get` | — | — | lets a GET carry a body — refused by default, most APIs ignore it |
| `fail_on_empty` | — | `false` | fails the run when no row was loaded, before the watermark moves |
| `allow_insecure_http` | — | — | accepts plain `http://` for `base_url` / `token_url`; `localhost` is always accepted |
| `allow_literal_secrets` | — | — | lifts the refusal of a literal secret in `auth` — throwaway tests only |
| `max_response_bytes` | — | `104 857 600` | caps one response body; a streamed read, so a huge or endless body is cut off |
| `max_run_seconds` | — | — | wall-clock limit for the run; checked before each request and between chunks |
| `single_instance` | — | — | refuses a second run of the same source while one is going (advisory lock) |
| `lock_ttl_seconds` | — | — | how long a run's lock lives if the run dies; must exceed `max_run_seconds` |
| `max_requests_per_second` | — | — | at most this many requests start per second, retries included; per run |

#### `pagination` — common to every strategy

| Key | Required | Default | Note |
|---|---|---|---|
| `items_field` | — | — | dotted path; without it, `data`/`items`/`results`/`value` are probed |
| `record_field` | — | — | extracts a sub-object out of each record |
| `params_in` | — | `"query"` | one of `query` / `body` / `body_template` |
| `params_path` | — | — | nests the params inside the body; requires `"params_in": "body"` |
| `max_pages` | — | — | safety bound; the run fails when it is hit |
| `max_rows` | — | — | safety bound; the run fails when it is hit |

#### `pagination` — `"type": "offset"`

| Key | Required | Default | Note |
|---|---|---|---|
| `limit` | — | `100` | also the stop condition — a shorter page ends the loop |
| `limit_param` | — | `"limit"` |  |
| `offset_param` | — | `"offset"` |  |

#### `pagination` — `"type": "page"`

| Key | Required | Default | Note |
|---|---|---|---|
| `page_param` | — | `"page"` |  |
| `start_page` | — | `1` |  |
| `size_param` | — | — | no effect unless `page_size` is set too — the pair goes together |
| `page_size` | — | — | no effect unless `size_param` is set too |
| `total_pages_header` | — | — | read from the first response; absent or non-numeric fails the run |
| `total_pages_field` | — | — | same, read by dotted path in the body; excludes `total_pages_header` |

#### `pagination` — `"type": "cursor"`

| Key | Required | Default | Note |
|---|---|---|---|
| `cursor_param` | conditional | — |  |
| `cursor_field` | conditional | — |  |
| `has_more_field` | — | — | set it whenever the API provides it — an empty mid-result page otherwise reads as the end |
| `limit` | — | — |  |
| `limit_param` | — | `"limit"` |  |

#### `pagination` — `"type": "keyset"`

| Key | Required | Default | Note |
|---|---|---|---|
| `key_field` | conditional | — |  |
| `key_param` | conditional | — |  |
| `initial_value` | — | — | floor of the first request |
| `value_format` | — | `"any"` | one of `any` / `numeric` / `iso_date` / `iso_datetime`; an explicit value is mandatory with `body_template` |
| `limit` | — | — |  |
| `limit_param` | — | `"limit"` |  |

#### `pagination` — `"type": "next_link"`

| Key | Required | Default | Note |
|---|---|---|---|
| `next_field` | — | `"next"` | top-level key, not a dotted path |
| `allowed_hosts` | — | — | bare hosts the next-page URL may name besides `base_url`'s own; others fail the run |

#### `auth` — `"type": "bearer_token"`

| Key | Required | Default | Note |
|---|---|---|---|
| `token` | one of | — |  |
| `token_env_var` | one of | — |  |
| `header_name` | — | `"Authorization"` |  |
| `value_prefix` | — | `"Bearer "` | raw concatenation, trailing space included; `""` sends the bare token |

#### `auth` — `"type": "api_key_header"`

| Key | Required | Default | Note |
|---|---|---|---|
| `key` | one of | — |  |
| `key_env_var` | one of | — |  |
| `header_name` | — | `"X-API-Key"` |  |

#### `auth` — `"type": "basic"`

| Key | Required | Default | Note |
|---|---|---|---|
| `username` | one of | — |  |
| `password` | one of | — |  |
| `username_env_var` | one of | — |  |
| `password_env_var` | one of | — |  |

#### `auth` — `"type": "oauth2_client_credentials"`

| Key | Required | Default | Note |
|---|---|---|---|
| `token_url` | one of | — |  |
| `tenant_id` | one of | — | Entra ID shortcut; builds the token URL instead of `token_url` |
| `client_id` | one of | — |  |
| `client_secret` | one of | — |  |
| `client_id_env_var` | one of | — |  |
| `client_secret_env_var` | one of | — |  |
| `scope` | — | — |  |
| `client_auth` | — | `"body"` |  |
| `urlencode_credentials` | — | — | form-urlencodes id and secret in the Basic header (RFC 6749 §2.3.1); needs `"client_auth": "basic"` |
| `timeout_seconds` | — | `30` | of the token call only |

#### `auth` — `"type": "token_endpoint"`

| Key | Required | Default | Note |
|---|---|---|---|
| `token_url` | yes | — |  |
| `method` | — | `"POST"` |  |
| `body` | — | `{}` |  |
| `body_format` | — | `"json"` |  |
| `headers` | — | `{}` |  |
| `token_json_path` | — | `"access_token"` |  |
| `expires_in_json_path` | — | — | without it, the token is only renewed after a 401 |
| `header_name` | — | `"Authorization"` |  |
| `value_prefix` | — | `"Bearer "` |  |
| `timeout_seconds` | — | `30` | of the login call only, independent of the data calls |

#### `auth` — `"type": "oauth1"`

| Key | Required | Default | Note |
|---|---|---|---|
| `consumer_key` | one of | — |  |
| `consumer_secret` | one of | — |  |
| `token` | — | — | goes with `token_secret`; omitting both gives the two-legged flavor |
| `token_secret` | — | — | goes with `token` |
| `consumer_key_env_var` | one of | — |  |
| `consumer_secret_env_var` | one of | — |  |
| `token_env_var` | — | — |  |
| `token_secret_env_var` | — | — |  |
| `realm` | — | — | sent in the header, outside the signature |
| `signature_method` | — | `"HMAC-SHA256"` | one of `HMAC-SHA1` / `HMAC-SHA256` |

#### `incremental`

| Key | Required | Default | Note |
|---|---|---|---|
| `enabled` | — | — |  |
| `field` | conditional | — | field of the *record*; its max becomes the next watermark |
| `param_name` | conditional | — |  |
| `inject` | — | `"query_param"` | one of `query_param` / `body_template` |
| `placeholder` | — | `"watermark"` | name substituted in `body` with `"inject": "body_template"` |
| `initial_value` | — | — |  |
| `value_format` | — | `"any"` | one of `any` / `numeric` / `iso_date` / `iso_datetime` |
| `normalize` | — | `"none"` | one of `none` / `utc_iso`; reshapes the watermark before it is **sent**, not as stored |
| `checkpoint` | — | — | commits the watermark batch by batch; incompatible with a replacing `write.mode` |
| `allow_regression` | — | `false` | lets a run lower the stored watermark; off, it only ever moves forward |
| `window` | — | — | declares the upper bound sent in `params`; needed by any `advance` but `field_max` |
| `advance` | — | `"field_max"` | one of `field_max` / `field_max_capped` / `upper_bound`; what the watermark becomes — the last two need `window` |
| `lookback` | — | — | days; sends `watermark - lookback`, stores the watermark unshifted; needs a `yyyy-MM-dd` watermark |

#### `incremental.window`

| Key | Required | Default | Note |
|---|---|---|---|
| `upper_param` | yes | — | must be a key of `params` |
| `upper_value` | yes | — | must equal `params[upper_param]`; at least the watermark, checked at run time |

#### `write`

| Key | Required | Default | Note |
|---|---|---|---|
| `mode` | — | `"append"` | one of `append` / `overwrite` / `replace_where` / `merge` |
| `replace_where` | conditional | — | SQL predicate over the target table; not templated |
| `partition_by` | — | — | Delta partition columns |
| `merge_keys` | conditional | — | columns identifying a row; required with `merge`, which accepts `checkpoint` |
| `when_matched` | — | — | only `update` for now, which is also the default |
| `column_types` | — | — | imposed types: `decimal(p,s)`, `double`, `date`, `string`; a value that does not fit fails the run |

#### `retry`

| Key | Required | Default | Note |
|---|---|---|---|
| `max_attempts` | — | `3` |  |
| `backoff_multiplier` | — | `1` |  |
| `max_retry_after_seconds` | — | `300` | ceiling on a `Retry-After` the server asks for |
| `jitter` | — | `true` | spreads the backoff between half and all of the delay; never a `Retry-After` |

#### `errors`

| Key | Required | Default | Note |
|---|---|---|---|
| `path` | — | `"errors"` | where the error envelope sits in a 200 response |
| `code_field` | — | `"extensions.code"` |  |
| `message_field` | — | `"message"` |  |
| `retryable_codes` | — | — | codes retried like a 5xx instead of failing the run |

<!-- END GENERATED KEY INDEX -->
