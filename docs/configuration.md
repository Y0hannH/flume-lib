# Configuration reference

Exhaustive reference of every option: the JSON configuration of a source (key by key, with required/optional status and defaults), the `run_source` parameters, and the secret reference format.

## Source overview

```json
{
  "name": "my_source",
  "base_url": "https://api.example.com/v1/items",
  "method": "GET",
  "params": {"status": "active"},
  "headers": {"Prefer": "transient"},
  "body": { ... },
  "body_format": "json",
  "template_paths": ["variables"],
  "auth": { ... },
  "pagination": { ... },
  "incremental": { ... },
  "errors": { ... },
  "target_schema": "bronze",
  "target_table": "my_source",
  "retry": { ... },
  "timeout_seconds": 60
}
```

| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | no | `<sans_nom>` | Source identifier — used in `watermark`, `log_runs` and `RunResult`. Strongly recommended. |
| `base_url` | string | **yes** | — | Data endpoint URL. |
| `params` | object | no | `{}` | Fixed query params added to every request (e.g. filters). |
| `auth` | object | no | no auth | See [Auth](#auth). Absent or `{"type": "none"}` = unauthenticated requests. |
| `pagination` | object | no | single call | See [Pagination](#pagination). Absent or `{"type": "none"}` = one HTTP call. |
| `incremental` | object | no | disabled | See [Incremental](#incremental-watermark). |
| `target_schema` | string | **yes** | — | Destination schema for the data (schema-enabled lakehouse required). Letters/digits/underscore only. |
| `target_table` | string | **yes** | — | Destination table. Written in `append` mode with `schema_mode=merge`. Letters/digits/underscore only. |
| `retry` | object | no | see [Retry](#retry) | HTTP retry policy. |
| `timeout_seconds` | number | no | `60` | Timeout of each data HTTP request. |
| `method` | string | no | `GET` | HTTP method of the data calls: `GET`, `POST`, `PUT`, `PATCH`. |
| `body` | object | no | `{}` | Request body sent on every data call. Requires a non-`GET` `method`. |
| `body_format` | string | no | `json` | `json` or `form` encoding of `body`. |
| `headers` | object | no | `{}` | Fixed HTTP headers added to every data call (literal strings only). Auth headers always win over these. For a credential, use `auth` — never put a secret here. |
| `errors` | object | no | disabled | Application-error envelope of APIs that report failures inside a successful HTTP response. See [Application errors in a 200](#application-errors-in-a-200). |
| `template_paths` | array | no | whole body | Restricts `{placeholder}` substitution to these dotted paths of `body`. See [Body templating](#body-templating). |

**Configuration is validated strictly**: an unknown key raises a `ConfigError` (with a "did you mean…" suggestion) instead of being ignored. This is deliberate — a typo on an optional key such as `pagintaion` used to silently disable pagination, producing a run reported as `success` with most of the data missing. Validate ahead of a long batch with:

```python
from flume_lib import validate_config, ConfigError

for source in sources:
    try:
        validate_config(source)
    except ConfigError as exc:
        print(f"{source.get('name')}: {exc}")
```

### POST endpoints

Search and reporting APIs often require a POST with a JSON payload. Set `method` and `body`; use `pagination.params_in` to say whether pagination and incremental params belong in the query string (default) or inside the payload:

```json
{
  "base_url": "https://api.example.com/v1/search",
  "method": "POST",
  "body": {"query": "status:active", "fields": ["id", "updated_at"]},
  "pagination": {"type": "offset", "limit": 200, "params_in": "body"},
  "target_schema": "bronze",
  "target_table": "search_results"
}
```

With `"params_in": "body"` the request payload for the second page is `{"query": …, "fields": …, "limit": 200, "offset": 200}`. With the default `"query"`, `body` stays constant and the pagination params go to the query string.

When the API expects those params nested rather than at the root, `pagination.params_path` says where to put them — GraphQL wants the page size and cursor **inside** `variables`, not at the root beside `query`:

```json
{
  "method": "POST",
  "body": {"query": "query($first: Int!, $after: String) { … }", "variables": {}},
  "pagination": {"type": "cursor", "params_in": "body", "params_path": "variables", "…": "…"}
}
```

The branch is created if `body` does not already carry it. A `params_path` that traverses something other than an object in `body` is rejected by validation.

### Static headers

Some APIs require a header that is not authentication. `headers` adds fixed headers to every data call:

```json
{
  "base_url": "https://1234567.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql",
  "headers": {"Prefer": "transient"},
  "method": "POST",
  "body": {"q": "SELECT id FROM customer"},
  "target_schema": "bronze",
  "target_table": "customers"
}
```

Values must be literal strings. A secret reference (`{"env_var": …}`) is rejected by validation: credentials belong in `auth`, which also keeps them out of the config file. Headers produced by `auth` are applied last and cannot be overridden from `headers`.

### Body templating

Some APIs carry their filter inside the request body rather than in a query param — an SQL-over-REST endpoint is the canonical case. Any string in `body` (or `params`) may contain `{placeholder}` markers, substituted at run start:

```json
{
  "method": "POST",
  "body": {"q": "SELECT id, updated FROM t WHERE updated >= '{watermark}' ORDER BY id"},
  "incremental": {
    "enabled": true,
    "field": "updated",
    "inject": "body_template",
    "initial_value": "1970-01-01 00:00:00",
    "value_format": "iso_datetime"
  }
}
```

Rules:

- Only known placeholders are substituted. A `{name}` with no matching variable fails the run — same reasoning as strict config validation: a typo must not reach the API silently.
- When no variable is in play (no `inject: "body_template"`), strings are left untouched, braces included.
- Interpolated values are checked: a value containing `'`, `"`, `;`, `--`, `/*`, a backslash or a newline is rejected, so a value can never change the structure of the query it lands in.
- **An explicit `value_format` is required** with `inject: "body_template"`. Character filtering only protects a placeholder that sits inside quotes; a bare one (`WHERE id > {last_id}`) would happily accept `0 OR 1=1`, which contains no forbidden character. Declaring `numeric`, `iso_date` or `iso_datetime` closes that — and a legitimate watermark is always a number or a date.

#### Restricting the templating to part of the body

A body that already uses braces for its own syntax cannot be scanned wholesale: a compact GraphQL query (`{orders{edges{node{id}}}}`) contains `{id}`, which is indistinguishable from a placeholder and fails the run. `template_paths` lists the dotted paths of `body` that carry placeholders; everything else is sent verbatim.

```json
{
  "method": "POST",
  "body": {
    "query": "query($q: String) { orders(query: $q) { … } }",
    "variables": {"q": "updated_at:>'{watermark}'"}
  },
  "template_paths": ["variables"]
}
```

A path listed here must exist in `body` — a typo is a `ConfigError`, not a silently un-templated branch.

### Application errors in a 200

Some APIs — every GraphQL endpoint, and a few SOAP-descended REST ones — report failures inside a successful HTTP response. `errors` declares that envelope so those responses fail the run instead of passing for data:

```json
{
  "errors": {
    "path": "errors",
    "code_field": "extensions.code",
    "message_field": "message",
    "retryable_codes": ["THROTTLED"]
  }
}
```

| Key | Default | Description |
|---|---|---|
| `path` | `errors` | Dotted path to the error list (or single error object). Absent or empty ⇒ the response is fine. |
| `code_field` | `extensions.code` | Dotted path, inside one error, to its machine-readable code. |
| `message_field` | `message` | Dotted path, inside one error, to its human-readable message. |
| `retryable_codes` | `[]` | Codes that mean "transient": the request is replayed under the `retry` policy instead of failing the run. |

The defaults follow the GraphQL specification (`errors[].message`, `errors[].extensions`), so a GraphQL source usually only needs `path` and `retryable_codes`. `path` may also resolve to a single error object rather than a list; it is then treated as one error.

Scope and timing:

- **Data calls only.** The token calls of `oauth2_client_credentials` and `token_endpoint` are not covered — they have their own error handling, driven by the HTTP status.
- **Checked on every page**, before the records are extracted. An error on page 7 of 12 fails the run, and nothing is written: `run_source` accumulates all pages and writes once at the end.
- **Exercised by `dry_run=True`**, like the rest of the request path — a missing scope surfaces without writing anything.

Without this block, two things go wrong. A **partial** failure — valid `data` next to an `errors` entry, e.g. one field refused by a missing scope — is reported `success` while quietly missing part of what was asked for. A **total** failure surfaces only as a pagination error ("Impossible de localiser les enregistrements"), which says nothing about the actual cause. The API's own message is what ends up in `log_runs`, truncated to 500 characters over at most 3 errors — an error message quoting the whole query would otherwise be stored page after page.

`retryable_codes` is also how throttling is caught on APIs that announce it in the body rather than with a 429 (Shopify's cost-based limiter does exactly that). A `Retry-After` header on such a response is still honored.

## GraphQL endpoints

GraphQL needs no dedicated source type: it is a POST of a JSON body to a single URL. Five generic options do the work, and this section puts them together. The worked example is Shopify's Admin API, but nothing below is Shopify-specific — any Relay connection has the same shape.

| GraphQL concept | Option that covers it |
|---|---|
| Single endpoint, POST, `{query, variables}` | `base_url` + `method: "POST"` + `body` |
| Records under `data.<connection>.edges` | `pagination.items_field` (dotted path) |
| Each record wrapped in `{cursor, node}` | `pagination.record_field: "node"` |
| `first` / `after` belong in `variables` | `pagination.params_in: "body"` + `params_path: "variables"` |
| `pageInfo.hasNextPage` / `endCursor` | `pagination.type: "cursor"` + `has_more_field` / `cursor_field` |
| Braces in the query vs. `{placeholder}` | `template_paths` |
| Errors returned with HTTP 200 | `errors` |
| Cost-based throttling in the body | `errors.retryable_codes: ["THROTTLED"]` |

### Full source

```json
{
  "name": "shopify_orders",
  "base_url": "https://my-shop.myshopify.com/admin/api/2026-07/graphql.json",
  "method": "POST",
  "auth": {
    "type": "api_key_header",
    "header_name": "X-Shopify-Access-Token",
    "key": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "shopify-admin-token"}
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
  "target_schema": "shopify",
  "target_table": "orders"
}
```

The request bodies this produces:

```jsonc
// first page — no cursor yet
{"query": "query Orders(…) {…}", "variables": {"q": "updated_at:>'1970-01-01T00:00:00Z'", "first": 250}}
// second page
{"query": "query Orders(…) {…}", "variables": {"q": "updated_at:>'1970-01-01T00:00:00Z'", "first": 250, "after": "eyJsYXN0X2lkIjo…"}}
```

### Writing the query

- **Declare the variables the pagination injects.** `$first` and `$after` are merged into `variables` at request time, so the query must declare them — but the library never edits the query text. `$first: Int!` is safe because a `limit` is sent on every call; `$after` must stay nullable (`String`), since the first request goes without it.
- **Sort on the watermark field** (`sortKey: UPDATED_AT` here). Not required, but it makes a failed run resumable at roughly the right place.
- **Keep the filter in a variable**, not inlined in the query text. That is what makes `template_paths: ["variables"]` sufficient, and it keeps the interpolated watermark under the `value_format` check.
- **Braces**: with `template_paths` set, the query text is never scanned for placeholders and may be written in any style. Without it, a compact `{id}` would fail the run.

### Nested selections in Delta

A GraphQL selection is a tree; a Delta table is flat. Objects and lists (`customer { id email }`, `lineItems { edges { … } }`) are serialized to **one JSON string column each**, named after the field. Scalars become typed columns. Two consequences:

- Query them downstream with the JSON functions of whatever reads the table; the library does not flatten.
- To get real columns, ask for scalars in the query (`customer { id }` → still one JSON column, but a shallow one), or split the connection into its own source.

### Common failure modes

| Symptom | Cause |
|---|---|
| `Impossible de localiser les enregistrements` | `items_field` missing on a GraphQL source — `data` is an object, so the default probing finds nothing. |
| `Champ 'data.x.edges' : liste attendue, NoneType reçu` | The connection returned `null` — usually an error in the response the `errors` block would have named. Add it. |
| `placeholder '{id}' sans variable correspondante` | The query text is being scanned for placeholders: add `template_paths`. |
| `Champ 'node' absent d'un enregistrement` | `record_field` set on a connection that is not edge-wrapped (some APIs expose `nodes` directly — then drop `record_field` and point `items_field` at `nodes`). |
| Run `success` with fewer rows than expected | An `errors` entry alongside valid `data`, with no `errors` block declared. |
| `THROTTLED` even after retries | The cost limiter, not the request rate: lower `limit`, or ask for fewer fields. |

Working notebook, auth to Delta: [examples/shopify_graphql.py](../examples/shopify_graphql.py).

## `run_source` parameters

```python
run_source(config, lakehouse_tables_path=..., storage_options=..., log_schema=..., dry_run=False)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | dict | — | One entry of the configuration JSON (see above). |
| `lakehouse_tables_path` | str | `/lakehouse/default/Tables` | Tables root. In Fabric, the default is automatically resolved to the OneLake ABFSS URI of the notebook's default lakehouse. Can be an explicit `abfss://...` URI to target another lakehouse. |
| `storage_options` | dict \| None | `None` | Passed through to delta-rs. If absent with an `abfss://` URI, a storage bearer token is obtained via `notebookutils`. |
| `log_schema` | str | `flume` | Schema of the technical tables `watermark` and `log_runs`. |
| `dry_run` | bool | `False` | See [Dry run](#dry-run). |

Returns a `RunResult` with `source_name`, `status` (`success`/`failed`), `rows_loaded`, `error_message` (None on success), `start_ts`, `end_ts` (ISO 8601 UTC), `run_id` (UUID), and `sample` (dry run only). **Never raises.**

### Dry run

`run_source(config, dry_run=True)` validates the configuration, **really calls the API** — so credentials, pagination and the response shape are all exercised — counts the rows, and writes nothing: no data, no watermark advance, no `log_runs` entry. The first few records are returned raw (without lineage columns) in `RunResult.sample`.

```python
result = run_source(config, dry_run=True)
print(result.status, result.rows_loaded, result.error_message)
print(result.sample)
```

Rows are counted as they stream, never accumulated, so a dry run is safe on a source of any size. Note that it does not exercise the write path: schema conflicts on the target table only surface on a real run.

### Lineage columns

Every written row carries two extra columns:

| Column | Content |
|---|---|
| `_flume_run_id` | UUID of the run — same value as `RunResult.run_id` and the matching `log_runs` row. |
| `_flume_ingested_at` | ISO 8601 UTC timestamp of the write. |

They make it possible to trace a row back to the run that produced it, de-duplicate after a partial retry, or isolate the rows of a bad run. The `_flume_` prefix avoids collisions with API fields.

## Secret references

Wherever a value is marked *secret ref*, three forms are accepted:

| Form | Example | Usage |
|---|---|---|
| Environment variable | `{"env_var": "MY_TOKEN"}` | Value injected before the call (os.environ). |
| Azure Key Vault | `{"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "my-secret"}` | Resolved via `notebookutils.credentials.getSecret` in Fabric (notebook identity — *Get* permission on secrets required), or `azure-identity`/`DefaultAzureCredential` outside Fabric (`pip install flume-lib[azure]`). |
| Literal | `"value"` | **Non-sensitive values only** (public username, `grant_type`, scope…). Never a password or token. |

Resolved values are automatically stripped of leading/trailing whitespace and newlines (a classic source of opaque 401s).

Legacy form still supported: `token_env_var`, `key_env_var`, `username_env_var`, `password_env_var` (equivalent to `{"env_var": ...}` on the matching key).

## Auth

The type is selected by `auth.type`. The token is obtained **once per run** (no mid-run refresh).

### `bearer_token` — static token

| Key | Required | Default | Description |
|---|---|---|---|
| `token` | **yes** | — | Secret ref of the token. |
| `header_name` | no | `Authorization` | Header carrying the token. |
| `value_prefix` | no | `Bearer ` (with a space) | Prefix before the token. `""` to send the raw token. |

```json
{"type": "bearer_token", "token": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-token"}}
```

Custom header (API expecting the raw token in a specific header):

```json
{"type": "bearer_token", "token": {"env_var": "TOKEN"}, "header_name": "X-Access-Token", "value_prefix": ""}
```

### `api_key_header` — API key in a header

| Key | Required | Default | Description |
|---|---|---|---|
| `key` | **yes** | — | Secret ref of the key. |
| `header_name` | no | `X-API-Key` | Header carrying the key (e.g. `Ocp-Apim-Subscription-Key`). |

```json
{"type": "api_key_header", "key": {"env_var": "MY_KEY"}, "header_name": "Ocp-Apim-Subscription-Key"}
```

### `basic` — HTTP Basic

| Key | Required | Default | Description |
|---|---|---|---|
| `username` | **yes** | — | Secret ref (or literal if non-sensitive). |
| `password` | **yes** | — | Secret ref. |

```json
{"type": "basic", "username": "svc_flume", "password": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "pwd"}}
```

### `oauth2_client_credentials` — standard OAuth2 flow (including Entra ID service principals)

Sends `grant_type=client_credentials` + `client_id` + `client_secret` (+ `scope` if provided) **form-encoded** to the `token_url`, and expects the token under the `access_token` key of the JSON response. If your endpoint deviates from this standard (different key name, JSON body…), use `token_endpoint` instead.

| Key | Required | Default | Description |
|---|---|---|---|
| `token_url` | yes, unless `tenant_id` | — | IdP token endpoint URL. |
| `tenant_id` | yes, unless `token_url` | — | Entra ID shortcut: derives `token_url = https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`. |
| `client_id` | **yes** | — | Secret ref (or literal — an Entra app id is not a secret). |
| `client_secret` | **yes** | — | Secret ref. |
| `scope` | no | absent | E.g. `https://graph.microsoft.com/.default` (Entra) or a proprietary scope. |
| `timeout_seconds` | no | `30` | Token call timeout. |

Service principal against a Microsoft API:

```json
{
  "type": "oauth2_client_credentials",
  "tenant_id": "00000000-0000-0000-0000-000000000000",
  "client_id": "11111111-1111-1111-1111-111111111111",
  "client_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "sp-secret"},
  "scope": "https://graph.microsoft.com/.default"
}
```

Non-Microsoft IdP with a proprietary scope:

```json
{
  "type": "oauth2_client_credentials",
  "token_url": "https://auth.example.com/token",
  "client_id": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "cid"},
  "client_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "csecret"},
  "scope": "monitoring"
}
```

### `token_endpoint` — arbitrary login call

For any API where the token comes from a non-standard preliminary call: login/password POST, JSON body, token nested in the response, custom output header…

| Key | Required | Default | Description |
|---|---|---|---|
| `token_url` | **yes** | — | URL of the token-obtaining call. |
| `method` | no | `POST` | HTTP method. With `GET`, the `body` goes to query params — **secret refs are rejected in that case** (they would leak into server/proxy logs); use `POST`. |
| `body` | no | `{}` | Key → value dict; each value is a literal **or** a secret ref, mixed freely. |
| `body_format` | no | `json` | `json` (JSON body) or `form` (form-encoded). |
| `headers` | no | `{}` | Headers of the token call; same rules as `body`. |
| `token_json_path` | no | `access_token` | Dotted path of the token in the JSON response (e.g. `data.token`, `result.auth.jwt`). |
| `header_name` | no | `Authorization` | Header used on subsequent data calls. |
| `value_prefix` | no | `Bearer ` | Token prefix in that header. |
| `timeout_seconds` | no | `30` | Token call timeout. |

Login/password (both in Key Vault), nested token:

```json
{
  "type": "token_endpoint",
  "token_url": "https://api.example.com/login",
  "body": {
    "username": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-user"},
    "password": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-pwd"}
  },
  "token_json_path": "data.token"
}
```

Form-encoded variant with a custom output header:

```json
{
  "type": "token_endpoint",
  "token_url": "https://api.example.com/token",
  "body_format": "form",
  "body": {"api_key": {"env_var": "K"}, "grant_type": "client_credentials"},
  "token_json_path": "token",
  "header_name": "X-Auth-Token",
  "value_prefix": ""
}
```

### `oauth1` — OAuth 1.0a request signing (NetSuite TBA, legacy APIs)

Unlike every other type, OAuth 1.0a cannot be reduced to a fixed header: the signature covers the method, the URL and the query params of **each** request, so it is recomputed page after page.

```json
{
  "type": "oauth1",
  "realm": "1234567",
  "consumer_key": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "ns-consumer-key"},
  "consumer_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "ns-consumer-secret"},
  "token": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "ns-token-id"},
  "token_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "ns-token-secret"}
}
```

| Key | Required | Description |
|---|---|---|
| `consumer_key` / `consumer_secret` | **yes** | Application credentials (secret references). |
| `token` / `token_secret` | no, but together | Access token credentials. Omitting both gives two-legged OAuth 1.0a. |
| `realm` | no | Sent in the header, outside the signature. NetSuite requires the account id here (`1234567`, `1234567_SB1`). |
| `signature_method` | no (default `HMAC-SHA256`) | `HMAC-SHA256` or `HMAC-SHA1`. |

Implemented on the standard library only — no extra wheel to freeze for Fabric. A JSON request body is never signed (RFC 5849 §3.4.1.3.1); a `form` body is.

> **Redirects.** The signature covers the request URL, and `requests` replays the original `Authorization` header on a same-host redirect instead of re-signing — the API then sees an invalid signature and answers 401. Point `base_url` at the final URL. NetSuite's SuiteQL endpoint does not redirect.

### `none` / absent

No auth header. `{"type": "none"}`, `{}` or no `auth` key.

## Pagination

The type is selected by `pagination.type`. All strategies accept:

| Common key | Default | Description |
|---|---|---|
| `items_field` | auto | Response field containing the record list, as a **dotted path** (`data.orders.edges`). Left out, the probing order is: a response that is already a list is used as-is, otherwise `data`, `items`, `results`, `value`. Explicit error if none is found, or if the configured path is absent or resolves to something other than a list. A response that is already a top-level list short-circuits this key — `items_field` is not applied to it. |
| `record_field` | absent | Dotted path unwrapped from **each item** of that list. Relay connections (GraphQL) wrap every record in a `{cursor, node}` — `"record_field": "node"` keeps the record. Missing from any item ⇒ explicit error. |
| `params_in` | `query` | Where pagination and incremental params are sent: `query` (query string) or `body` (merged into the request payload). `body` requires a non-`GET` `method`. |
| `params_path` | root | With `"params_in": "body"`, dotted path inside `body` under which those params are merged (GraphQL: `variables`). The branch is created if absent. |

### `offset` — offset/limit

| Key | Default | Description |
|---|---|---|
| `limit` | `100` | Requested page size. |
| `limit_param` | `limit` | Name of the size query param (e.g. `top`). |
| `offset_param` | `offset` | Name of the offset query param (e.g. `skip`). |

Stops on: empty page, or partial page (`< limit`).

```json
{"type": "offset", "limit": 500, "limit_param": "top", "offset_param": "skip"}
```

### `page` — page number (header-provided total supported)

| Key | Default | Description |
|---|---|---|
| `page_param` | `page` | Name of the page-number query param. |
| `start_page` | `1` | First page number (use `0` for zero-based APIs). |
| `size_param` | absent | Name of the page-size query param; only sent if `page_size` is also set. |
| `page_size` | absent | Requested page size. |
| `total_pages_header` | absent | Response header carrying the total page count (e.g. `X-Total-Pages`). Read on the first response; explicit error if missing or non-numeric. |

Stops after: `total_pages` pages when `total_pages_header` is set; otherwise empty page, or partial page when `page_size` is known.

```json
{"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 100, "total_pages_header": "X-Total-Pages"}
```

### `next_link` — next-page URL in the response

| Key | Default | Description |
|---|---|---|
| `next_field` | `next` | Response field containing the full next-page URL (e.g. `@odata.nextLink` for Microsoft APIs). |

The `params`/`incremental` query params are sent on the first call only — the next-page URL already carries its own. Stops when the field is absent or `null`.

```json
{"type": "next_link", "next_field": "@odata.nextLink", "items_field": "value"}
```

### `cursor` — opaque cursor (Relay/GraphQL connections)

| Key | Default | Description |
|---|---|---|
| `cursor_param` | **required** | Name of the param carrying the cursor. Sent from the second request onwards — the first goes without it. |
| `cursor_field` | **required** | Dotted path to the next cursor in the response (`data.orders.pageInfo.endCursor`). |
| `has_more_field` | absent | Dotted path to a boolean saying whether another page exists (`data.orders.pageInfo.hasNextPage`). |
| `limit` | absent | Requested page size; only sent if set. |
| `limit_param` | `limit` | Name of the size param (GraphQL: `first`). |

Stops when: `has_more_field` is false; or, without it, on an empty page or a `null`/absent cursor.

`has_more_field` is worth setting whenever the API provides it. A heavily filtered connection can return an **empty page in the middle** of the results, which every "stop on empty page" heuristic reads as the end. Conversely, a response announcing a next page while carrying no cursor raises rather than truncating — a partial load reported as `success` is worse than a failed run.

A cursor that does not advance between two requests also raises, instead of looping forever.

```json
{
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
}
```

Full GraphQL source, auth to Delta: [examples/shopify_graphql.py](../examples/shopify_graphql.py).

### `none` / absent

A single HTTP call, no loop. The common keys still apply: `items_field` and `record_field` locate and unwrap the records of that one response, which a nested payload (a GraphQL query returning fewer than one page of results) needs just as much as a paginated one.

```json
{"type": "none", "items_field": "data.shop.metafields.edges", "record_field": "node"}
```

> **Offset ceilings.** Some APIs refuse an offset beyond a hard limit (NetSuite stops at 100 000). Past that point, split the source into bounded slices — one run per month or per id range, with the bounds in the query — rather than paging further.

## Incremental (watermark)

| Key | Required | Description |
|---|---|---|
| `enabled` | no (default `false`) | Enables incremental mode. |
| `field` | yes if enabled | Record field whose **max** becomes the new watermark. |
| `param_name` | yes if enabled and `inject` is `query_param` | Query param sent to the API with the last watermark (e.g. `updated_since`). |
| `inject` | no (default `query_param`) | Where the watermark goes: `query_param` (a query string param) or `body_template` (substituted into the `{placeholder}` markers of `body`). |
| `placeholder` | no (default `watermark`) | With `inject: "body_template"`, the variable name to substitute. |
| `initial_value` | no | Value used on the very first run, before any watermark exists. Required in practice with `body_template`, otherwise the placeholder would have nothing to resolve to. |
| `value_format` | **yes** with `body_template`, otherwise no (default `any`) | Validation applied to the watermark before it is used: `any`, `numeric`, `iso_date`, `iso_datetime`. |

Behavior: the last watermark is read from `<log_schema>.watermark` at run start (`initial_value` is used on the very first run, and no param is sent if neither exists); the new watermark is written **only if the run succeeded** and at least one record was loaded. Comparison uses Python `max()` — works for ISO 8601 timestamps and numerics; beware of date formats that don't sort lexicographically.

## Retry

| Key | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total attempts per request. |
| `backoff_multiplier` | `1` | Exponential backoff multiplier (tenacity `wait_exponential`). |
| `max_retry_after_seconds` | `300` | Ceiling on a server-provided `Retry-After` delay. |

Retried: network errors (connection, timeout), HTTP 429 and 5xx, and application errors whose code appears in [`errors.retryable_codes`](#application-errors-in-a-200). **Not retried**: other 4xx (401, 403, 404…) and any other application error — they fail immediately. Applies to data calls; the token call (`oauth2_client_credentials`/`token_endpoint`) is not retried.

`max_attempts` bounds every one of those causes alike, so an API answering "transient" forever costs at most `max_attempts` calls per page, not an unbounded loop.

When the response carries a `Retry-After` header (seconds or an HTTP date), that delay is used instead of the exponential backoff — APIs with strict governance ban clients that retry earlier than they were told to. The delay is capped at `max_retry_after_seconds`; beyond the cap the wait is truncated and the next attempt will most likely fail, which surfaces as a `failed` run in `log_runs` rather than a notebook hanging for an hour.

## Technical tables

Created automatically on first run in `<log_schema>` (default `flume`):

| Table | Columns |
|---|---|
| `watermark` | `source_name`, `last_value`, `updated_ts` — one row appended per advance (the current value is the max `updated_ts` per source). |
| `log_runs` | `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — one row per `run_source` call, success or failure. |

`error_message` holds the exception type and message of whatever failed. When the failure came from an [application error](#application-errors-in-a-200), that message is the API's own, truncated to the first 3 errors and 500 characters — a GraphQL error quotes the failing query in full, and `log_runs` is a table, not a log file. URLs in it are stripped of their query string (see [docs/security.md](security.md)).
