# Configuration reference

Exhaustive reference of every option: the JSON configuration of a source (key by key, with required/optional status and defaults), the `run_source` parameters, and the secret reference format.

Looking something up rather than reading through? [cookbook.md](cookbook.md) maps what a vendor's API does onto what to write, and carries a flat index of every key with its default. This page explains *why*; that one answers *which key*.

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
  "write": { ... },
  "batch_size": 50000,
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
| `target_table` | string | **yes** | — | Destination table, written with `schema_mode=merge`. Letters/digits/underscore only. |
| `write` | object | no | append | How the target table is written: append, full overwrite, or replacement of one window. See [Write mode](#write-mode). |
| `batch_size` | integer | no | `50000` | Rows buffered before each Delta write. Bounds the memory of a run. See [Batched writes](#batched-writes). |
| `retry` | object | no | see [Retry](#retry) | HTTP retry policy. |
| `timeout_seconds` | number | no | `60` | Timeout of each data HTTP request. |
| `method` | string | no | `GET` | HTTP method of the data calls: `GET`, `POST`, `PUT`, `PATCH`. |
| `body` | object | no | `{}` | Request body sent on every data call. Requires a non-`GET` `method`, unless `allow_body_on_get` says otherwise. |
| `allow_body_on_get` | boolean | no | `false` | Lets a `GET` carry a body. See [A body on a GET](#a-body-on-a-get). |
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
  "base_url": "https://api.example.com/services/rest/query/v1/sql",
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

`retryable_codes` is also how throttling is caught on APIs that announce it in the body rather than with a 429 (cost-based limiters commonly do exactly that). A `Retry-After` header on such a response is still honored.

## GraphQL endpoints

GraphQL needs no dedicated source type: it is a POST of a JSON body to a single URL. Five generic options do the work, and this section puts them together. The worked example below is a commerce admin API, but nothing in it is specific to one vendor — every connection built on the `edges`/`node` convention has the same shape.

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

How scalars themselves are typed: [Column types in Delta](#column-types-in-delta).

### Common failure modes

| Symptom | Cause |
|---|---|
| `Impossible de localiser les enregistrements` | `items_field` missing on a GraphQL source — `data` is an object, so the default probing finds nothing. |
| `Champ 'data.x.edges' : liste attendue, NoneType reçu` | The connection returned `null` — usually an error in the response the `errors` block would have named. Add it. |
| `placeholder '{id}' sans variable correspondante` | The query text is being scanned for placeholders: add `template_paths`. |
| `Champ 'node' absent d'un enregistrement` | `record_field` set on a connection that is not edge-wrapped (some APIs expose `nodes` directly — then drop `record_field` and point `items_field` at `nodes`). |
| Run `success` with fewer rows than expected | An `errors` entry alongside valid `data`, with no `errors` block declared. |
| `THROTTLED` even after retries | The cost limiter, not the request rate: lower `limit`, or ask for fewer fields. |

Working notebook, auth to Delta: [examples/graphql_cursor_api.py](../examples/graphql_cursor_api.py).

## `run_source` parameters

```python
run_source(config, lakehouse_tables_path=..., storage_options=..., log_schema=..., dry_run=False)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | dict | — | One entry of the configuration JSON (see above). |
| `lakehouse_tables_path` | str | `/lakehouse/default/Tables` | Tables root. In Fabric, the default is automatically resolved to the OneLake ABFSS URI of the default lakehouse — using that lakehouse's workspace, which is not always the notebook's. Can be an explicit `abfss://...` URI to target another lakehouse. |
| `storage_options` | dict \| None | `None` | Passed through to delta-rs. If absent with an `abfss://` URI, a storage bearer token is obtained via `notebookutils`. |
| `log_schema` | str | `flume` | Schema of the technical tables `watermark` and `log_runs`. |
| `dry_run` | bool | `False` | See [Dry run](#dry-run). |

Returns a `RunResult` with `source_name`, `status` (`success`/`failed`), `rows_loaded`, `error_message` (None on success), `start_ts`, `end_ts` (ISO 8601 UTC), `run_id` (UUID), and `sample` (dry run only). **Never raises.**

All four parameters worked through, with the cases each one exists for: [examples/run_options.py](../examples/run_options.py).

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

All of them side by side in one runnable notebook, with a probe loop that checks credentials without writing anything: [examples/rest_api_auth_variants.py](../examples/rest_api_auth_variants.py).

### Token expiry on long runs

The token of an `oauth2_client_credentials` or `token_endpoint` auth expires — 60 minutes is common. A run longer than that used to see its last pages answered with 401, a status that is never retried: the whole run failed. Both types are now renewed mid-run, in two ways.

**Proactively**, when the token endpoint announces a lifetime: `expires_in` for `oauth2_client_credentials` (standard OAuth2, read automatically), or the path declared in `expires_in_json_path` for `token_endpoint`. The token is renewed 60 seconds before the announced expiry, so the API never sees an expired credential.

**Reactively**, on a 401, for endpoints that announce nothing. The token is renewed and the page replayed immediately, without backoff — nothing is saturated, the credential was simply stale. This happens **once per page**: if a freshly issued token is refused again, the run fails with the 401, because that is no longer an expiry and replaying would only delay the diagnosis.

The other types carry a static credential (`bearer_token`, `api_key_header`, `basic`) or sign each request individually (`oauth1`). They are never renewed: a 401 there is a configuration error, and it fails immediately.

Renewal costs one extra token call, and only when needed. It is bounded by `retry.max_attempts` like every other retry cause.

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
| `expires_in_json_path` | no | — | Dotted path of the token lifetime, in seconds, in the same response. Enables proactive refresh — see [Token expiry on long runs](#token-expiry-on-long-runs). |
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

### `oauth1` — OAuth 1.0a request signing (ERP token auth, legacy APIs)

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
| `realm` | no | Sent in the header, outside the signature. APIs that use it expect the account id here (e.g. `1234567`). |
| `signature_method` | no (default `HMAC-SHA256`) | `HMAC-SHA256` or `HMAC-SHA1`. |

Implemented on the standard library only — no extra wheel to freeze for Fabric. A JSON request body is never signed (RFC 5849 §3.4.1.3.1); a `form` body is.

> **Redirects.** The signature covers the request URL, and `requests` replays the original `Authorization` header on a same-host redirect instead of re-signing — the API then sees an invalid signature and answers 401. Point `base_url` at the final URL.

### `none` / absent

No auth header. `{"type": "none"}`, `{}` or no `auth` key.

## Pagination

The type is selected by `pagination.type`. All strategies accept:

| Common key | Default | Description |
|---|---|---|
| `items_field` | auto | Response field containing the record list, as a **dotted path** (`data.orders.edges`). Left out, the probing order is: a response that is already a list is used as-is, otherwise `data`, `items`, `results`, `value`. Explicit error if none is found, or if the configured path is absent or resolves to something other than a list. A response that is already a top-level list short-circuits this key — `items_field` is not applied to it. |
| `record_field` | absent | Dotted path unwrapped from **each item** of that list. GraphQL connections wrap every record in a `{cursor, node}` — `"record_field": "node"` keeps the record. Missing from any item ⇒ explicit error. |
| `params_in` | `query` | Where pagination and incremental params are sent: `query` (query string), `body` (merged into the request payload as JSON values), or `body_template` (each param whose `{placeholder}` appears in `body` is substituted there, the rest go to the query string). The last two require a non-`GET` `method`. |
| `params_path` | root | With `"params_in": "body"`, dotted path inside `body` under which those params are merged (GraphQL: `variables`). The branch is created if absent. |
| `max_pages` | none | Stops the run with an error past that many pages. See [Safety bounds](#safety-bounds). |
| `max_rows` | none | Same, on the number of rows read. |

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

Runnable source: [examples/rest_api_paginated.py](../examples/rest_api_paginated.py).

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

Runnable source: [examples/rest_api_paginated.py](../examples/rest_api_paginated.py).

### `keyset` — filter by the last key seen (seek method)

Each page is filtered by the value of `key_field` taken from the **last record of the previous page**, sent back in `key_param`. Unlike `offset`, the cost of a page does not grow with its depth and nothing caps it: this is the only strategy that reaches the bottom of a multi-million-row table on APIs that bound the offset — some stop at 100 000, which is what forces month-by-month slicing.

| Key | Required | Default | Description |
|---|---|---|---|
| `key_field` | **yes** | — | Dotted path of the key inside a record (`id`, `meta.cursor_id`). |
| `key_param` | **yes** | — | Param carrying the key on the next request — or the `{placeholder}` name with `"params_in": "body_template"`. |
| `initial_value` | no | none | Key used on the very first request. Required with `body_template`, otherwise the placeholder has nothing to resolve to. |
| `value_format` | **yes** with `body_template`, otherwise no (default `any`) | `any` | Validation applied to the key before it is sent: `any`, `numeric`, `iso_date`, `iso_datetime`. |
| `limit` | no | none | Page size. |
| `limit_param` | no | `limit` | Param carrying it. |

**The source must be sorted by `key_field`, with unique values.** That is inherent to the method, not a limitation of the library: a key that goes backwards would re-read pages forever, and a duplicated key would skip everything sharing it. The library checks that the key advances from page to page and stops with an explicit error if it does not — it never loops.

Stop conditions: an empty page, a page shorter than `limit`, or `key_field` missing from the last record (an error — the next page cannot be built).

Query-string form (`since_id`, `starting_after`…):

```json
{
  "type": "keyset",
  "key_field": "id",
  "key_param": "since_id",
  "limit": 250
}
```

SQL-over-REST form, where the key belongs inside the query itself:

```json
{
  "method": "POST",
  "body": {"q": "select id, amount from transactions where id > {since_id} order by id"},
  "pagination": {
    "type": "keyset",
    "key_field": "id",
    "key_param": "since_id",
    "params_in": "body_template",
    "value_format": "numeric",
    "initial_value": 0,
    "limit": 1000,
    "limit_param": "rows"
  }
}
```

The key comes back from the API, so with `body_template` it is interpolated into a query and `value_format` is **required** — same rule, and the same reason, as the incremental watermark.

`body_template` routes **per param**, not per request: a param whose `{placeholder}` appears in `body` is substituted there, every other one goes to the query string. Both channels of a request stay available, which is what the shape above needs — a SQL-over-REST endpoint takes the key inside the SQL and `limit` as a query param:

```
POST /sql?limit=1000
Body: {"q": "select id, amount from transactions where id > 2000 order by id"}
```

Nothing is silently dropped: a param always lands in whichever channel can carry it. The placeholder named by `key_param` must exist in `body` — with `template_paths`, inside one of the declared branches — or the config is rejected, since the key would have nowhere to go.

Top-level `params` and a `query_param` watermark are both fine here; they take the query string while the key takes the body.

The watermark and the key can share one body — the watermark bounds the window, the key walks it:

```json
{"q": "select id, ts from t where ts > '{watermark}' and id > {since_id} order by id"}
```

### `next_link` — next-page URL in the response

| Key | Default | Description |
|---|---|---|
| `next_field` | `next` | Response field containing the full next-page URL (e.g. `@odata.nextLink` for Microsoft APIs). |

The `params`/`incremental` query params are sent on the first call only — the next-page URL already carries its own. Stops when the field is absent or `null`.

```json
{"type": "next_link", "next_field": "@odata.nextLink", "items_field": "value"}
```

`next_field` is a top-level response key, not a dotted path — `@odata.nextLink` works because that is the key, dot included, whereas a URL nested under `links.next` is out of reach.

Runnable sources: [examples/rest_api_paginated.py](../examples/rest_api_paginated.py), and OData end to end (Microsoft Graph, Business Central) in [examples/microsoft_graph_odata.py](../examples/microsoft_graph_odata.py).

### `cursor` — opaque cursor (GraphQL connections)

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

Full GraphQL source, auth to Delta: [examples/graphql_cursor_api.py](../examples/graphql_cursor_api.py). The same strategy against a flat REST response, cursor in the query string: [examples/rest_api_paginated.py](../examples/rest_api_paginated.py).

### `none` / absent

A single HTTP call, no loop. The common keys still apply: `items_field` and `record_field` locate and unwrap the records of that one response, which a nested payload (a GraphQL query returning fewer than one page of results) needs just as much as a paginated one.

```json
{"type": "none", "items_field": "data.shop.metafields.edges", "record_field": "node"}
```

> **Offset ceilings.** Some APIs refuse an offset beyond a hard limit (100 000 is a common one). Past that point, split the source into bounded slices — one run per month or per id range, with the bounds in the query — rather than paging further.

### Safety bounds

`max_pages` and `max_rows` bound a run whose order of magnitude is known. Reaching one is an **error**, not a clean stop: truncating silently would produce a `success` run short of part of its data, which is the failure mode this library exists to avoid. Rows already written stay written, and the message says what happened. The bound fires on the count alone — a source that happens to end exactly on it still fails, because the run stopped before observing the end.

Independently of any configuration, a page **identical to the one before it** stops the run. An API that clamps an out-of-range page number and serves the first page again has no natural stop condition — the notebook used to run until its timeout, memory climbing. The strategies that read a cursor, a next link or a keyset key have their own no-progress checks on top.

## Batched writes

Records are written to Delta in batches of `batch_size` rows (default 50 000) instead of being accumulated for a single write at the end of the run. The memory of a run no longer depends on the size of the source, and a run that breaks on page 900 leaves the first 899 pages in the table rather than losing everything. A source smaller than `batch_size` still produces exactly one commit.

`RunResult.rows_loaded` — and the `rows_loaded` column of `log_runs` — count the rows **actually written**, including on a failed run.

**This makes ingestion at-least-once.** A failed run can leave partial data behind. Rows of a given run all carry the same `_flume_run_id`, so a failed run is identifiable and removable:

```sql
DELETE FROM bronze.my_table WHERE _flume_run_id = '<the failed run_id>'
```

For a run over a window you can name — a backfill slice, a reload — [`write.mode: "replace_where"`](#write-mode) removes the need for that cleanup entirely: the rerun replaces the window instead of adding to it.

Sizing `batch_size` is a trade-off: smaller means less memory and finer-grained resumption, but more Delta commits and more small files. Leave it alone unless a run runs out of memory (lower it) or a source is small and frequent (raise it above its row count to keep one commit per run).

### Resuming with `incremental.checkpoint`

Without `checkpoint`, the watermark is committed once, at the end of a successful run: a failed run never advances it, and the next run replays the whole window — the rows already written become duplicates.

With `"checkpoint": true`, the watermark is committed after each batch. An interrupted run resumes where it stopped, and only the last incomplete batch is replayed.

This is only correct if **the source returns its rows sorted by `incremental.field`**. Otherwise a batch could carry a value lower than an already-committed watermark, and resuming would skip rows that were never written. The library checks this: a batch that goes backwards fails the run with an explicit message, before writing anything, rather than advancing a watermark that would silently lose data. Add an `ORDER BY` to the query, or leave `checkpoint` off.

Data is always committed **before** its watermark. If the watermark commit fails after the data commit, the next run replays the window and duplicates — recoverable through `_flume_run_id`. The opposite order would lose the rows for good.

## Write mode

By default every run **appends**. That is the safe behavior and it stays the default, but it makes a backfill a one-shot operation: rerunning a slice that failed halfway, or reloading a month whose source data was corrected, adds a second copy of every row. `write` gives the two ways out.

| Key | Required | Description |
|---|---|---|
| `mode` | no (default `append`) | `append`, `overwrite`, or `replace_where`. |
| `replace_where` | **yes** with `mode: "replace_where"` | SQL predicate over the **target table**. The rows it matches are deleted, and the run's rows written in their place — Delta's `replaceWhere`. |
| `partition_by` | no | Partition columns of the target table. Fixed at creation. |

```json
"write": {
  "mode": "replace_where",
  "replace_where": "trandate_iso >= '2026-01-01' AND trandate_iso < '2026-02-01'"
}
```

Rerunning that configuration leaves exactly one copy of January, however many times it runs. `mode: "overwrite"` does the same for the whole table — a reference table reloaded in full, where the source is the truth and history is not kept.

### The predicate must match what the source returns

delta-rs validates it: if a written row falls outside `replace_where`, the commit is **refused** and the run fails without touching the table. This is a feature — a predicate on January combined with a query returning February would otherwise delete January and write February into it. Derive both from the same bounds, as [examples/sql_over_rest_api.py](../examples/sql_over_rest_api.py) does for its monthly slices.

The predicate runs against the *target* table, so it can only use columns already written there, plus the `_flume_*` lineage columns. Remember that this library writes dates as **strings** — it does no temporal typing — so `trandate >= '2026-01-01'` is a string comparison, and the column has to be projected in an ISO-ordered format for it to mean anything. A window that does not exist in the table yet is not an error: nothing is deleted, the rows are simply written. Backfilling a new month and replaying an old one take the same path.

`replace_where` is **not templated**: a `{month}` marker would stay literal, match no row, and the run would replace nothing. The configuration is a Python dict — build the string in the notebook, one window per run. A placeholder found in it fails validation rather than running.

### A source that returns nothing replaces nothing

If a run loads zero rows, no replacement happens and the target keeps its previous contents. This is deliberate: an API that is down, a filter that is too narrow and a token missing a scope all answer "0 rows", and emptying a window on that signal would destroy data without anything having failed. Because the natural expectation is the opposite, the run says so — `RunResult.warnings` carries an explicit message, and a run reported as `success` with a warning is worth reading.

### What a failed run leaves behind

The replacement is committed with the first batch, so a run that breaks at batch 3 of 10 leaves the window holding those three batches — and the contents it replaced are gone. Delta keeps the previous version (`RESTORE`, or a time-travel read, recovers it until it is vacuumed), but the live table is short of the rest until the run is replayed. Replaying it is exactly what this mode makes safe: rerun the same configuration and the window is rebuilt whole.

A source under `batch_size` writes one batch and never sees this: the replacement and the data land in the same commit, all or nothing.

### Not compatible with `incremental.checkpoint`

The replacement happens **on the first batch only**; later batches of the same run append. Applying the predicate to every batch would make the batches of one run erase each other, leaving a 300 000-row run with only its last 50 000.

That is also why `checkpoint` is refused with a replacing mode: resuming mid-run would restart from the watermark and replace the window a second time, erasing what the interrupted run had already written into it. A backfill is replayed from the start of its window, not from its middle — the combination fails validation.

### Every mode side by side

[examples/write_modes.py](../examples/write_modes.py) runs through all of them against one API: append, full overwrite, a date window, an id range, a rolling window, partitioning, an empty source, a predicate that disagrees with its query, and the `checkpoint` combination the library refuses.

### Partitioning

`partition_by` sets the partition columns **when the table is created**. Delta fixes them at that point: passing them for an existing unpartitioned table fails with an explicit message, since changing them means rewriting the table whole, which this library does not do. Partition on a column a `replace_where` predicate filters on and the replacement only rewrites the matching partitions.

The library performs no `OPTIMIZE` and no `VACUUM`. A table written batch after batch accumulates small files, and the replaced files stay on disk until vacuumed — schedule both out of band if a table grows enough to need them.

### A body on a GET

HTTP gives a body on a `GET` no defined semantics, and most APIs drop it without a word, so a `body` alongside `"method": "GET"` is refused by default — the mistake is common and silent. Some APIs do read one, though, and put their filter nowhere else: the incidents endpoint of SolarWinds Service Desk is one. `"allow_body_on_get": true` declares that exception.

Once declared, a `GET` follows exactly the same paths as a `POST`: `pagination.params_in` decides where the parameters go, not the method. Pagination therefore stays in the query string while the filter lives in the body, with nothing special to write:

```json
{
  "method": "GET",
  "allow_body_on_get": true,
  "body": {"updated_from": "{watermark}"},
  "pagination": {"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 100},
  "incremental": {
    "enabled": true, "field": "updated_at", "inject": "body_template",
    "placeholder": "watermark", "value_format": "iso_datetime", "normalize": "utc_iso"
  }
}
```

The flag only lifts a refusal; it changes nothing for a config that does not set a body.

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
| `normalize` | no (default `none`) | Reshapes the watermark **before it is sent**: `none` leaves it exactly as read, `utc_iso` converts it to UTC in `2026-08-25T12:57:44.000Z` form. For an API that dates its records with a local offset but only filters in UTC. |
| `checkpoint` | no (default `false`) | Commits the watermark after **each batch** instead of once at the end, making an interrupted run resumable. Requires the source to return its rows sorted by `field`. See [Batched writes](#batched-writes). |

Behavior: the last watermark is read from `<log_schema>.watermark` at run start (`initial_value` is used on the very first run, and no param is sent if neither exists); the new watermark is written **only if the run succeeded** and at least one record was loaded — unless [`checkpoint`](#resuming-with-incrementalcheckpoint) is on, in which case it advances batch by batch.

The max of each batch is computed **before** that batch is written, so a `field` whose values cannot be compared (mixed types) fails the run without leaving rows behind an unadvanced watermark. Comparison uses Python `max()` — works for ISO 8601 timestamps and numerics; beware of date formats that don't sort lexicographically.

`normalize` exists because the watermark makes a round trip: it is **read** from a record and **sent back** as a filter, and an API is free to disagree with itself about the format between the two. One that returns `2026-08-25T14:57:44.000+02:00` but only accepts `...Z` in its filter could not be loaded incrementally at all, since the value is otherwise reinjected verbatim. `"normalize": "utc_iso"` closes that gap, and applies to `initial_value` as well, so the first run and every run after it send the same shape. A value carrying no offset is read as UTC — the convention of `pandas.to_datetime(..., utc=True)`, and the only stable one: binding it to the machine's timezone would make a run's bound depend on where the notebook happens to execute. A value that is not an ISO 8601 instant fails the run before the first HTTP call.

Normalization applies only to what is **sent**. The watermark is still stored as the API wrote it, and the max of a batch is still taken over the raw record values — so on the night of a DST change, a lexicographic max over mixed offsets can retain a value that is chronologically earlier than the true maximum. Never later: the max is taken over rows that were actually loaded, so the bound stays at or below the most recent instant seen. The consequence is bounded to re-reading a short window, deduplicated downstream on the lineage columns; no row can be skipped.

`inject: "query_param"` sends the watermark as the **entire value** of one param: it produces `?updated_since=2026-08-01T00:00:00Z`, and cannot build an expression around it. An API whose filter is a composed string — OData's `$filter=lastModifiedDateTime ge 2026-08-01T00:00:00Z`, a SQL `WHERE` clause — needs the value *inside* a string, which is `inject: "body_template"` and therefore a non-`GET` `method`. For a GET endpoint that only accepts a composed filter, the bound has to be computed in the notebook and written into `params` there; [examples/microsoft_graph_odata.py](../examples/microsoft_graph_odata.py) shows that form, and what it costs (re-read overlap, deduplicated downstream on the lineage columns).

Runnable incremental sources: query param in [examples/rest_api_paginated.py](../examples/rest_api_paginated.py), `body_template` into a SQL `WHERE` in [examples/sql_over_rest_api.py](../examples/sql_over_rest_api.py), and into GraphQL `variables` in [examples/graphql_cursor_api.py](../examples/graphql_cursor_api.py).

## Retry

| Key | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total attempts per request. |
| `backoff_multiplier` | `1` | Exponential backoff multiplier (tenacity `wait_exponential`). |
| `max_retry_after_seconds` | `300` | Ceiling on a server-provided `Retry-After` delay. |

Retried: network errors (connection, timeout), HTTP 429 and 5xx, and application errors whose code appears in [`errors.retryable_codes`](#application-errors-in-a-200). **Not retried**: other 4xx (401, 403, 404…) and any other application error — they fail immediately. Applies to data calls; the token call (`oauth2_client_credentials`/`token_endpoint`) is not retried.

`max_attempts` bounds every one of those causes alike, so an API answering "transient" forever costs at most `max_attempts` calls per page, not an unbounded loop.

When the response carries a `Retry-After` header (seconds or an HTTP date), that delay is used instead of the exponential backoff — APIs with strict governance ban clients that retry earlier than they were told to. The delay is capped at `max_retry_after_seconds`; beyond the cap the wait is truncated and the next attempt will most likely fail, which surfaces as a `failed` run in `log_runs` rather than a notebook hanging for an hour.

## Column types in Delta

Each column is typed from **all** the values of the batch being written, not from the first non-null one:

| Values seen in the column | Delta type |
|---|---|
| integers only | `bigint` |
| floats, or integers **and** floats | `double` |
| booleans only | `boolean` |
| anything else — strings, mixed scalars, objects, lists | `string` |

Objects and lists are serialized to a JSON string. Dates and timestamps arrive as strings and stay strings: the library does not parse them, so downstream casting is the consumer's job.

A column whose values fit none of those types — an integer beyond `bigint`, say — is still written, as text, and the degradation is reported in `RunResult.warnings`. A run can be `success` and carry warnings; that is the point. It used to be silent, which is how a column of amounts became a column of text without anyone noticing.

Within a run, the types chosen by the first batch are applied to the following ones, so a source whose later rows look different cannot produce two incompatible schemas for the same table. Across runs, Delta's `schema_mode=merge` handles new columns, but not a column that changes type from one run to the next — that is a `SchemaMismatchError` at commit time.

## Technical tables

Created automatically on first run in `<log_schema>` (default `flume`):

| Table | Columns |
|---|---|
| `watermark` | `source_name`, `last_value`, `updated_ts` — one row appended per advance (the current value is the max `updated_ts` per source). |
| `log_runs` | `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — one row per `run_source` call, success or failure. |

`error_message` holds the exception type and message of whatever failed. When the failure came from an [application error](#application-errors-in-a-200), that message is the API's own, truncated to the first 3 errors and 500 characters — a GraphQL error quotes the failing query in full, and `log_runs` is a table, not a log file. URLs in it are stripped of their query string (see [docs/security.md](security.md)).
