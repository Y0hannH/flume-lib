# Configuration reference

Exhaustive reference of every option: the JSON configuration of a source (key by key, with required/optional status and defaults), the `run_source` parameters, and the secret reference format.

## Source overview

```json
{
  "name": "my_source",
  "base_url": "https://api.example.com/v1/items",
  "params": {"status": "active"},
  "auth": { ... },
  "pagination": { ... },
  "incremental": { ... },
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

## `run_source` parameters

```python
run_source(config, lakehouse_tables_path=..., storage_options=..., log_schema=...)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | dict | — | One entry of the configuration JSON (see above). |
| `lakehouse_tables_path` | str | `/lakehouse/default/Tables` | Tables root. In Fabric, the default is automatically resolved to the OneLake ABFSS URI of the notebook's default lakehouse. Can be an explicit `abfss://...` URI to target another lakehouse. |
| `storage_options` | dict \| None | `None` | Passed through to delta-rs. If absent with an `abfss://` URI, a storage bearer token is obtained via `notebookutils`. |
| `log_schema` | str | `flume` | Schema of the technical tables `watermark` and `log_runs`. |

Returns a `RunResult` with `source_name`, `status` (`success`/`failed`), `rows_loaded`, `error_message` (None on success), `start_ts`, `end_ts` (ISO 8601 UTC), `run_id` (UUID). **Never raises.**

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

### `none` / absent

No auth header. `{"type": "none"}`, `{}` or no `auth` key.

## Pagination

The type is selected by `pagination.type`. All strategies accept:

| Common key | Default | Description |
|---|---|---|
| `items_field` | auto | Response field containing the record list. By default: a list response is used as-is, otherwise `data`, `items`, `results`, `value` are probed. Explicit error if none found. |

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

### `cursor` — not implemented (stub)

Raises `NotImplementedError` (the run ends `failed` with that message).

### `none` / absent

A single HTTP call, no loop.

## Incremental (watermark)

| Key | Required | Description |
|---|---|---|
| `enabled` | no (default `false`) | Enables incremental mode. |
| `field` | yes if enabled | Record field whose **max** becomes the new watermark. |
| `param_name` | yes if enabled | Query param sent to the API with the last watermark (e.g. `updated_since`). |

Behavior: the last watermark is read from `<log_schema>.watermark` at run start (no param sent on the very first run); the new watermark is written **only if the run succeeded** and at least one record was loaded. Comparison uses Python `max()` — works for ISO 8601 timestamps and numerics; beware of date formats that don't sort lexicographically.

## Retry

| Key | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total attempts per request. |
| `backoff_multiplier` | `1` | Exponential backoff multiplier (tenacity `wait_exponential`). |

Retried: network errors (connection, timeout), HTTP 429 and 5xx. **Not retried**: other 4xx (401, 403, 404…) fail immediately. Applies to data calls; the token call (`oauth2_client_credentials`/`token_endpoint`) is not retried.

## Technical tables

Created automatically on first run in `<log_schema>` (default `flume`):

| Table | Columns |
|---|---|
| `watermark` | `source_name`, `last_value`, `updated_ts` — one row appended per advance (the current value is the max `updated_ts` per source). |
| `log_runs` | `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — one row per `run_source` call, success or failure. |
