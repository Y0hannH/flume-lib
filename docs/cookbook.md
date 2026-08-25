# Cookbook

Two lookup tables and nothing else: **what the vendor's API does → what to write**, then **every key, its default, in one flat index**.

This page assumes you already know what the library does. It is the one to open when you are writing the fourth source of the month and need the exact spelling of a key, or when a vendor hands you a doc page and you want the config in two minutes. It answers *which key*; [configuration.md](configuration.md) answers *why*, and [`examples/`](../examples/) shows a complete notebook.

---

## 1. From the API to the config

### Auth — match the vendor's wording

| The vendor says | Write |
|---|---|
| "Send `Authorization: Bearer <token>`" | `{"type": "bearer_token", "token": <ref>}` |
| "Send your token in the `X-Whatever` header" | `{"type": "api_key_header", "header_name": "X-Whatever", "key": <ref>}` |
| "Send `Authorization: token <t>`" or any other prefix | `bearer_token` + `"value_prefix": "token "` |
| "Subscription key" (Azure APIM) | `{"type": "api_key_header", "header_name": "Ocp-Apim-Subscription-Key", "key": <ref>}` |
| "HTTP Basic, user + password" | `{"type": "basic", "username": …, "password": <ref>}` |
| "POST your credentials to `/login`, use the token you get back" | `{"type": "token_endpoint", "token_url": …, "body": {…}, "token_json_path": …}` |
| "OAuth2 client credentials" / "client id + secret" | `{"type": "oauth2_client_credentials", "token_url": …, "client_id": …, "client_secret": <ref>}` |
| Any Microsoft API (Graph, Fabric, Business Central) | same, with `"tenant_id"` instead of `token_url` |
| "OAuth 1.0a", "TBA", "token-based authentication" (NetSuite and other ERPs) | `{"type": "oauth1", "consumer_key": …, "consumer_secret": …, "token": …, "token_secret": …}` |
| "The API key goes in the query string" | no auth type — [ask for the header form](../examples/rest_api_auth_variants.py); a key in a URL lands in every proxy log |
| Nothing — the endpoint is public | `{"type": "none"}`, or no `auth` block |

`<ref>` is **never a literal**. It is `{"keyvault_url": …, "secret_name": …}` in Fabric, or `{"env_var": "…"}` locally. The config file itself carries no secret.

### Pagination — match the response

| What you observe | Write |
|---|---|
| `?limit=100&offset=200`, or `$top`/`$skip`, or `count`/`start` | `{"type": "offset", "limit_param": …, "offset_param": …}` |
| `?page=3`, and you want to stop on an empty page | `{"type": "page", "page_param": "page"}` |
| `?page=3`, and the total is in a **response header** | same + `"total_pages_header": "X-Total-Pages"` — stops exactly, no probing call |
| The response carries the **full URL** of the next page | `{"type": "next_link", "next_field": "next"}` |
| The response carries an **opaque token** (`next_cursor`, `endCursor`) | `{"type": "cursor", "cursor_param": …, "cursor_field": …}` |
| …and also a `has_more` boolean | same + `"has_more_field"` — **set it**, see below |
| "Pass the last id you saw", or an offset ceiling forces you off `offset` | `{"type": "keyset", "key_field": "id", "key_param": "since_id"}` |
| One response, no loop (reference data) | `{"type": "none"}`, or no `pagination` block |

Two traps worth naming, because both fail silently rather than loudly:

- **The API filters server-side *after* cutting the page.** It then returns short pages, and sometimes empty ones, in the middle of the results. Every stop-on-empty heuristic reads that as the end and the run succeeds with a fraction of the data. You need an explicit end-of-stream signal: `has_more_field` with `cursor`, or `total_pages_header` with `page`.
- **`items_field` is worth naming even when the probe finds it.** Left out, the library probes `data`, `items`, `results`, `value`. Named, a vendor who renames the envelope in v2 fails the run instead of loading nothing.

### Reload strategy — what a rerun should do

| What you want | Write |
|---|---|
| Full reload, the table mirrors the source | `"write": {"mode": "overwrite"}`, no `incremental` |
| Append-only stream (events, logs) | nothing — `append` is the default |
| Only what changed since the last run | `"incremental": {"enabled": true, "field": …, "param_name": …}` |
| …and the API dates its records in a local offset but filters in UTC | same + `"normalize": "utc_iso"` |
| A rerunnable backfill, one window at a time | `"write": {"mode": "replace_where", "replace_where": "<SQL>"}` |

`overwrite` and `replace_where` share one deliberate behaviour: **a run that loads zero rows replaces nothing.** The target keeps its previous contents and `RunResult.warnings` says so, on a run still marked `success`. An API that is down and a genuinely empty window both answer "0 rows", and emptying the table on that signal would destroy data without anything having failed. Read `warnings` in your run loop — it does not reach `log_runs`.

---

## 2. The configs that cover most of it

Two shapes account for most vendor APIs. They differ only in the `auth` block — nothing else in a source config depends on how the credential is obtained.

### 2.1 Static token in a header

Token in Key Vault, page-number pagination with the total in a header, full reload every run:

```python
from flume_lib import run_source

API = "https://api.vendor.com/v1"
KEYVAULT = "https://mykv.vault.azure.net"

BASE = {
    "auth": {
        "type": "bearer_token",
        "token": {"keyvault_url": KEYVAULT, "secret_name": "vendor-api-token"},
    },
    "pagination": {
        "type": "page",
        "page_param": "page",
        "total_pages_header": "X-Total-Pages",
        "items_field": "data",
    },
    "target_schema": "bronze",
    "write": {"mode": "overwrite"},
}

SOURCES = [
    {**BASE, "name": "vendor_clients", "base_url": f"{API}/clients", "target_table": "clients"},
    {**BASE, "name": "vendor_orders",  "base_url": f"{API}/orders",  "target_table": "orders"},
]

for config in SOURCES:
    result = run_source(config)
    print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
```

`{**BASE, …}` is a **shallow** copy: the nested `pagination` dict is shared by every source built from it. To vary one, rebuild the sub-dict — `{**BASE["pagination"], "items_field": "results"}` — never mutate it in place.

### 2.2 Token obtained by a login call, with a client id and secret

The vendor gives you a client id and a client secret and expects a call to get a token, which then goes on every data request. Both credentials sit in Key Vault; the client id is rarely secret, but there is no reason to treat it differently.

Swap the `auth` block of 2.1 for this one and change nothing else:

```python
BASE = {
    "auth": {
        "type": "oauth2_client_credentials",
        "token_url": "https://idp.vendor.com/oauth2/token",
        "client_id": {"keyvault_url": KEYVAULT, "secret_name": "vendor-client-id"},
        "client_secret": {"keyvault_url": KEYVAULT, "secret_name": "vendor-client-secret"},
        "scope": "read:clients read:orders",   # omit when the vendor asks for none
        "timeout_seconds": 30,                 # of the token call only
    },
    "pagination": {
        "type": "page",
        "page_param": "page",
        "total_pages_header": "X-Total-Pages",
        "items_field": "data",
    },
    "target_schema": "bronze",
    "write": {"mode": "overwrite"},
}
```

That is the **standard** flow: a form-encoded `POST` carrying `grant_type=client_credentials`, the token read from `access_token`, sent as `Authorization: Bearer <token>`. Use it whenever the vendor's doc says "OAuth2", "client credentials" or "client id + secret". For any Microsoft API, drop `token_url` and pass `"tenant_id"` instead — the Entra ID token URL is built for you.

**When the login call is the vendor's own invention** — a bespoke `POST /auth/login` answering `{"data": {"jwt": …}}` — it is the same idea under `token_endpoint`, where every part of the request and the response path is spelled out:

```python
    "auth": {
        "type": "token_endpoint",
        "token_url": f"{API}/auth/login",
        "method": "POST",
        "body": {
            "clientId": {"keyvault_url": KEYVAULT, "secret_name": "vendor-client-id"},
            "clientSecret": {"keyvault_url": KEYVAULT, "secret_name": "vendor-client-secret"},
        },
        "body_format": "json",              # "form" for application/x-www-form-urlencoded
        "token_json_path": "data.jwt",      # default: "access_token"
        "expires_in_json_path": "data.expiresIn",
        "header_name": "Authorization",     # how it is then sent on the data calls
        "value_prefix": "Bearer ",
        "timeout_seconds": 30,
    },
```

Three things worth knowing about both, because they are what makes a long run survive:

- **The token is renewed mid-run.** An API handing out 30-minute tokens no longer breaks a two-hour backfill. With a lifetime known — `expires_in` is standard in OAuth2, and `expires_in_json_path` supplies it for `token_endpoint` — renewal happens 60 seconds *before* expiry, wasting no call. Without one, renewal is reactive: on a 401 the library logs in again and replays the page, once per page. Both work; the first is free.
- **A `token_endpoint` declared as GET with a secret reference in `body` is refused at validation.** Those values would leave as query params, into every server log and proxy in between. It is a POST or it is nothing.
- **A wrong client secret fails as `AuthError`, before any data call.** A dry run is therefore a complete credential test, and the cheapest way to check a rotation.

### 2.3 Before the first real run

Whichever of the two you took, especially with `overwrite`, do a pass that writes nothing:

```python
result = run_source(config, dry_run=True)   # validates, calls the API, writes nothing
print(result.status, result.rows_loaded, result.error_message)
print(result.sample)                        # the first 3 raw records
```

It exercises the credential, the pagination and the response shape, and touches neither the target table, nor the watermark, nor `log_runs`. It does walk the **whole** pagination — bound it with `max_pages` while testing a large source.

---

## 3. When a run fails

`run_source` never raises. A failure is a `RunResult` with `status == "failed"`, and the prefix of `error_message` names the layer that broke:

| Prefix | Meaning | Look at |
|---|---|---|
| `ConfigError:` | never reached the network | the key it names — an unknown key comes with a "did you mean" |
| `AuthError:` | the credential could not be *obtained* | Key Vault secret name, env var, the login call |
| `HTTPError: HTTP 401` | obtained, and refused | the token itself, `header_name`, `value_prefix` |
| `HTTPError: HTTP 403` | accepted, but the scope or role is short | vendor-side permissions |
| `RetryableHTTPError: HTTP 429` | rate limited past `retry.max_attempts` | raise `max_attempts`; a `Retry-After` is already honored |
| `RetryableHTTPError: HTTP 5xx` | the vendor stayed down for every attempt | the vendor |
| `PaginationError:` | the loop lost its footing | `items_field`, `cursor_field`, a missing `total_pages_header` |
| `IncrementalError:` | the watermark did not advance, or does not sort | `incremental.field`, `value_format` |
| `APIError:` | the API returned an error inside a 200 | your `errors` block |

Query strings are stripped from these messages before they reach `log_runs`: the table is readable by the whole lakehouse, and a config that put a token in a URL would otherwise republish it there.

A `success` is worth reading too — `RunResult.warnings` carries the empty-source case above, and column-type degradations.

---

## 4. Key index

Every key the validator accepts, its block, whether it is required, and its default. **Generated from the source** by `scripts/gen_key_index.py` and checked in CI, so it cannot drift from what the library actually does.

`Required` reads: `yes` — always; `one of` — at least one key of its group; `conditional` — required by another key of the same block (`cursor_field` once the type is `cursor`, `field` once `incremental.enabled` is true); `—` — optional.

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
| `checkpoint` | — | — | commits the watermark batch by batch; incompatible with any `write.mode` but `append` |

#### `write`

| Key | Required | Default | Note |
|---|---|---|---|
| `mode` | — | `"append"` | one of `append` / `overwrite` / `replace_where` |
| `replace_where` | conditional | — | SQL predicate over the target table; not templated |
| `partition_by` | — | — | Delta partition columns |

#### `retry`

| Key | Required | Default | Note |
|---|---|---|---|
| `max_attempts` | — | `3` |  |
| `backoff_multiplier` | — | `1` |  |
| `max_retry_after_seconds` | — | `300` | ceiling on a `Retry-After` the server asks for |

#### `errors`

| Key | Required | Default | Note |
|---|---|---|---|
| `path` | — | `"errors"` | where the error envelope sits in a 200 response |
| `code_field` | — | `"extensions.code"` |  |
| `message_field` | — | `"message"` |  |
| `retryable_codes` | — | — | codes retried like a 5xx instead of failing the run |

<!-- END GENERATED KEY INDEX -->
