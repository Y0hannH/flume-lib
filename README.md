# flume-lib

Generic API ingestion accelerator for **Microsoft Fabric Python notebooks** (non-Spark). Pure Python: Delta writes via [delta-rs](https://github.com/delta-io/delta-rs) (`deltalake`), no PySpark dependency.

Point it at a JSON list of API sources; it handles authentication (Key Vault-backed secrets, OAuth2 service principals, OAuth 1.0a signing, custom login endpoints), pagination (offset, page, next-link, cursor), incremental loading with watermarks, bounded retries, and writes everything — data and run logs — as Delta tables in a schema-enabled lakehouse. REST and GraphQL endpoints are both covered by the same generic options — no per-API code.

## Requirements

- A Microsoft Fabric **Python notebook** (non-Spark) — kernels 3.10 / 3.11 / 3.12
- A **schema-enabled lakehouse** attached as default (schemas cannot be enabled on an existing lakehouse — it must be created with the option on)
- For Key Vault secret references: the notebook identity needs **Get** permission on the vault's secrets

## Installation

### Recommended: offline wheels from the lakehouse

No code is fetched from GitHub or PyPI at runtime — the notebook installs exactly the files you uploaded.

1. On your workstation, generate the wheel batch (library + all dependencies, resolved for the Fabric kernel — Linux x86_64, Python 3.12 by default):

   ```bash
   git clone https://github.com/Y0hannH/flume-lib.git
   cd flume-lib
   pip install -e ".[dev]"
   python scripts/build_fabric_wheels.py          # add --python-version 3.11 for another kernel
   ```

2. Upload the `.whl` files from `fabric-wheels/` to a folder in the lakehouse — `Files/libs/` by convention.

3. In the notebook:

   ```python
   %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.9.0
   ```

`--no-index` guarantees pip resolves only from that folder — nothing is fetched from PyPI or GitHub. The folder layout is entirely up to you: any path works as long as the same path is passed to `--find-links`.

Pin the version with `==` so the installed version is explicit and visible in the notebook. Without a pin, pip picks the highest version present in the folder, so dropping a newer batch alongside the old one silently upgrades every notebook pointing at it. Upgrading is then a deliberate act: upload the new batch, and bump the pin in the notebooks you intend to move. Keep the previous batch archived (elsewhere in `Files/`, or in the release zip) so a rollback is just re-uploading it.

### Alternative: direct install from GitHub (dev environments)

Pin the **full commit SHA** of a release, never a tag — a tag can be re-pointed by an attacker with write access, a SHA cannot:

```
%pip install git+https://github.com/Y0hannH/flume-lib.git@679a15adf13b7169a520194b76ee8cfd2cfe48aa
```

| Version | SHA |
|---|---|
| v0.9.0 | `809a82df70d6790574853e1329b8f3bf5b1b663c` |
| v0.8.1 | `8e26f5e250a690f77b7ca7e196c855588efd76eb` |
| v0.8.0 | `526840b400baaf5de3889af5b0d783833fa9066a` |
| v0.7.0 | `d69aad0b1539558266fdbf02f20b327bbf4f3c71` |
| v0.6.0 | `c71716012f6c846d900ddae10892c683ec5b58b3` |
| v0.5.0 | `679a15adf13b7169a520194b76ee8cfd2cfe48aa` |
| v0.4.0 | `75fb767dad8c453930ad5249c2b4540c5f263ce0` |

Find a version's SHA with `git rev-parse vX.Y.Z` or on the [tags page](https://github.com/Y0hannH/flume-lib/tags).

## Usage

```python
from flume_lib import run_source
import json

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

for source_config in sources:
    result = run_source(source_config)
    print(f"{source_config['name']}: {result.status} ({result.rows_loaded} rows)")
```

`run_source(config)` **never raises**: every error is caught and reported in the returned `RunResult` (`status`, `rows_loaded`, `error_message`, `start_ts`, `end_ts`, `run_id`), so the calling loop always continues with the next source.

Before a first run — or when onboarding a new client API — use dry-run mode: it validates the config and really calls the API (credentials and pagination included), but writes nothing.

```python
result = run_source(config, dry_run=True)
print(result.status, result.rows_loaded, result.error_message)
print(result.sample)   # first raw records
```

Every written row carries `_flume_run_id` and `_flume_ingested_at`, so any row can be traced back to the run that produced it — and to its `log_runs` entry.

The library targets **schema-enabled lakehouses only**: each source declares its destination schema (`target_schema`, required — e.g. `bronze`), and the technical tables (`watermark`, `log_runs`) live in a dedicated schema, `flume` by default (`run_source(..., log_schema="...")` to change it).

## Source configuration

> 📖 **Full reference**: [docs/configuration.md](docs/configuration.md) — every key of every auth and pagination type, with required/optional status, defaults, stop conditions and examples. Below is an overview.

Configurations are **strictly validated**: an unknown key is an error with a "did you mean…" suggestion, never a silent no-op. `validate_config(config)` is exported so a whole source list can be checked before running anything.

```json
{
  "name": "example_source",
  "base_url": "https://api.example.com/v1/items",
  "auth": {
    "type": "bearer_token",
    "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "src-token"}
  },
  "pagination": {
    "type": "offset",
    "limit": 100
  },
  "incremental": {
    "enabled": true,
    "field": "updated_at",
    "param_name": "updated_since"
  },
  "target_schema": "bronze",
  "target_table": "example_source",
  "batch_size": 50000,
  "retry": {
    "max_attempts": 3,
    "backoff_multiplier": 1
  }
}
```

### Auth

Credentials are **never stored in the configuration** — every credential is a **secret reference** resolved at runtime:

- `{"env_var": "VAR_NAME"}` — environment variable
- `{"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "my-secret"}` — Azure Key Vault, via `notebookutils` in Fabric (preinstalled) or `flume-lib[azure]` outside Fabric
- a literal string — **only** for non-sensitive values (public username, `grant_type`, scope…)

| Type | Purpose |
|---|---|
| `bearer_token` | Static token; custom header name/prefix supported |
| `api_key_header` | API key in a header (e.g. `Ocp-Apim-Subscription-Key`) |
| `basic` | HTTP Basic |
| `oauth2_client_credentials` | Standard OAuth2 flow — Entra ID service principals (Microsoft APIs) via `tenant_id`, or any IdP via `token_url` |
| `token_endpoint` | Arbitrary login call (JSON/form body, secret refs in any field, token extracted by JSON path) |
| `oauth1` | OAuth 1.0a request signing (RFC 5849, HMAC-SHA256/SHA1, `realm`) — NetSuite TBA and legacy OAuth 1.0a APIs |

`oauth2_client_credentials` and `token_endpoint` tokens are **renewed mid-run**: proactively when the endpoint announces an `expires_in`, and reactively on a 401 otherwise (once per page — a freshly issued token refused again fails the run). A run longer than the token's lifetime no longer dies on its last pages. The other types carry a static credential and are never renewed; `oauth1` signs every request individually, so nothing expires mid-run.

Beyond authentication, `headers` adds fixed headers to every data call (literal strings only — a secret belongs in `auth`).

### Pagination

| Type | Purpose |
|---|---|
| `offset` | Offset/limit query params; stops on empty or partial page |
| `page` | Page number; total page count read from a response header (`total_pages_header`) or stop on empty/partial page |
| `next_link` | Follows a next-page URL from the response body (e.g. `@odata.nextLink`) |
| `cursor` | Opaque cursor read from the response; `has_more_field` covers Relay/GraphQL connections |

Records are located with `items_field` (a dotted path — `data.orders.edges`) and, where each item is a wrapper, unwrapped with `record_field` (`node`).

Data endpoints can be `GET` (default) or `POST`/`PUT`/`PATCH` via `method` + `body`; `pagination.params_in` decides whether pagination params go to the query string or into the request payload, and `params_path` where inside that payload (GraphQL: `variables`).

Some APIs cap how far an offset may go (NetSuite refuses past 100 000). Past that, split the source into bounded slices — one run per month or per id range — rather than paging further; see [examples/netsuite_suiteql.py](examples/netsuite_suiteql.py).

### Incremental (watermark)

When `incremental.enabled`, the last watermark is read from `<log_schema>.watermark` and sent as a query param (`param_name`). After a **successful** run only, the max of `incremental.field` over the loaded records becomes the new watermark.

With `"inject": "body_template"` the watermark is substituted into the `{placeholder}` markers of `body` instead — for APIs whose filter lives inside the request payload, such as an SQL-over-REST endpoint. `initial_value` provides the floor used on the very first run, and `value_format` validates the watermark before it is used.

`"checkpoint": true` commits the watermark after each batch instead of at the end of the run, so an interrupted run resumes where it stopped — see [Batched writes](#batched-writes).

### Batched writes

Rows are written by batches of `batch_size` (default 50 000) instead of being accumulated for one write at the end. A run's memory no longer depends on the size of the source, and a run that breaks half-way keeps what it already wrote — `RunResult.rows_loaded` counts the rows actually committed. A source smaller than one batch still produces a single commit.

The trade-off is at-least-once ingestion: a failed run can leave partial data. Every row carries `_flume_run_id`, so a failed run's rows are identifiable and removable. Pair with `incremental.checkpoint` to make the next run resume instead of replay.

### Retry

Exponential backoff via `tenacity` on network errors and HTTP 429/5xx, driven by `retry.max_attempts` (default 3) and `retry.backoff_multiplier` (default 1). Other 4xx fail immediately.

A `Retry-After` header on the response overrides the backoff — retrying earlier than a server asked is what gets a client banned on APIs with strict governance. Capped by `retry.max_retry_after_seconds` (default 300).

### Application errors in a 200

GraphQL endpoints — and a few REST ones — report failures inside a successful HTTP response. The optional `errors` block declares that envelope (`path`, `code_field`, `message_field`), so such a response fails the run with the API's own message instead of passing for data; `retryable_codes` replays the ones the API calls transient, which is how cost-based throttling arrives when it is not a 429. Without it, a partial failure — valid rows *and* an error — is reported `success`, short of part of what was asked for.

### GraphQL endpoints

There is no GraphQL source type — a GraphQL endpoint is a POST of `{query, variables}` to a single URL, and five generic options cover it:

| GraphQL concept | Option |
|---|---|
| Records under `data.<connection>.edges`, each wrapped in `{cursor, node}` | `items_field` (dotted) + `record_field` |
| `first`/`after` belong inside `variables` | `params_in: "body"` + `params_path: "variables"` |
| `pageInfo.hasNextPage` / `endCursor` | `pagination.type: "cursor"` + `has_more_field` / `cursor_field` |
| Braces in the query vs. `{placeholder}` markers | `template_paths` |
| Errors and throttling returned with HTTP 200 | `errors` + `retryable_codes` |

Full walkthrough with the request bodies it produces, how to write the query, how nested selections land in Delta, and a table of common failure modes: [docs/configuration.md#graphql-endpoints](docs/configuration.md#graphql-endpoints). Working notebook: [examples/shopify_graphql.py](examples/shopify_graphql.py).

## Examples

Every file in [`examples/`](examples/) is a complete Fabric notebook, runnable as-is once the URLs and secret names are yours. Start with the first one — the other five assume it.

| Example | What it covers |
|---|---|
| [rest_api_paginated.py](examples/rest_api_paginated.py) | **The ordinary REST API.** One fictional endpoint read with each of the five pagination shapes (`offset`, `page`, `next_link`, `cursor`, `none`), incremental by query param, config validation and dry run. The reference file. |
| [rest_api_auth_variants.py](examples/rest_api_auth_variants.py) | **The six auth blocks side by side** — static token, API key header, Basic, vendor login endpoint, OAuth2 client credentials, none — plus a probe loop that checks credentials without writing anything. |
| [microsoft_graph_odata.py](examples/microsoft_graph_odata.py) | **OData v4**: Microsoft Graph and Business Central through the same three options, an Entra ID service principal, `$select`/`$filter`/`$top`, and why an OData `$filter` cannot carry a watermark. |
| [netsuite_suiteql.py](examples/netsuite_suiteql.py) | **SQL over REST**: OAuth 1.0a signing (NetSuite TBA), the watermark templated into a `WHERE` clause, and monthly backfill slices under an offset ceiling. |
| [shopify_graphql.py](examples/shopify_graphql.py) | **GraphQL**: Relay cursor, pagination params inside `variables`, `template_paths` against the braces of the query, and errors returned with an HTTP 200. |
| [notebook_ingest_example.py](examples/notebook_ingest_example.py) | **Config-driven run**: a JSON source list read from `Files/conf/`, one loop, a failure summary. |

## Delta writes in Fabric (OneLake)

The local `/lakehouse/default/...` mount in Fabric notebooks does not support the atomic rename that the delta-rs transaction log commit requires (`Operation not permitted (os error 1)`, table left without a valid `_delta_log`). The library works around this automatically: in Fabric, the default path is resolved to the OneLake ABFSS URI of the notebook's default lakehouse, and writes authenticate with a storage token obtained via `notebookutils` — nothing to configure.

To target another lakehouse, pass its ABFSS URI directly:

```python
run_source(config, lakehouse_tables_path="abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables")
```

Outside Fabric (Azure or local storage), `storage_options` is passed through to delta-rs as-is.

## Technical tables

Delta tables created automatically in the technical schema (`log_schema`, default `flume`):

- **`flume.watermark`**: `source_name`, `last_value`, `updated_ts`
- **`flume.log_runs`**: `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — one row per `run_source` call, success or failure

Data columns are typed from all the values of a batch — integers and floats mixed give a `double`, not a text column — and a column that cannot be typed is written as text with the degradation reported in `RunResult.warnings`. See [Column types in Delta](docs/configuration.md#column-types-in-delta).

## Security

See [docs/security.md](docs/security.md) — threat model, supply-chain posture, secret handling, and how to report a vulnerability. Key point for operators: **the source configuration decides where tokens are sent** — protect `Files/conf/` like code.

## Development

```bash
git clone https://github.com/Y0hannH/flume-lib.git
cd flume-lib
pip install -e ".[dev]"
pytest
```

Unit tests are fully mocked — no network calls. Python ≥ 3.10 required locally.

### Release procedure

1. Bump the version in `pyproject.toml` and `src/flume_lib/__init__.py`
2. Add the version's section to [CHANGELOG.md](CHANGELOG.md), breaking changes first
3. `pytest`
4. Commit the release, then tag that commit: `git tag -a vX.Y.Z`
5. Add the tagged SHA to the table above (`git rev-list -n1 vX.Y.Z`) in a **separate** commit — a commit cannot contain its own SHA, so this one always lands after the tag
6. Push branch and tag. Tags are protected against update and deletion: review the tag before pushing, it cannot be moved afterwards
7. `python scripts/build_fabric_wheels.py`, then verify the bundle before uploading it to `Files/libs/` in the target lakehouses:

```bash
cd fabric-wheels && sha256sum -c SHA256SUMS.txt
```

Version history: [CHANGELOG.md](CHANGELOG.md).

## Out of scope

- Client-side scaffolding/installation CLI
- Asynchronous bulk-export APIs (submit a job, poll it, download a JSONL/CSV artifact — Shopify Bulk Operations, NetSuite saved-search exports). The whole library assumes one HTTP call returns one page of JSON.
- Exactly-once ingestion: writes are `append` batches, so a failed or replayed run can leave duplicates. They are identifiable by `_flume_run_id`; de-duplication is the consumer's job.

## License

[MIT](LICENSE)
