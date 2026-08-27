# flume-lib

Generic API ingestion accelerator for **Microsoft Fabric Python notebooks** (non-Spark). Pure Python: Delta writes via [delta-rs](https://github.com/delta-io/delta-rs) (`deltalake`), no PySpark dependency.

Point it at a JSON list of API sources; it handles authentication (Key Vault-backed secrets, OAuth2 service principals, OAuth 1.0a signing, custom login endpoints), pagination (offset, page, next-link, cursor, keyset), incremental loading with watermarks, bounded retries, and writes everything — data and run logs — as Delta tables in a schema-enabled lakehouse. REST and GraphQL endpoints are both covered by the same generic options — no per-API code.

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
   %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.15.0
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
| v0.16.0 | `dad44901a257e3a3b07ca303fd00e11060c5b899` |
| v0.15.0 | `0087a656f1c1e073e3aa359f547dfc771bd6b876` |
| v0.14.0 | `adab6dced6d596c63612ac8340560274fd985872` |
| v0.13.0 | `cc9194d4de86fb9dc3897e9abfa2f67adadeab1e` |
| v0.12.0 | `08fa0ee46487ee08cbe791bfd045f1971dbf8b76` |
| v0.11.1 | `317604e68657fd5cabfe1740863924d7d35583af` |
| v0.11.0 | `411dfcc762f8ffe3744c1ba494ddf30585cdb9a8` |
| v0.10.2 | `3e9c199bbe0def4c279809c166129f90ac8e01c8` |
| v0.10.1 | `aaf83b668cd4c3840cb8e8f394340d0e596fb3a3` |
| v0.10.0 | `67c0d77537ed80a73effda8a9bc26e766174c805` |
| v0.9.0 | `809a82df70d6790574853e1329b8f3bf5b1b663c` |
| v0.8.1 | `8e26f5e250a690f77b7ca7e196c855588efd76eb` |
| v0.8.0 | `526840b400baaf5de3889af5b0d783833fa9066a` |
| v0.7.0 | `d69aad0b1539558266fdbf02f20b327bbf4f3c71` |
| v0.6.0 | `c71716012f6c846d900ddae10892c683ec5b58b3` |
| v0.5.0 | `679a15adf13b7169a520194b76ee8cfd2cfe48aa` |
| v0.4.0 | `75fb767dad8c453930ad5249c2b4540c5f263ce0` |

Find a version's SHA with `git rev-list -n1 vX.Y.Z` or on the [tags page](https://github.com/Y0hannH/flume-lib/tags).

This table is kept honest by CI: [scripts/check_readme_shas.py](scripts/check_readme_shas.py) fails the build when a published tag is missing from it. A tag can only be added to the table by a commit that comes *after* it — a commit cannot contain its own SHA — so the check ignores the tag being published and starts asking on the next commit.

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

> ⚡ **Writing a config right now**: [docs/cookbook.md](docs/cookbook.md) — "the vendor's API does X → write Y" for auth, pagination and reload strategy, plus a flat index of every key with its default, generated from the source. Start here once you know the library.
>
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
| `oauth1` | OAuth 1.0a request signing (RFC 5849, HMAC-SHA256/SHA1, `realm`) — ERP token-based auth and legacy OAuth 1.0a APIs |

`oauth2_client_credentials` and `token_endpoint` tokens are **renewed mid-run**: proactively when the endpoint announces an `expires_in`, and reactively on a 401 otherwise (once per page — a freshly issued token refused again fails the run). A run longer than the token's lifetime no longer dies on its last pages. The other types carry a static credential and are never renewed; `oauth1` signs every request individually, so nothing expires mid-run.

Beyond authentication, `headers` adds fixed headers to every data call (literal strings only — a secret belongs in `auth`).

### Pagination

| Type | Purpose |
|---|---|
| `offset` | Offset/limit query params; stops on empty or partial page |
| `page` | Page number; total page count read from a response header (`total_pages_header`) or from the body (`total_pages_field`), or stop on empty/partial page |
| `next_link` | Follows a next-page URL from the response body (e.g. `@odata.nextLink`) |
| `cursor` | Opaque cursor read from the response; `has_more_field` covers GraphQL connections |
| `keyset` | Filters each page by the key of the last record seen (`id > last`); the only strategy that gets past an offset cap |

Records are located with `items_field` (a dotted path — `data.orders.edges`) and, where each item is a wrapper, unwrapped with `record_field` (`node`).

Data endpoints can be `GET` (default) or `POST`/`PUT`/`PATCH` via `method` + `body`; `pagination.params_in` decides whether pagination params go to the query string, are merged into the request payload as JSON values, or — with `body_template`, for SQL-over-REST endpoints — are routed per param: those whose `{placeholder}` appears in `body` are substituted there, the others take the query string. `params_path` says where inside that payload (GraphQL: `variables`).

Some APIs cap how far an offset may go (100 000 is a common ceiling). `keyset` is the answer: it filters on the last key seen instead of counting rows, so depth costs nothing and no cap applies — at the price of a source sorted by that key. Slicing the source into bounded windows remains an option; see [examples/sql_over_rest_api.py](examples/sql_over_rest_api.py).

`max_pages` and `max_rows` bound a run. Reaching one **fails** the run rather than truncating it silently. Independently of them, a page identical to the one before it stops the run — an API that clamps an out-of-range page number and re-serves the first one has no natural stop condition.

### Incremental (watermark)

When `incremental.enabled`, the last watermark is read from `<log_schema>.watermark` and sent as a query param (`param_name`). After a **successful** run only, the max of `incremental.field` over the loaded records becomes the new watermark.

With `"inject": "body_template"` the watermark is substituted into the `{placeholder}` markers of `body` instead — for APIs whose filter lives inside the request payload, such as an SQL-over-REST endpoint. `initial_value` provides the floor used on the very first run, and `value_format` validates the watermark before it is used.

`"checkpoint": true` commits the watermark after each batch instead of at the end of the run, so an interrupted run resumes where it stopped — see [Batched writes](#batched-writes).

### Batched writes

Rows are written by batches of `batch_size` (default 50 000) instead of being accumulated for one write at the end. A run's memory no longer depends on the size of the source, and a run that breaks half-way keeps what it already wrote — `RunResult.rows_loaded` counts the rows actually committed. A source smaller than one batch still produces a single commit.

The trade-off is at-least-once ingestion: a failed run can leave partial data. Every row carries `_flume_run_id`, so a failed run's rows are identifiable and removable. Pair with `incremental.checkpoint` to make the next run resume instead of replay.

### Write mode

By default a run appends, which makes a backfill a one-shot: rerunning a slice that failed halfway adds a second copy of its rows. `write` changes that.

```json
"write": {
  "mode": "replace_where",
  "replace_where": "trandate_iso >= '2026-01-01' AND trandate_iso < '2026-02-01'"
}
```

The window described by the predicate is replaced rather than appended to, so the run is **rerunnable** — however many times it runs, one copy of January remains. `mode: "overwrite"` does the same for the whole table, for a reference table reloaded in full. `partition_by` sets the partition columns at creation.

delta-rs refuses to commit rows falling outside the predicate, so a predicate and a query that disagree fail the run instead of replacing the wrong window — derive both from the same bounds. A run that returns zero rows replaces nothing and says so in `RunResult.warnings`: an API that is down answers "0 rows" too, and emptying a window on that signal would destroy data without anything having failed. Details, and why this cannot be combined with `incremental.checkpoint`: [docs/configuration.md#write-mode](docs/configuration.md#write-mode).

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

Full walkthrough with the request bodies it produces, how to write the query, how nested selections land in Delta, and a table of common failure modes: [docs/configuration.md#graphql-endpoints](docs/configuration.md#graphql-endpoints). Working notebook: [examples/graphql_cursor_api.py](examples/graphql_cursor_api.py).

## Examples

Every file in [`examples/`](examples/) is a complete Fabric notebook, runnable as-is once the URLs and secret names are yours. Start with the first one — the others assume it. Between them they exercise **every option of every block**: each auth type, each pagination strategy, each write mode, each value format, and the four `run_source` parameters.

| Example | What it covers |
|---|---|
| [rest_api_paginated.py](examples/rest_api_paginated.py) | **The ordinary REST API.** One fictional endpoint read with each of the five pagination shapes (`offset`, `page`, `next_link`, `cursor`, `none`), incremental by query param, config validation and dry run. The reference file. |
| [rest_api_auth_variants.py](examples/rest_api_auth_variants.py) | **The seven auth blocks side by side** — static token, API key header, Basic, vendor login endpoint, OAuth2 client credentials, OAuth 1.0a signing, none — each in its Key Vault and environment-variable form, plus a probe loop that checks credentials without writing anything. |
| [microsoft_graph_odata.py](examples/microsoft_graph_odata.py) | **OData v4**: Microsoft Graph and Business Central through the same three options, an Entra ID service principal, `$select`/`$filter`/`$top`, and why an OData `$filter` cannot carry a watermark. |
| [sql_over_rest_api.py](examples/sql_over_rest_api.py) | **SQL over REST**: OAuth 1.0a request signing, the watermark templated into a `WHERE` clause, mid-run resumption with `checkpoint`, and rerunnable monthly backfill slices under an offset ceiling. |
| [graphql_cursor_api.py](examples/graphql_cursor_api.py) | **GraphQL**: cursor connections, pagination params inside `variables`, `template_paths` against the braces of the query, and errors returned with an HTTP 200. |
| [write_modes.py](examples/write_modes.py) | **Every write mode side by side**: append, full overwrite, `replace_where` over a date window and over an id range, `partition_by`, rolling windows, what an empty source does, what a mismatched predicate does, and what a failed run leaves behind. |
| [run_options.py](examples/run_options.py) | **The `run_source` parameters**: dry runs, bulk validation, writing to another lakehouse or outside Fabric (`storage_options`), isolating the technical tables with `log_schema`, and reading back `log_runs`. |
| [notebook_ingest_example.py](examples/notebook_ingest_example.py) | **Config-driven run**: a JSON source list read from `Files/conf/`, one loop, a failure summary. |

## Delta writes in Fabric (OneLake)

The local `/lakehouse/default/...` mount in Fabric notebooks does not support the atomic rename that the delta-rs transaction log commit requires (`Operation not permitted (os error 1)`, table left without a valid `_delta_log`). The library works around this automatically: in Fabric, the default path is resolved to the OneLake ABFSS URI of the notebook's default lakehouse, and writes authenticate with a storage token obtained via `notebookutils` — nothing to configure.

The default lakehouse may live in another workspace than the notebook — the URI carries the lakehouse's workspace, not the notebook's. If the runtime context does not expose it, resolution falls back to the notebook's workspace and the run fails with a 404 on `_delta_log`; pass `lakehouse_tables_path` explicitly in that case.

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
ruff check .
pytest
```

Unit tests are fully mocked — no network calls. Python ≥ 3.10 required locally.

[CI](.github/workflows/ci.yml) runs the lint and the suite on 3.10, 3.11 and 3.12 — the three Fabric kernel versions — on every push and pull request, then builds the wheel and checks it carries every module and the `py.typed` marker. A wheel installed offline from the lakehouse only reveals a missing module when a notebook imports it.

An `audit` job queries [OSV.dev](https://osv.dev) for known vulnerabilities in the runtime dependencies, on every push **and every Monday**. The weekly run is the point: an offline install freezes its versions, so a CVE published after a release would surface nowhere otherwise. See [Verifying a batch](docs/security.md#verifying-a-batch) for the two scripts and what they do — and do not — prove.

### Release procedure

1. Bump the version in `pyproject.toml` and `src/flume_lib/__init__.py`, **and the pinned version of the `%pip install` lines** in this README and in `examples/` (`grep -rn "flume-lib==" README.md examples/`). These belong to the release commit, not after the tag: otherwise the tagged tree — and every artifact built from it — tells readers to install the previous version.
2. Add the version's section to [CHANGELOG.md](CHANGELOG.md), breaking changes first. The release workflow uses it as the release notes and refuses to publish without it.
3. `ruff check . && pytest`
4. Commit the release, then tag that commit: `git tag -a vX.Y.Z`
5. Add the tagged SHA to the table above (`git rev-list -n1 vX.Y.Z`) in a **separate** commit — a commit cannot contain its own SHA, so this one always lands after the tag. `python scripts/check_readme_shas.py` prints the line to add, and CI fails on the next push if it is missing: v0.10.1 and v0.10.2 were both forgotten here before the check existed
6. Push branch and tag. Tags are protected against update and deletion: review the tag before pushing, it cannot be moved afterwards
7. Pushing the tag triggers [the release workflow](.github/workflows/release.yml): it builds one offline batch **per Fabric kernel version** (3.10, 3.11, 3.12), verifies each one, and publishes the GitHub Release with the three zips and the wheel attached, using the CHANGELOG section as its notes. Nothing to build by hand.

   A batch must be built by the interpreter of the version it targets — `pip download --python-version` only drives wheel tags and `Requires-Python`, never environment markers — which is why there is one runner per version rather than one runner for three batches. Building a 3.11 batch from a 3.12 machine silently drops `typing-extensions`, and the offline install then fails at the client. `build_fabric_wheels.py` refuses that combination by default.

   To publish a tag that is already pushed, run the workflow manually from the Actions tab with the tag as input.

8. Download the batch matching the kernel from the Release, upload the `.whl` files to `Files/libs/`, and verify on the lakehouse side — the transfer is where a file gets truncated:

```bash
python scripts/verify_wheels.py /lakehouse/default/Files/libs --offline
```

`--offline` skips the PyPI comparison, so this one needs no network. The script is standard-library only and can be pasted straight into a notebook cell when a shell is not available.

Version history: [CHANGELOG.md](CHANGELOG.md).

## Out of scope

- Client-side scaffolding/installation CLI
- Asynchronous bulk-export APIs (submit a job, poll it, download a JSONL/CSV artifact). The whole library assumes one HTTP call returns one page of JSON.
- Exactly-once ingestion: batches are committed one by one, so a run that fails mid-way leaves partial data behind — rows written, or a window replaced only in part. What a rerun no longer leaves is *duplicates*, provided it covers a window `write.replace_where` can name; an appending source still needs de-duplication on `_flume_run_id`, which is the consumer's job.
- Row-level upserts (Delta `MERGE`): replacement is per window, not per key. A source that only exposes changed rows without a window to bound them is still appended.

## License

[MIT](LICENSE)
