# flume-lib

> 🤖 **Are you an AI agent writing a notebook with this library?** Go straight to the [agent cookbook](docs/agent-cookbook.md): contract, decision procedure, validated configs for the common API shapes, error catalogue and a key reference, in one file.

**flume-lib loads data from REST and GraphQL APIs into Delta tables in a Microsoft Fabric lakehouse, from a Python notebook, by describing each API in a small JSON configuration instead of writing code.**

You say where the API is, how it authenticates, how it paginates and how the table should be refreshed. The library does the calls, the retries, the paging, the incremental bookkeeping and the Delta writes, and tells you exactly what happened. It is pure Python — no Spark, no per-API code.

It is built to be driven by an AI agent as much as by a person: the configuration is plain data, it is validated strictly (an unknown key is an error with a "did you mean…" suggestion, never a silent no-op), a dry run exercises the real API without writing anything, and a run always returns a structured result instead of raising.

## Is it for you?

| Use it when | Do not use it when |
|---|---|
| You ingest JSON from REST or GraphQL endpoints into a Fabric lakehouse. | The API hands you XML, CSV or SOAP, or is a bulk export (submit a job, poll it, download a file). |
| You have several APIs and want one way to ingest all of them, not one script each. | You need Spark, or your destination is not Delta. |
| You want the same notebook to run unattended: incremental loads, safe reruns, a log of every run. | You need exactly-once delivery: a failed run can leave partial data (see below). |
| You want an AI agent to write the notebooks and be able to check its own work. | The ingestion logic is a one-off that a single `requests` call would do. |

## What it looks like

One source is one dictionary (or one entry of a `sources.json` file):

```python
from flume_lib import run_source

source = {
    "name": "vendor_orders",
    "base_url": "https://api.vendor.com/v1/orders",
    "auth": {"type": "bearer_token",
             "token": {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "vendor-token"}},
    "pagination": {"type": "offset", "limit": 100, "items_field": "data"},
    "incremental": {"enabled": True, "field": "updated_at", "param_name": "updated_since"},
    "target_schema": "bronze",
    "target_table": "orders",
}

result = run_source(source)
print(result.status, result.rows_loaded, result.warnings)
```

The first run loads everything; the next ones load only what changed. Every row written carries the id and time of the run that wrote it, and each run leaves one line in a `log_runs` table. Before the first real run, `run_source(source, dry_run=True)` calls the API for real — credentials, paging, response shape — and writes nothing.

## What it takes care of

- **Authentication**: static token or API key, Basic, OAuth2 client credentials (including Microsoft Entra service principals), a vendor's own login call, OAuth 1.0a. Secrets are references to Azure Key Vault or environment variables, never values in the configuration. Tokens are renewed during long runs.
- **Pagination**: offset, page number, next link, cursor, keyset. A run that cannot tell it has reached the end **fails** instead of quietly loading part of the data.
- **Incremental loading**: a stored watermark that only moves forward, resumable runs, and date-window APIs.
- **Refresh strategy**: append, full overwrite, replace one window, or merge on a key — chosen per source.
- **Many sources**: `run_sources` runs a list of them, one after the other or in parallel, isolates their failures, prints a summary and can fail the notebook so a pipeline sees it.
- **Reliability**: bounded retries that honour `Retry-After`, limits on response size and run time, and an optional guard against two runs of the same source overlapping.
- **Typing**: column types inferred from the data, with exact `decimal` and `date` on request.

## What to know before you commit

- **At-least-once, not exactly-once.** Data is written in batches; a run that fails midway leaves its earlier batches in the table. Rows carry their run id, and `replace_where` or `merge` make reruns safe.
- **The configuration decides where credentials are sent.** Treat it like code. The library refuses literal secrets, plain `http://`, and next-page URLs that leave the API's host.
- **Nested JSON is stored as text**, in one JSON column per nested field; it is not flattened.
- **Version 0.x**: until 1.0, minor versions may contain breaking changes, always listed first in the [changelog](CHANGELOG.md).

## Requirements and installation

A Microsoft Fabric **Python notebook** (non-Spark, Python 3.10–3.12) with a **schema-enabled lakehouse** as its default. The library is installed from wheels you upload to the lakehouse — nothing is downloaded when the notebook runs:

```text
%pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.19.0
```

Details, the GitHub alternative and how to verify the upload: [docs/install.md](docs/install.md).

## Where to go next

| You want to… | Read |
|---|---|
| Have an AI agent write the notebook | [Agent cookbook](docs/agent-cookbook.md) |
| Write a configuration for a given API | [Cookbook](docs/cookbook.md) — "the vendor's API does X, write Y" |
| Look up an option, its default and its reasoning | [Configuration reference](docs/configuration.md) |
| Install it, or upgrade it | [Installation](docs/install.md) |
| Judge the security posture | [Security](docs/security.md) |
| See complete runnable notebooks | [`examples/`](examples/) |
| Work on the library or cut a release | [Development](docs/development.md) |
| See what changed between versions | [Changelog](CHANGELOG.md) |

All the technical documents are indexed in [docs/README.md](docs/README.md).

## License

[MIT](LICENSE)
