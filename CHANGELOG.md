# Changelog

All notable changes to this project are documented here. Versions follow [semantic versioning](https://semver.org/); until 1.0.0, minor versions may contain breaking changes — these are always listed first.

## [0.7.0] — 2026-08-03

### Breaking

- **Configuration is now strictly validated.** Unknown keys raise a `ConfigError` instead of being silently ignored. A config that previously "worked" with a typo (`pagintaion`, `retrry`) or a stray key will now fail fast — which is the point: a misspelled optional key used to silently disable a whole feature (a typo on `pagination` meant a single API call and a run reported as `success` with most of the data missing). Run `validate_config(config)` over your existing sources before upgrading.
- **Written rows carry two extra columns**, `_flume_run_id` and `_flume_ingested_at`. Existing tables absorb them through `schema_mode=merge`; downstream `SELECT *` consumers will see them.

### Added

- **Lineage columns** on every written row: `_flume_run_id` (matches `RunResult.run_id` and the `log_runs` entry) and `_flume_ingested_at`. Makes it possible to trace a row back to its run, de-duplicate after a partial retry, or isolate the rows of a run to discard.
- **`validate_config(config)`** exported at package level, with `ConfigError`. Validates required keys, known types, per-type key sets, and required credential groups. Unknown keys get a "did you mean…" suggestion. Useful to check every source before a long batch.
- **POST (and PUT/PATCH) data endpoints** via `method` and `body`. `pagination.params_in` controls whether pagination and incremental params go to the query string (`"query"`, default) or are merged into the request body (`"body"`), covering search/reporting APIs that paginate inside the payload. `body_format` selects JSON or form encoding.
- **Dry-run mode**: `run_source(config, dry_run=True)` validates the config, really calls the API (so credentials and pagination are exercised too) and counts rows, but writes nothing — no data, no watermark, no `log_runs` entry. The first records are returned raw in `RunResult.sample`. Rows are counted without being accumulated in memory, so a dry run is safe on any source size.

## [0.6.0] — 2026-08-03

### Added

- `docs/security.md`: threat model, supply-chain posture, secret handling, vulnerability reporting.
- `LICENSE` (MIT) and a documented release procedure in the README.

### Changed

- Schema and table names from the configuration are validated before use in a path, blocking path traversal from a malformed or hostile config.
- `token_endpoint` rejects secret references in the body when `method` is `GET` (query params leak into server and proxy logs).
- All documentation translated to English.

## [0.5.0] — 2026-08-03

### Breaking

- **Schema-enabled lakehouses only.** Each source must declare `target_schema`; data is written to `Tables/<target_schema>/<target_table>`. Technical tables moved to a dedicated schema (`log_schema`, default `flume`).

## [0.4.0] — 2026-08-03

### Fixed

- **Delta writes in Fabric.** The local `/lakehouse/default/…` mount does not support the atomic rename required by the delta-rs transaction log commit, leaving tables without a valid `_delta_log` (`Operation not permitted (os error 1)`). The default path is now resolved to the lakehouse's OneLake ABFSS URI, authenticated with a storage token from `notebookutils`.
- Resolved secrets are stripped of surrounding whitespace — a trailing newline in a Key Vault secret produced opaque 401s.

### Added

- `storage_options` parameter on `run_source`, passed through to delta-rs for non-Fabric storage.

## [0.3.0] — 2026-08-03

### Added

- `bearer_token`: configurable `header_name` and `value_prefix`, for APIs expecting the token in a custom header.
- `page` pagination strategy: page-number iteration with the total page count read from a response header (`total_pages_header`).

## [0.2.0] — 2026-08-03

### Added

- **Secret references**: any credential can be `{"env_var": …}` or `{"keyvault_url": …, "secret_name": …}`, resolved at runtime via `notebookutils` in Fabric or `azure-identity` outside (extra `[azure]`).
- `oauth2_client_credentials` implemented — Entra ID service principals via `tenant_id`, any IdP via `token_url`.
- `token_endpoint`: token obtained through an arbitrary login call, extracted by dotted JSON path.

## [0.1.0] — 2026-08-03

Initial release: `run_source(config) -> RunResult` (never raises), `bearer_token`/`api_key_header`/`basic` auth, `offset`/`next_link` pagination, incremental watermark, `log_runs` table, pure-Python Delta writes via delta-rs and arro3 (no PySpark).
