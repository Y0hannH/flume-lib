# Changelog

All notable changes to this project are documented here. Versions follow [semantic versioning](https://semver.org/); until 1.0.0, minor versions may contain breaking changes — these are always listed first.

## [Unreleased]

### Changed

- **Every message the library raises is now in English.** The exception text was French throughout — all 104 `raise` sites across the nine modules — while the README, `docs/` and the public repository were English, so a user followed English documentation and then got `ConfigError: pagination : 'page_size' doit être supérieur à 0`. The mismatch also reached further than the console: `error_message` is persisted in the `log_runs` Delta table, which means French prose was landing in the lakehouse for whatever reads it downstream. The three `RunResult.warnings` messages went with them — the empty-source notice under a replacing write mode, and the two column type-degradation notices — for the same reason, and because a run that reports an English failure and a French warning is worse than one that is consistently either. Wording, structure and the actionable second half of each message are unchanged; only the language is. **Code that matches on message text will need updating** — the exception *classes* are untouched, and remain the supported way to branch on a failure.

### Added

- **[docs/cookbook.md](docs/cookbook.md).** Everything was already documented, and finding it still meant reading. The material was organised by library concept — the auth types together, the pagination types together — while writing a config means taking five independent decisions whose answers live in five sections of two files, and nobody thinks "I need the `page` strategy": they think "the vendor returns the page count in a header". The cookbook is the missing axis: three tables mapping an observable property of an API onto the block to write, the two configs that cover most vendor APIs — a static token in a header, and a token fetched by a login call with a client id and secret — and a failure-prefix table for reading `RunResult.error_message`. It replaces nothing — `configuration.md` still explains *why*, the examples still teach — it is the entry point and the place to come back to.
- **A flat index of every configuration key**, with its block, whether it is required, and its default. That index is the half you want on the fourth source of the month, and the half most likely to rot: **[scripts/gen_key_index.py](scripts/gen_key_index.py)** generates it from the source rather than from anyone's memory. The keys come from `validation.py`, which is the authority — a key absent from those tuples is refused at validation. The defaults are read out of the implementation by AST, because that is where they live: `pagination_config.get("page_param", "page")` is the only honest statement of what `page_param` defaults to. The requirements written inline in `validate_config` — `cursor_field` once the type is `cursor`, `field` once `incremental.enabled` is true — cannot be introspected, so the script *proves* each one by building a valid config, removing the key, and demanding that validation object. A requirement that quietly disappears fails the generator instead of publishing a false line.
- **A CI job for it** (`key-index`), on the model of the SHA table check: `--check` regenerates in memory and fails if the page has drifted. Nobody regenerates a doc when adding a key, and a reference index that is wrong is worse than no index at all — it gets consulted instead of the source, and believed.

## [0.11.1] — 2026-08-25

One fix, and the reason it stayed invisible: resolving the default lakehouse to its OneLake URI had only ever been exercised where the notebook and its lakehouse share a workspace — the arrangement nobody thinks to question until a lakehouse is attached from somewhere else.

### Fixed

- **A default lakehouse attached from another workspace resolved to a path that does not exist.** The OneLake URI was assembled from `currentWorkspaceId` — the workspace of the *notebook* — and the id of the default lakehouse, which are two different workspaces as soon as the attached lakehouse comes from elsewhere. The resulting `abfss://<notebook_workspace>@…/<lakehouse_id>/Tables` points nowhere, and every run failed on the first read of `_delta_log` with a 404 that named neither the workspace nor what was wrong with it. The lakehouse's own workspace (`defaultLakehouseWorkspaceId`) is now preferred, `currentWorkspaceId` remaining the fallback for the common case where both coincide. Passing `lakehouse_tables_path` explicitly was, and remains, the way to target a lakehouse that is not the default one.

## [0.11.0] — 2026-08-25

The first release since 0.10.0 to change the library itself. Until now every write was an `append`, which made a backfill a one-shot operation: rerunning a monthly slice that had failed halfway added a second copy of its rows, and de-duplicating them by hand was the only way back. That mattered more since 0.10.0 made ingestion at-least-once. A run can now replace the window it loads instead of adding to it.

### Added

- **`write` configuration block.** `"mode": "replace_where"` with a SQL predicate replaces the matching rows of the target table with the run's rows (Delta's `replaceWhere`) — rerun the same configuration as often as you like, one copy of the window remains. `"mode": "overwrite"` replaces the table whole, for a reference table reloaded in full. `"partition_by"` sets the partition columns at creation. The default stays `append`, unchanged.
- **The replacement happens on the first batch only**; later batches of the same run append. Applying the predicate per batch would make the batches of one run erase each other, leaving a 300 000-row run with only its last 50 000.
- **A run that loads zero rows replaces nothing**, and says so in `RunResult.warnings`. An API that is down, a filter that is too narrow and a token missing a scope all answer "0 rows"; emptying a window on that signal would destroy data without anything having failed. The warning is there because the natural expectation is the opposite.
- **Delta write errors are explained.** delta-rs reports a predicate the written rows do not satisfy as `Invalid data found: N rows failed validation check` with a truncated table preview. It now arrives as a sentence naming the predicate and what it means — likewise for a predicate over a column the target table does not have, and for `partition_by` on a table already created without it.
- **[examples/write_modes.py](examples/write_modes.py)**: every write mode side by side — append, full overwrite, `replace_where` over a date window and over an id range, `partition_by`, a rolling window, what an empty source does, what a predicate that disagrees with its query does, and what a failed run leaves behind.
- **[examples/run_options.py](examples/run_options.py)**: the `run_source` parameters, which had no example of their own — dry runs, bulk validation, writing to another lakehouse or outside Fabric through `storage_options`, isolating the technical tables with `log_schema`, and reading `log_runs` back.
- **[scripts/check_readme_shas.py](scripts/check_readme_shas.py)** and a CI job for it: the README's SHA table is what makes the install-from-GitHub form usable, and a version missing from it is a version nobody can install without digging through git. v0.10.1 and v0.10.2 were both left out — the step existed in the release procedure and was forgotten twice, which is what happens to a step kept in someone's memory. Both rows have been added. The check knows a commit cannot contain its own SHA, so it ignores the tag being published and only asks from the next commit onwards.
- **`tests/test_delta_write.py`**: the first tests that actually commit to a Delta table. The rest of the suite mocks the write layer, which checks what the library asks of delta-rs but never what delta-rs does with it — and the whole value of `replaceWhere` is in the latter. Includes two `run_source` calls over the same window, asserting a single copy of the rows.

### Changed

- **Two examples renamed**, and every vendor name dropped from the repository along with them: `netsuite_suiteql.py` is now [examples/sql_over_rest_api.py](examples/sql_over_rest_api.py) and `shopify_graphql.py` is [examples/graphql_cursor_api.py](examples/graphql_cursor_api.py). Both describe an API *shape* rather than a product, which is what they always demonstrated — no library code was ever specific to either. Links in older CHANGELOG entries were repointed; the GitHub Releases already published still carry the previous wording.
- **The examples now cover every option of every block.** Filling the gaps added `timeout_seconds`, `max_rows`, `incremental.placeholder`, `incremental.checkpoint`, `retry.max_retry_after_seconds`, the `iso_date` and `any` value formats, the environment-variable form of every credential (including the historical `*_env_var` spelling), `oauth1` in its three-legged, two-legged and HMAC-SHA1 variants, `expires_in_json_path`, and the per-auth `timeout_seconds`.
- **Rejected at validation: `write.mode` other than `append` together with `incremental.checkpoint`.** Resuming mid-run would restart from the watermark and replace the window a second time, erasing what the interrupted run had already written into it. A backfill is replayed from the start of its window, not from its middle.
- **Rejected at validation: a `{placeholder}` inside `replace_where`.** The predicate is not templated — a marker would stay literal, match no row, and the run would replace nothing at all. Build the string in the notebook, one window per run.
- **`_delta.append_records()` is now `write_records()`**, taking `mode`, `predicate` and `partition_by`. Internal helper; `run_source` callers are unaffected.
- **[examples/sql_over_rest_api.py](examples/sql_over_rest_api.py)**: the monthly backfill slices are rerunnable, each replacing its own month. The transaction query now projects `trandate` through `TO_CHAR` as well — the library writes dates as strings, so a predicate over a display-format date would order `3/1/2026` before `3/10/2026` before `3/2/2026`, and replace a window other than the one loaded.

### Known limits

- A run that fails mid-way in a replacing mode leaves the window holding the batches it committed, the previous contents already gone. Delta keeps the prior version until it is vacuumed, and replaying the run rebuilds the window whole — which is what this mode is for. A source under `batch_size` never sees this: replacement and data land in one commit.
- No row-level `MERGE`. Replacement is per window, not per key.
- No `OPTIMIZE`, no `VACUUM`. A table written batch after batch accumulates small files, and replaced files stay on disk until vacuumed.

## [0.10.2] — 2026-08-24

**The library is unchanged** — `src/flume_lib` is identical to 0.10.0. This release closes the gap the 0.10.1 pipeline left open.

### Added

- **A release is gated on its own test suite.** CI and Release are separate workflows and Actions offers no `needs:` between workflows, so both now call one reusable workflow ([.github/workflows/tests.yml](.github/workflows/tests.yml)) that lints and runs the suite on 3.10, 3.11 and 3.12. Release runs it **against the exact ref it is about to publish**, before building a single batch. Until now the two workflows started in parallel on a tag push and the release verified the batches but never the code: a tag placed on a commit that had never passed CI would have been published all the same. Defining the steps once, rather than copying them into both workflows, is deliberate — two copies drift, and it is the Release copy that matters.

## [0.10.1] — 2026-08-24

**The library itself is unchanged**: `src/flume_lib` is identical to 0.10.0, so the installed package behaves exactly the same. What this release carries is the tooling around it — batch verification, an automated release pipeline, and a fix to the batch builder that was producing incomplete bundles for kernels other than the one it ran on.

### Added

- **`scripts/verify_wheels.py`**: verifies a wheel batch twice over — every file matches `SHA256SUMS.txt` (catches a truncated or corrupted transfer, no network required, so it runs on the lakehouse side) and every file matches the digest **PyPI publishes** for that name and version (catches an altered mirror or a substituted file). A wheel present but missing from `SHA256SUMS.txt` is reported too — it would be covered by no digest at all.
- **`scripts/audit_dependencies.py`**: queries [OSV.dev](https://osv.dev) for known vulnerabilities, either over the installed runtime closure or over a specific wheel batch. Exits non-zero when a vulnerability is known **and** when the check could not run — a check that did not execute is not a check that passed.
- **`audit` CI job**, on every push and pull request, and **every Monday morning**. The weekly run is the reason it exists: an offline install freezes its versions, so a CVE published after a release surfaces nowhere. Nothing in a deployed lakehouse announces that its `urllib3` became vulnerable last week.
- **Automated releases** ([.github/workflows/release.yml](.github/workflows/release.yml)): pushing a `v*` tag builds one offline batch **per Fabric kernel version** (3.10, 3.11, 3.12), verifies each — digests against PyPI, dependencies against OSV — and publishes the GitHub Release with the three zips and the wheel attached, using the CHANGELOG section as its notes. A failing check stops the publication. Runnable manually from the Actions tab for a tag that is already pushed.
- **`scripts/release_notes.py`**: extracts a version's section from the CHANGELOG. Exits non-zero when the section is missing — a release without notes is a release published without knowing what it contains.
- **[docs/security.md](docs/security.md) gains "Verifying a batch"** and "What these checks do not prove" — including that PyPI stopped signing packages in 2023, so a digest match proves the file equals what PyPI serves and nothing about whether what PyPI serves is sound.

### Fixed

- **A batch built for another Python version was silently incomplete.** `pip download --python-version` drives wheel tags and `Requires-Python` only — environment markers stay evaluated against the *running* interpreter. Building a 3.11 batch from a 3.12 machine therefore produced the right compiled wheels but dropped `typing-extensions`, which `arro3-core` requires below 3.12, and the offline install failed at the client with no earlier warning. `build_fabric_wheels.py` now defaults to the running interpreter's version and refuses a mismatch unless `--allow-marker-mismatch` is passed. The 0.10.0 batch published for 3.12 is unaffected — below-3.12 was the only version-conditional dependency, and it was correctly excluded.
- **The batch zip now carries its kernel version** (`…-fabric-wheels-py312.zip`). Two batches for two kernels were indistinguishable by name, and uploading one for the other only showed at install time.

### Documentation

- The release procedure now describes the automated workflow, and the offline verification to run after upload — the transfer is where a file gets truncated.

## [0.10.0] — 2026-08-24

Robustness pass on long runs. A source of a few million rows used to be an all-or-nothing bet: everything was accumulated in memory for a single write, the token expired mid-way, and nothing bounded a pagination that stopped progressing. The five changes below address that, and the type inference that silently turned a column of amounts into a column of text.

### Breaking

- **Ingestion is now at-least-once.** Records are written in batches of `batch_size` rows (default 50 000) instead of one write at the end of the run, so a failed run can leave partial data behind — where it previously left none. Rows of a run all carry the same `_flume_run_id`, so a failed run is identifiable and removable (`DELETE FROM … WHERE _flume_run_id = '…'`). This is the price of a run's memory no longer depending on the size of its source, and of a run that breaks on page 900 keeping the first 899.
- **`RunResult.rows_loaded` counts the rows actually written**, not the rows read. On a failed run it is no longer 0 but however far the run got — which is what makes it usable as a resumption point.
- **`append_records()` returns `(types, fallbacks)`** instead of `None`, and **`records_to_table()` returns `(table, fallbacks)`**. Internal helpers; `run_source` is unaffected.

### Added

- **`batch_size`**: rows buffered before each Delta write. A source smaller than one batch still produces a single commit, as before.
- **`incremental.checkpoint`**: commits the watermark after each batch instead of once at the end, so an interrupted run resumes where it stopped rather than replaying the whole window. Only correct on a source sorted by `incremental.field` — the library checks it, and a batch that goes backwards fails the run **before writing**, rather than advancing a watermark that would silently skip rows.
- **Mid-run token renewal** for `oauth2_client_credentials` and `token_endpoint`. Proactively when the endpoint announces a lifetime (`expires_in`, or the path given in the new `expires_in_json_path`), 60 seconds before expiry; reactively on a 401 otherwise, replaying the page immediately without backoff. Once per page: a freshly issued token refused again is not an expiry, and the run fails with the 401. Static credentials and `oauth1` are never renewed — their 401 is a configuration error.
- **`keyset` pagination**: each page is filtered by the key of the last record of the previous one (`id > last`). The cost of a page does not grow with its depth and nothing caps it — the only strategy that reaches the bottom of a table on APIs that bound the offset, 100 000 being a common ceiling. Requires a source sorted by the key, with unique values; the library verifies the key advances and stops instead of looping.
- **`pagination.params_in: "body_template"`**: for SQL-over-REST endpoints where the key belongs inside the query. Routing is **per param**: one whose `{placeholder}` appears in `body` is substituted there, every other one goes to the query string — so a SQL-over-REST source can carry the keyset key in its SQL *and* `limit` in the URL, which is where such endpoints expect it. Nothing is dropped in between. `value_format` is mandatory on the key — it comes back from the API, same rule and same reason as the incremental watermark — and the placeholder it names must exist in `body`, or the config is rejected.
- **`pagination.max_pages` / `pagination.max_rows`**: bounds on a run. Reaching one is an **error**, not a clean stop — truncating silently would produce a `success` run short of part of its data.
- **`RunResult.warnings`**: degradations a run survived. A `success` run can carry them; that is the point.
- **`py.typed`**: the library is typed, consumers can now use it.

### Fixed

- **Column types are inferred from all the values of a column**, not from the first non-null one. A column of amounts starting with an integer (`[10, 10.5]`) was inferred `bigint`, the Arrow build failed, and the fallback wrote the **whole column as text** — with no signal whatsoever. Integers mixed with floats now give a `double`.
- **The remaining text fallback is reported** in `RunResult.warnings` instead of being silent.
- **The types chosen by the first batch apply to the following ones.** Without this, batched writing could produce two incompatible schemas for the same table within a single run.
- **The max of the incremental field is computed before the batch is written.** A field with mixed types used to fail `max()` *after* the append, leaving rows behind a `failed` run with no watermark to cover them.
- **A page identical to the previous one stops the run.** An API that clamps an out-of-range page number and re-serves the first page has no natural stop condition; `offset` and `page` looped until the notebook timed out, memory climbing.

### Packaging

- **`arro3-core` is now a declared dependency.** `_delta.py` imports it directly; it only arrived through `deltalake`. The Fabric wheel set is unchanged — it already contained it.
- **CI** ([.github/workflows/ci.yml](.github/workflows/ci.yml)): lint and test suite on Python 3.10, 3.11 and 3.12 — the three Fabric kernel versions — on every push and pull request, then a wheel build checked for every module and the `py.typed` marker. A wheel installed offline from the lakehouse only reveals a missing module when a notebook imports it.
- **ruff** (E, F, W, I, UP, B) and pytest configured in `pyproject.toml`.

### Known limitations

- The no-regression check on `incremental.checkpoint` compares against the maximum seen **during the current run**, not the watermark stored at its start. An API that returns rows older than the stored watermark can therefore move it backwards — pre-existing behavior, unchanged.
- Warnings live in `RunResult` only; `log_runs` does not carry them.
- A column that changes type from one run to the next is still a `SchemaMismatchError` at commit time. Only within a run is the type stabilized.

## [0.9.0] — 2026-08-24

GraphQL APIs are now reachable without any endpoint-specific library code. The four additions below are generic; together they cover the `edges`/`node` connection convention every GraphQL API of this shape uses — see [examples/graphql_cursor_api.py](examples/graphql_cursor_api.py).

### Added

- **`cursor` pagination**, previously a stub. The next cursor is read from the response by dotted path (`cursor_field`) and sent back in `cursor_param`; the first request goes without one. `has_more_field` — the `pageInfo.hasNextPage` of GraphQL connections — takes precedence over the empty-page heuristic when present, because a heavily filtered connection can return an empty page in the middle of its results. Optional `limit`/`limit_param` for the page size (GraphQL: `first`).
- **Dotted paths and unwrapping in record extraction.** `items_field` accepts `data.orders.edges`; `record_field` unwraps each item (`node`), the shape every such connection uses. An `items_field` that resolves to something other than a list is now an explicit error instead of whatever came next.
- **`pagination.params_path`**: with `"params_in": "body"`, nests the pagination params under a dotted path of the payload instead of merging them at the root. GraphQL expects the page size and cursor inside `variables`, beside `query` rather than next to it. The branch is created if the body does not carry it.
- **`errors`**: declares the application-error envelope of APIs that report failures inside a successful HTTP response (`path`, `code_field`, `message_field`, `retryable_codes`), with defaults matching the GraphQL specification. Codes listed in `retryable_codes` are replayed under the `retry` policy — that is how cost-based throttling arrives when the API does not use a 429. A `Retry-After` header on such a response is still honored.
- **`template_paths`**: restricts `{placeholder}` substitution to the listed dotted paths of `body`. A body that uses braces for its own syntax cannot be scanned wholesale — a compact GraphQL query (`{orders{edges{node{id}}}}`) contains `{id}`, indistinguishable from a placeholder, and failed the run. A listed path must exist in `body`.
- **`examples/graphql_cursor_api.py`**: a commerce admin GraphQL API end to end — token in Key Vault, cursor connection, incremental on `updatedAt` through the API's search syntax, monthly backfill slices, and the scope caveat on older records.

### Fixed

- **`items_field` and `record_field` are honored without pagination** (`"type": "none"` or absent). A single-call source against a nested response previously fell back to probing `data`/`items`/`results`/`value` and failed.

### Documentation

- **[docs/configuration.md](docs/configuration.md) gains a GraphQL section**: the option-by-concept mapping, a complete source, the request bodies it actually produces, how to write the query so the injected variables land correctly, how nested selections are stored in Delta, and a table of common failure modes with their cause.
- **[docs/security.md](docs/security.md) gains "Values that come back from a response"**, covering the three values an API can influence the next request with — the watermark (filtered and format-checked), the cursor (passed as a discrete parameter, never concatenated), and the `next_link` URL. That last one is documented for the first time: `next_link` fetches whatever URL the response provides, on the authenticated session, so a hostile endpoint can redirect the next page to a host of its choosing and receive the auth header. Pre-existing behavior, inherent to the strategy, and not applicable to `offset`/`page`/`cursor`/`none`.
- **Application errors in `log_runs`** are documented as what they are: text written by the remote API, truncated, landing in a table readable by everyone with lakehouse access.

### Security

- Error messages coming from an API are truncated (3 errors, 500 characters) before reaching `log_runs`. A GraphQL error quotes the failing query in full; stored page after page, that turns a technical table into a copy of the request.

### Known limitations

- An `errors` block is **opt-in**. Without it, behavior is unchanged: a response carrying valid `data` alongside an `errors` entry — one field refused by a missing scope, typically — is still reported `success`, short of part of what was asked for. Existing sources are unaffected; new GraphQL sources should always declare it.
- Asynchronous bulk-export APIs remain out of scope: they submit a job, poll it and download a JSONL artifact, which does not fit the "one call, one page of JSON" model.

## [0.8.1] — 2026-08-22

Aucun changement fonctionnel : la bibliothèque installée est identique à la
0.8.0. Hygiène de release uniquement — inutile de redéployer en urgence.

### Fixed

- **`.gitattributes` normalise les fins de ligne en LF** dans le dépôt et dans la copie de travail. Sans lui, un fichier créé sous Windows restait en CRLF : la wheel construite sous Windows embarquait des modules en CRLF, celle construite sous Linux les mêmes en LF, et le lot de wheels n'était pas reproductible d'une machine à l'autre (vérifié sur la 0.8.0 : 4 modules sur 11 en CRLF). Sans effet à l'exécution — Python lit en universal newlines — mais le lot livré n'était pas comparable à sa source.
- **Procédure de release du README complétée** : mise à jour du CHANGELOG, ordre tag / commit du SHA explicité (un commit ne peut pas contenir son propre SHA), immuabilité des tags rappelée, et vérification du lot par `sha256sum -c` avant dépôt dans le lakehouse.

## [0.8.0] — 2026-08-22

### Added

- **`headers`**: fixed HTTP headers on every data call, for APIs that require a non-authentication header (`Prefer`, `Accept-Language`, tenant selectors). Literal strings only — a secret reference is rejected, credentials belong in `auth`. Auth headers are applied last and cannot be overridden.
- **`oauth1` auth type**: OAuth 1.0a request signing (RFC 5849, HMAC-SHA256/SHA1), with `realm` support. Unlike the other types the signature depends on the URL and query params of each request, so it is recomputed page after page — `build_auth()` now returns a `(headers, signer)` pair and the signer is installed on the session. Implemented on the standard library, no new dependency. Covers ERP token-based authentication and legacy OAuth 1.0a APIs.
- **Body templating**: strings in `body` and `params` may contain `{placeholder}` markers. Combined with `incremental.inject: "body_template"`, the watermark lands inside the request body instead of the query string — required for SQL-over-REST endpoints where the filter lives in the query itself. A placeholder with no matching variable fails the run; interpolated values are rejected if they contain characters that could change the structure of the query (`'`, `"`, `;`, `--`, `/*`, backslash, newline).
- **`incremental.initial_value`**: value used on the very first run, before any watermark exists. Applies to both injection modes.
- **`incremental.value_format`**: `any` (default), `numeric`, `iso_date` or `iso_datetime` — validates the watermark before it is used.
- **`Retry-After` is honored** on 429 and 5xx responses (delay in seconds or HTTP date), instead of the local exponential backoff. Capped by `retry.max_retry_after_seconds` (default 300). Retrying earlier than a server asked is what gets a client banned on APIs with strict governance.

### Security

- **`incremental.value_format` is required** with `inject: "body_template"`. The forbidden-character check only protects a placeholder inside quotes; a bare one (`WHERE id > {last_id}`) accepted `0 OR 1=1`, which contains none of them. An explicit `numeric` / `iso_date` / `iso_datetime` constrains the value to a shape with no room for syntax.
- **Query strings are stripped from URLs in error messages.** `raise_for_status()` produced a message containing the full URL, which is persisted in `log_runs` — a Delta table readable by everyone with lakehouse access. `RunResult.rows_loaded` still indicates how far a run got.

### Known limitations

- `oauth1` and HTTP redirects do not mix: the signature covers the URL, and `requests` replays the original `Authorization` header on a same-host redirect rather than re-signing, so the API answers 401. Point `base_url` at the final URL.

### Changed

- `build_auth_headers()` still exists but raises on `oauth1`, which cannot be expressed as a static header; use `build_auth()`.

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
