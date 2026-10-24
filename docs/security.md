# Security

Threat model and security posture of flume-lib. Audience: anyone deploying the library in a client workspace, and contributors reviewing changes.

## Threat model

**Trusted:**

- The **source configuration** (`sources.json` or equivalent). It decides where tokens are sent (`base_url`, `token_url`) and where data is written. **Anyone who can edit the configuration can redirect an API token to a server they control.** Store it with the same access restrictions as code: workspace write access should be limited, and changes to `Files/conf/` should be deliberate acts.
- The Fabric runtime (`notebookutils`), the notebook identity, and the lakehouse it is attached to.
- The wheels uploaded to `Files/libs/<version>/` (see [Supply chain](#supply-chain)).

**Untrusted:**

- Remote API responses (data, headers, pagination fields, application errors). They are parsed defensively: pagination stops are bounded by explicit conditions, record structures are serialized, and nothing from a response is ever executed or used to build a filesystem path. Four values travel back out into the next request — see [Values that come back from a response](#values-that-come-back-from-a-response).
- GitHub and PyPI **at runtime** — by design they are not on the execution path when using the recommended install.

## Supply chain

- **Recommended install is fully offline**: wheels (library + all dependencies, at the exact versions fixed by a reviewed lockfile — see below) are uploaded to the lakehouse and installed with `pip --no-index`. Nothing is downloaded at runtime; a compromise of GitHub or PyPI after the release cannot affect running notebooks.
- **The wheels folder is the trust boundary**: whoever can write to it decides what runs in every notebook installing from it. Restrict write access to `Files/libs/` the same way you would restrict access to code, and pin the version (`flume-lib==X.Y.Z`) so an added batch cannot silently upgrade notebooks. `SHA256SUMS.txt` ships with each batch for verification.
- **Git installs must pin a full commit SHA**, never a tag: tags are mutable, SHAs are not.
- **Repository hardening**: two active rulesets, with no bypass actor. `protect-main` forbids deleting `main` and rewriting its history; `protect-tags` forbids updating or deleting any `v*` tag. Modifying a ruleset requires web-authenticated admin access (2FA), not just a git credential. What they do **not** do: they do not require a pull request, a review or passing checks before a change lands on `main`, and they do not restrict who may *create* a `v*` tag — which starts the release workflow, and that signs and publishes whatever the tagged commit builds. Treat write access to the repository as release authority.
- **The dependencies of a batch are fixed by a reviewed lockfile, with hashes.** `requirements/runtime.txt` lists every package that goes into the offline bundle — the whole closure, one version each, with the SHA-256 of every file PyPI publishes for it. `scripts/build_fabric_wheels.py` downloads from it with `pip download --require-hashes --no-deps`: a file whose digest is not in the lockfile stops the build instead of entering the bundle. Before, `pip download` resolved the versions on the day of the release, so whatever PyPI served at that moment became what the client installed, unreviewed. The moment of trust is now the pull request that changes the lockfile (Dependabot opens it weekly), which shows exactly which version moved. On Linux the build also installs the library from the bundle alone, without an index and without writing anything, to prove the lockfile covers every dependency; the CI job `lockfile` fails when `pyproject.toml` and the lockfile disagree.
- **GitHub Actions are pinned to a commit SHA**, not to a tag (`actions/checkout@<sha> # vX.Y.Z`) — the same rule as for git installs, and for the same reason: a tag can be moved after the fact, and every workflow here runs with the repository's permissions. `.github/dependabot.yml` proposes SHA updates in a pull request. `tests/test_security_notes.py` fails when a workflow step uses anything but a 40-character SHA.
- **Each release is signed.** The `publish` job attests the wheel and the bundles with `actions/attest-build-provenance` (Sigstore, keyless): a signature binds each file to this repository, this workflow and this commit. `SHA256SUMS.txt` cannot do it — it travels next to the files it protects, so whoever can rewrite a file rewrites its digest too. Verify a downloaded file with `gh attestation verify <file> --repo Y0hannH/flume-lib`.
- **Dependency bounds are tested, not declared.** Each lower bound in `pyproject.toml` is installed by the `minimum-versions` job, and each upper bound is the next major version. Measuring them found that the declared floors were wrong: with `arro3-core` below 0.8 the watermark read panics or the suite fails, and `deltalake` before 1.3.2 fails at least one test (a refused `replace_where` is no longer explained).

### Verifying a batch

Two scripts, two different questions. Both are standard-library only, so either can be pasted into a Fabric notebook cell.

**Integrity** — is this the file it claims to be?

```bash
python scripts/verify_wheels.py                    # fabric-wheels/, both checks
python scripts/verify_wheels.py --offline          # local digests only, no network
python scripts/verify_wheels.py /path/to/libs      # another folder
```

It runs two independent checks. Every wheel matches `SHA256SUMS.txt` — which catches a file truncated, corrupted or missing after a transfer, and needs no network, so it is the one to run on the lakehouse side after upload. And every wheel matches the digest **PyPI publishes** for that filename and version — which catches an altered mirror or a file substituted between PyPI and the batch. The one exception is flume-lib's own wheel: it is built here and is not on PyPI, so only `SHA256SUMS.txt` and the release attestation cover it. A wheel present but absent from `SHA256SUMS.txt` is reported too: it would be covered by no digest at all.

**Known vulnerabilities** — is anything in the batch publicly known to be broken?

```bash
python scripts/audit_dependencies.py                       # installed runtime closure
python scripts/audit_dependencies.py --wheels fabric-wheels  # a specific batch
```

Queries [OSV.dev](https://osv.dev), the public database aggregating the GitHub Advisory Database, PyPA advisories and NVD. It audits the installed environment, a folder of wheels (`--wheels`) or the pinned versions of a lockfile (`--requirements`). Exits **1** if a vulnerability is known and **2** if the check could not run (OSV unreachable, nothing to audit) — a check that did not execute is not a check that passed.

The [release workflow](../.github/workflows/release.yml) runs both checks on every batch it builds, and refuses to publish if either fails. A release therefore never carries a wheel whose digest diverges from PyPI, nor a dependency with a publicly known vulnerability, at the moment it is cut.

The `audit` job of [CI](../.github/workflows/ci.yml) audits the **locked versions** (`--requirements requirements/runtime.txt`) on every push and pull request, and **every Monday at 06:00 UTC**. Those are the versions the offline bundles ship — including the two `arro3-core` versions the lockfile carries for different kernels — not the newest ones a fresh `pip install` would pick, which is what this job audited until now and which says nothing about a lakehouse that froze older versions. The weekly run is the one that matters: an offline install freezes its versions, so a CVE published after a release would otherwise surface nowhere. Nothing in a deployed lakehouse tells you its `urllib3` became vulnerable last week.

The job audits the lockfile of `main`, not what a particular lakehouse holds: a notebook still on an old batch is covered only while its versions are still in the lockfile. To audit a deployed folder, run `python scripts/audit_dependencies.py --wheels /lakehouse/default/Files/libs` there.

### What these checks do not prove

Stated plainly, because the gap is real.

- **Neither proves the code is safe.** Nobody has read delta-rs's 50 MB of Rust. What they establish is that you got the artifact the ecosystem believes it published, and that nothing about it is publicly known to be broken.
- **PyPI stopped signing packages in 2023.** A SHA-256 match proves the file equals what PyPI serves; it says nothing about whether what PyPI serves is sound. A compromised maintainer publishing a poisoned release would produce a batch that verifies perfectly — **unless it is published after the lockfile was last updated**: the lockfile does stop a version nobody reviewed from entering a bundle. It does not stop one that was already poisoned when its update was reviewed, and a reviewer sees a version number and a digest, not code.
- **The build tooling is pinned too, with the same limit.** `build` and `hatchling` — the backend that shapes the library's own wheel — are installed from `requirements/build.txt` with hashes, and the wheel is built with `--no-isolation` so nothing else is fetched. Until then the release ran `pip install build` and let `build` download an unpinned `hatchling` into a throwaway environment. The same limit applies as for the runtime lockfile: it stops a version nobody reviewed, not a hostile one that was reviewed. The attestation proves where a file was built, not that the build was honest.
- **An unknown vulnerability stays unknown.** The audit reports the state of public knowledge on the day it runs — hence the weekly schedule rather than a one-off check at release time.

What genuinely limits the blast radius is the offline install: nothing is fetched at runtime, so a compromise of PyPI or GitHub *after* a release cannot reach a running notebook. The trade-off is that versions freeze, which is exactly what the weekly audit exists to watch.

## Secrets

- Secrets are **never stored in the configuration** — only references: `{"env_var": ...}` or `{"keyvault_url": ..., "secret_name": ...}`. Literal strings are for non-sensitive values only, and **validation enforces it**: a literal in `auth.token`, `auth.key`, `auth.password`, `auth.client_secret`, the OAuth 1.0a secrets, a credential-named `token_endpoint` body/header entry or a credential-named top-level header is a `ConfigError` that names the field and never repeats the value. `"allow_literal_secrets": true` lifts the check, for a throwaway test; a source that runs on a schedule should never need it.
- Key Vault resolution uses the notebook identity via `notebookutils.credentials.getSecret` (or `DefaultAzureCredential` outside Fabric). Grant that identity **Get on secrets only**, ideally scoped per secret.
- Resolved secret values are stripped of leading/trailing whitespace, held in memory (the masking registry below keeps them for the life of the process), and sent only to the configured `base_url` and `token_url` — and to a host listed in `pagination.allowed_hosts` if a `next_link` response names it. They travel over TLS: `http://` is refused unless the host is local or `allow_insecure_http` is set.
- Secrets are kept out of logs and error messages: `RunResult.error_message` and the `log_runs` table contain exception types, HTTP status codes, URLs and reference *names* (env var name, secret name), not resolved values. The only text from outside the library that reaches them is what the remote API wrote — see [Error messages from the API in log_runs](#error-messages-from-the-api-in-log_runs) — and the masking described next covers the credentials the library itself resolved.
- **Resolved secrets are masked before a message leaves the library.** Every value the library resolves — from an environment variable, from Key Vault, or a token obtained from an IdP (and the Base64 form of a Basic header) — is remembered for the life of the process. `RunResult.error_message`, `RunResult.warnings` and the `log_runs` row have those values replaced by `<redacted>` (raw and percent-encoded), **before** the message is truncated, so a secret cut in two by the cap is not left half-readable. This is what closes the gap the next section describes: an API that echoes a credential back in an error body, or a secret placed in a URL path, can no longer reach `log_runs` *when the library resolved that credential*. It cannot mask a value it never saw (a literal, which validation now refuses for secret fields, or a credential an API invents), and it ignores values shorter than 6 characters. Whatever the content, the whole message is capped at 2 000 characters.
- **URLs in error messages are stripped of their query string and of any credentials** (`user:password@host`). The configuration refuses a `base_url` or `token_url` that embeds credentials, but `next_link` URLs come from the API, so the stripping stays. `log_runs` is a Delta table readable by everyone with lakehouse access — a wider audience than the configuration file — and a full URL would copy the query params into it. `RunResult.rows_loaded` still shows how far the run got. This covers both halves of the problem: the messages the library writes, and the ones `requests` and urllib3 write for it. A `ConnectionError` is raised before the library regains control and copies the requested URL into its own message (`Max retries exceeded with url: /items?api_key=…`); every `requests` exception is rebuilt with that query redacted, its type preserved so that retry behaviour is unchanged.
- `token_endpoint` refuses secret references in the request body when `method` is `GET` — query parameters end up in server and proxy logs. Use `POST`.

## Values that come back from a response

Four values read from an API response are used to build the next request. They are the only paths by which an API influences what the library does next.

| Value | Where it goes | What bounds it |
|---|---|---|
| **Watermark** (`incremental.field`) | Interpolated into `body`/`params` | Character filter + mandatory `value_format` — see [Request bodies](#request-bodies). |
| **Cursor** (`pagination.cursor_field`) | Sent back as a query param or a JSON value under `params_path` | Never concatenated into a string: it is passed as a discrete parameter value, URL-encoded by `requests` or serialized as JSON, so it cannot alter the structure of the request. A cursor that does not advance, or that is missing while the API announces another page, raises instead of looping or truncating. |
| **Keyset key** (`pagination.key_field`) | Sent back as a query param, or substituted into `body` with `"params_in": "body_template"` | As a query param, the same as a cursor: a discrete, encoded value. Substituted into the body, it is interpolated into a query, so `value_format` is mandatory there — `numeric`, `iso_date` or `iso_datetime` leave no room for syntax. The character filter applies in both cases. |
| **Next-page URL** (`pagination.next_field`) | Fetched directly, with the session's auth headers | Must be on the origin of `base_url` (same scheme, host, port) or on a host of `allowed_hosts`, and never a move from `https` to `http` — see below. |

**`next_link` follows a URL chosen by the API — on the origin of `base_url` only.** The strategy requests whatever URL the response puts in `next_field`, on the same authenticated session. Left unchecked, a compromised or hostile endpoint could point the next page at a host it controls and receive the credentials with the request. The URL is therefore compared with `base_url`: same scheme, host and port, or the run fails **before** the request is sent, with a message naming the host. A legitimate second host (a CDN, a regional endpoint) is declared with `pagination.allowed_hosts`; even then, a move from `https` to `http` is refused. A relative URL carries no host and is left to `requests`, which rejects it. The other strategies (`offset`, `page`, `cursor`, `keyset`, `none`) only build URLs from `base_url`. A `302` is a different path, closed separately — see [Network](#network). Residual risk: a host you list in `allowed_hosts` receives the credentials, so list only hosts that belong to the API provider.

## Error messages from the API in log_runs

Three paths copy text written by the remote API into `log_runs.error_message`, a Delta table readable by everyone with lakehouse access:

- **Application errors in a `200`.** When a source declares an [`errors`](configuration.md#application-errors-in-a-200) block, the message the API returns in a successful HTTP response is persisted verbatim. At most the first 3 errors, 500 characters total. A GraphQL error quotes the failing query and its position; without a cap it would be stored page after page, turning a technical table into a copy of the request. Only the declared `message_field` is read — the rest of the payload, including the data, never reaches the logs.
- **The body of a `4xx`.** A client error is never retried, and the only place the API says *why* it refuses is that body: without it, `error_message` is a status code and a URL, and diagnosing means replaying the request by hand. Only its first 16 KiB are even read; it is stored with its whitespace collapsed and under the same 500-character cap. Unlike the case above, this is the start of the body **as the API wrote it**, whatever the API chose to put in it — an error response commonly quotes the request it rejected, so a filter or an SQL predicate sent in the body can be echoed back into the table. Statuses that *are* retried (`429`, `5xx`) do not carry their body: they are transient, and the status is the diagnosis.
- **The reply of a refused token call.** `oauth2_client_credentials` and `token_endpoint` quote the start of the IdP's answer (whitespace collapsed, 500 characters) when it is not a 200: the status alone cannot tell a misplaced credential from an expired one.

`log_runs.warnings` stores the run's `RunResult.warnings`. They are written by the library, but they name things the API chose: a column in a warning is a key of the JSON the API returned. They go through the same masking and the same 2 000-character cap as `error_message`.

What the library cannot do is judge the *content* of either message: both are written by the remote API. It can, however, recognise the credentials **it resolved itself**: an echoed environment variable, Key Vault secret or issued token is replaced by `<redacted>` before the message is stored (see [Secrets](#secrets)). A credential the library never held — one the API invents, or one typed as a literal with `allow_literal_secrets` — still passes through. Keep treating `log_runs` as readable-by-many (it already is); without the feature, the same failure is simply invisible.

## Request bodies

`incremental.inject: "body_template"` interpolates the watermark into strings of `body` — typically into an SQL `WHERE` clause. Two things bound that:

- **The template comes from the configuration**, which is already trusted at the same level as code. The library never builds a query from a response.
- **The interpolated value comes from a response**, and is therefore untrusted. It is rejected if it contains `'`, `"`, `;`, `--`, `/*`, a backslash, a newline or carriage return, or a NUL byte — so a value cannot close a literal or start a new clause.
- **`value_format` is mandatory** with `body_template`. Character filtering alone only protects a placeholder inside quotes: a bare one (`WHERE id > {last_id}`) accepts `0 OR 1=1`, which contains no forbidden character. Declaring `numeric`, `iso_date` or `iso_datetime` constrains the value to a shape with no room for syntax.

A rejected watermark fails the run before any HTTP call, and the failure lands in `log_runs`.

## Network

- TLS certificate verification is always on. There is deliberately **no option to disable it**.
- **Plain `http://` is refused** for `base_url` and `auth.token_url`: the token and the data would cross the network in clear text. `localhost`, `127.0.0.1`, `[::1]` and `*.localhost` are exempt (a local mock), and `"allow_insecure_http": true` accepts a network you trust. Credentials embedded in the URL (`https://user:password@host`) are refused whatever the scheme. `file://`, `ftp://` and relative URLs are not valid endpoints and are refused too.
- **Auth headers do not follow a redirect off the original origin.** `requests` strips only the literal `Authorization` header when a redirect changes host, which left an `api_key_header` — or any auth carried by a custom `header_name` — being re-sent to wherever a `302` pointed. The session now drops every header it set for authentication as soon as the redirect leaves the original host, or downgrades `https` to `http`. Headers declared in `config['headers']` are untouched: validation requires them to be literal strings and refuses a literal under a credential-looking name (`Authorization`, `X-API-Key`, anything containing `token`, `secret`, `password`…), so nothing sensitive is supposed to be in them.
- Every HTTP call has a timeout (data calls: `timeout_seconds`, default 60 s; token calls: default 30 s).
- Retries are bounded (`max_attempts`, default 3) and only for network errors, HTTP 429/5xx, and application error codes the configuration explicitly lists in `errors.retryable_codes`. Auth failures fail immediately rather than being replayed, so a misconfiguration cannot hammer an IdP — with one bounded exception: a 401 on an auth whose token is renewable (`oauth2_client_credentials`, `token_endpoint`) triggers **one** token renewal and one replay per page, because an expired token is not a misconfiguration. A 401 that survives a fresh token, and every 403, fail on the spot. The retryable codes come from the configuration, not from the response: an API cannot declare its own errors retryable, and even a listed code is capped by `max_attempts`.
- A `Retry-After` header from the server takes precedence over the local backoff, so the library does not retry earlier than it was told to. The delay a server can impose is itself capped (`retry.max_retry_after_seconds`, default 300 s) — a hostile or broken server cannot park a notebook for hours.
- `oauth1` signs each request with HMAC over the method, URL and query params, with a fresh nonce and timestamp per request; credentials are never sent on the wire.

## Resource limits

A remote API is untrusted, and `requests` bounds neither the size of a response nor its total duration — `timeout` is the silence between two bytes. Two limits fill the gap (details in [configuration.md](configuration.md#limits-on-a-run)):

- **`max_response_bytes`** (100 MiB by default) cuts a body off as it streams in, whether or not the server announces a length, and counts decompressed bytes. Before, `response.json()` loaded whatever arrived, and the body of a `4xx` was read whole to quote 500 characters of it.
- **`max_run_seconds`** (opt-in) is checked before every request and after every chunk, so a server dripping one byte every few seconds no longer holds a notebook indefinitely. It does not interrupt a request already blocked in a socket read: a run can overrun by one chunk read (at most `timeout_seconds`) or by one backoff wait (at most `retry.max_retry_after_seconds` when the server asked for it).

Not covered: the token calls (`oauth2_client_credentials`, `token_endpoint`) read their response whole under their own `timeout_seconds`. They go to the IdP named in the configuration, which is trusted at the same level as the rest of it.

## Writes

- Schema and table names from the configuration are validated (`[A-Za-z_][A-Za-z0-9_]*`) before being used in a path — a value like `../../Files/x` is rejected, so a malformed or malicious configuration cannot write outside `Tables/<schema>/`.
- **Writes are append-only by default, and only by default.** `write.mode` is `append` unless the configuration says otherwise. `"mode": "replace_where"` deletes the rows matching its predicate before writing, and `"mode": "overwrite"` replaces the **entire** target table; `"mode": "merge"` rewrites in place every row whose `merge_keys` match what the source returns. All three are deliberate features — a backfill has to be replayable — but they mean the configuration decides whether a run adds to a table or empties it. That is the same trust boundary as the rest of the configuration, restated here because the blast radius is larger than sending a token to the wrong host: read the `Trusted` section above as covering data destruction too. Delta time travel is the recovery path, until a `VACUUM` removes it.
- `replace_where` is an SQL predicate taken **verbatim** from the configuration and handed to delta-rs, like `partition_by`; it has the trust of the configuration, not of a response. `merge_keys` are checked (no backtick) and quoted.
- Every SQL string the library builds around a value — the watermark read and reset, the run lock, the upkeep of the technical tables — quotes that value with single-quote escaping before interpolation into the local delta-rs query engine, or builds it from a fixed-format timestamp. Verified against the engine rather than assumed: a backslash carries no escaping meaning in a DataFusion string literal, so doubling the quote is sufficient there.
- **The technical tables are not a security boundary.** `watermark`, `log_runs` and `run_locks` are ordinary Delta tables: whoever can write the lakehouse can move a watermark (hiding or replaying data), forge a log line or a lock. `single_instance` guards against accidental overlap of honest runs, not against a hostile writer. `compact_technical_tables()` deletes — old `watermark` rows and long-expired `run_locks` rows, never `log_runs` — and is an operator action to run between runs.

## Reporting a vulnerability

Open a [private security advisory](https://github.com/Y0hannH/flume-lib/security/advisories/new) on GitHub. Please do not open public issues for exploitable findings.
