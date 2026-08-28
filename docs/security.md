# Security

Threat model and security posture of flume-lib. Audience: anyone deploying the library in a client workspace, and contributors reviewing changes.

## Threat model

**Trusted:**

- The **source configuration** (`sources.json` or equivalent). It decides where tokens are sent (`base_url`, `token_url`) and where data is written. **Anyone who can edit the configuration can redirect an API token to a server they control.** Store it with the same access restrictions as code: workspace write access should be limited, and changes to `Files/conf/` should be deliberate acts.
- The Fabric runtime (`notebookutils`), the notebook identity, and the lakehouse it is attached to.
- The wheels uploaded to `Files/libs/<version>/` (see [Supply chain](#supply-chain)).

**Untrusted:**

- Remote API responses (data, headers, pagination fields, application errors). They are parsed defensively: pagination stops are bounded by explicit conditions, record structures are serialized, and nothing from a response is ever executed or used to build a filesystem path. Three things do travel back out — see [Values that come back from a response](#values-that-come-back-from-a-response).
- GitHub and PyPI **at runtime** — by design they are not on the execution path when using the recommended install.

## Supply chain

- **Recommended install is fully offline**: wheels (library + all dependencies, resolved at release time) are uploaded to the lakehouse and installed with `pip --no-index`. Nothing is downloaded at runtime; a compromise of GitHub or PyPI after the release cannot affect running notebooks.
- **The wheels folder is the trust boundary**: whoever can write to it decides what runs in every notebook installing from it. Restrict write access to `Files/libs/` the same way you would restrict access to code, and pin the version (`flume-lib==X.Y.Z`) so an added batch cannot silently upgrade notebooks. `SHA256SUMS.txt` ships with each batch for verification.
- **Git installs must pin a full commit SHA**, never a tag: tags are mutable, SHAs are not.
- **Repository hardening**: rulesets forbid force-pushes and deletion on `main`, and forbid update/deletion of `v*` tags. Modifying a ruleset requires web-authenticated admin access (2FA), not just a git credential.

### Verifying a batch

Two scripts, two different questions. Both are standard-library only, so either can be pasted into a Fabric notebook cell.

**Integrity** — is this the file it claims to be?

```bash
python scripts/verify_wheels.py                    # fabric-wheels/, both checks
python scripts/verify_wheels.py --offline          # local digests only, no network
python scripts/verify_wheels.py /path/to/libs      # another folder
```

It runs two independent checks. Every wheel matches `SHA256SUMS.txt` — which catches a file truncated, corrupted or missing after a transfer, and needs no network, so it is the one to run on the lakehouse side after upload. And every wheel matches the digest **PyPI publishes** for that filename and version — which catches an altered mirror or a file substituted between PyPI and the batch. A wheel present but absent from `SHA256SUMS.txt` is reported too: it would be covered by no digest at all.

**Known vulnerabilities** — is anything in the batch publicly known to be broken?

```bash
python scripts/audit_dependencies.py                       # installed runtime closure
python scripts/audit_dependencies.py --wheels fabric-wheels  # a specific batch
```

Queries [OSV.dev](https://osv.dev), the public database aggregating the GitHub Advisory Database, PyPA advisories and NVD. Exits non-zero if a vulnerability is known, **and also if the check could not run** — a check that did not execute is not a check that passed.

The [release workflow](../.github/workflows/release.yml) runs both checks on every batch it builds, and refuses to publish if either fails. A release therefore never carries a wheel whose digest diverges from PyPI, nor a dependency with a publicly known vulnerability, at the moment it is cut.

The `audit` job of [CI](../.github/workflows/ci.yml) runs the vulnerability check on every push and pull request, and **every Monday morning**. The weekly run is the one that matters: an offline install freezes its versions, so a CVE published after a release would otherwise surface nowhere. Nothing in a deployed lakehouse tells you its `urllib3` became vulnerable last week.

### What these checks do not prove

Stated plainly, because the gap is real.

- **Neither proves the code is safe.** Nobody has read delta-rs's 50 MB of Rust. What they establish is that you got the artifact the ecosystem believes it published, and that nothing about it is publicly known to be broken.
- **PyPI stopped signing packages in 2023.** A SHA-256 match proves the file equals what PyPI serves; it says nothing about whether what PyPI serves is sound. A compromised maintainer publishing a poisoned release would produce a batch that verifies perfectly.
- **An unknown vulnerability stays unknown.** The audit reports the state of public knowledge on the day it runs — hence the weekly schedule rather than a one-off check at release time.

What genuinely limits the blast radius is the offline install: nothing is fetched at runtime, so a compromise of PyPI or GitHub *after* a release cannot reach a running notebook. The trade-off is that versions freeze, which is exactly what the weekly audit exists to watch.

## Secrets

- Secrets are **never stored in the configuration** — only references: `{"env_var": ...}` or `{"keyvault_url": ..., "secret_name": ...}`. Literal strings are for non-sensitive values only.
- Key Vault resolution uses the notebook identity via `notebookutils.credentials.getSecret` (or `DefaultAzureCredential` outside Fabric). Grant that identity **Get on secrets only**, ideally scoped per secret.
- Resolved secret values are stripped of leading/trailing whitespace, held in memory for the duration of the run, and sent only to the configured `base_url`/`token_url` over TLS.
- Secrets never appear in logs or error messages: `RunResult.error_message` and the `log_runs` table contain exception types, HTTP status codes, URLs and reference *names* (env var name, secret name) — never resolved values. The one piece of response *content* that reaches them is the body of a failing response, truncated — see [Error messages from the API in log_runs](#error-messages-from-the-api-in-log_runs).
- **URLs in error messages are stripped of their query string.** `log_runs` is a Delta table readable by everyone with lakehouse access — a wider audience than the configuration file — and a full URL would copy the query params into it. `RunResult.rows_loaded` still shows how far the run got. This covers both halves of the problem: the messages the library writes, and the ones `requests` and urllib3 write for it. A `ConnectionError` is raised before the library regains control and copies the requested URL into its own message (`Max retries exceeded with url: /items?api_key=…`); every `requests` exception is rebuilt with that query redacted, its type preserved so that retry behaviour is unchanged.
- `token_endpoint` refuses secret references in the request body when `method` is `GET` — query parameters end up in server and proxy logs. Use `POST`.

## Values that come back from a response

Four values read from an API response are used to build the next request. They are the only paths by which an API influences what the library does next.

| Value | Where it goes | What bounds it |
|---|---|---|
| **Watermark** (`incremental.field`) | Interpolated into `body`/`params` | Character filter + mandatory `value_format` — see [Request bodies](#request-bodies). |
| **Cursor** (`pagination.cursor_field`) | Sent back as a query param or a JSON value under `params_path` | Never concatenated into a string: it is passed as a discrete parameter value, URL-encoded by `requests` or serialized as JSON, so it cannot alter the structure of the request. A cursor that does not advance, or that is missing while the API announces another page, raises instead of looping or truncating. |
| **Keyset key** (`pagination.key_field`) | Sent back as a query param, or substituted into `body` with `"params_in": "body_template"` | As a query param, the same as a cursor: a discrete, encoded value. Substituted into the body, it is interpolated into a query, so `value_format` is mandatory there — `numeric`, `iso_date` or `iso_datetime` leave no room for syntax. The character filter applies in both cases. |
| **Next-page URL** (`pagination.next_field`) | Fetched directly, with the session's auth headers | ⚠️ **Only the API's own honesty.** See below. |

**`next_link` follows a URL chosen by the API.** The `next_link` strategy requests whatever URL the response puts in `next_field`, on the same authenticated session — so a compromised or hostile endpoint can point the next page at a host it controls and receive the `Authorization` header with it. There is currently no host allowlist. This is inherent to the strategy, not new, and it does not apply to `offset`, `page`, `cursor`, `keyset` or `none`, which only ever build URLs from `base_url`. Note that building the URL is not the same as choosing the destination: any server can answer with a `302`. That path is closed separately — see [Network](#network) — but `next_link` is a direct request to a URL the API named, not a redirect, so nothing there covers it. If the API is not fully trusted, prefer a strategy that keeps the URL under your control.

## Error messages from the API in log_runs

Two paths copy text written by the remote API into `log_runs.error_message`, a Delta table readable by everyone with lakehouse access:

- **Application errors in a `200`.** When a source declares an [`errors`](configuration.md#application-errors-in-a-200) block, the message the API returns in a successful HTTP response is persisted verbatim. At most the first 3 errors, 500 characters total. A GraphQL error quotes the failing query and its position; without a cap it would be stored page after page, turning a technical table into a copy of the request. Only the declared `message_field` is read — the rest of the payload, including the data, never reaches the logs.
- **The body of a `4xx`.** A client error is never retried, and the only place the API says *why* it refuses is that body: without it, `error_message` is a status code and a URL, and diagnosing means replaying the request by hand. It is stored with its whitespace collapsed and under the same 500-character cap. Unlike the case above, this is the **whole** body, whatever the API chose to put in it — an error response commonly quotes the request it rejected, so a filter or an SQL predicate sent in the body can be echoed back into the table. Statuses that *are* retried (`429`, `5xx`) do not carry their body: they are transient, and the status is the diagnosis.

What the library cannot do is judge the *content* of either message: both are written by the remote API. An API that echoed a credential back in an error would put it in `log_runs`. That is a reason to treat `log_runs` as readable-by-many (it already is) rather than to distrust the feature — without it, the same failure is simply invisible.

## Request bodies

`incremental.inject: "body_template"` interpolates the watermark into strings of `body` — typically into an SQL `WHERE` clause. Two things bound that:

- **The template comes from the configuration**, which is already trusted at the same level as code. The library never builds a query from a response.
- **The interpolated value comes from a response**, and is therefore untrusted. It is rejected if it contains `'`, `"`, `;`, `--`, `/*`, a backslash, a newline or a NUL byte — so a value cannot close a literal or start a new clause.
- **`value_format` is mandatory** with `body_template`. Character filtering alone only protects a placeholder inside quotes: a bare one (`WHERE id > {last_id}`) accepts `0 OR 1=1`, which contains no forbidden character. Declaring `numeric`, `iso_date` or `iso_datetime` constrains the value to a shape with no room for syntax.

A rejected watermark fails the run before any HTTP call, and the failure lands in `log_runs`.

## Network

- TLS certificate verification is always on. There is deliberately **no option to disable it**.
- **Auth headers do not follow a redirect off the original origin.** `requests` strips only the literal `Authorization` header when a redirect changes host, which left an `api_key_header` — or any auth carried by a custom `header_name` — being re-sent to wherever a `302` pointed. The session now drops every header it set for authentication as soon as the redirect leaves the original host, or downgrades `https` to `http`. Headers declared in `config['headers']` are untouched: validation already requires them to be literal, non-sensitive strings.
- Every HTTP call has a timeout (data calls: `timeout_seconds`, default 60 s; token calls: default 30 s).
- Retries are bounded (`max_attempts`, default 3) and only for network errors, HTTP 429/5xx, and application error codes the configuration explicitly lists in `errors.retryable_codes`. Auth failures fail immediately rather than being replayed, so a misconfiguration cannot hammer an IdP — with one bounded exception: a 401 on an auth whose token is renewable (`oauth2_client_credentials`, `token_endpoint`) triggers **one** token renewal and one replay per page, because an expired token is not a misconfiguration. A 401 that survives a fresh token, and every 403, fail on the spot. The retryable codes come from the configuration, not from the response: an API cannot declare its own errors retryable, and even a listed code is capped by `max_attempts`.
- A `Retry-After` header from the server takes precedence over the local backoff, so the library does not retry earlier than it was told to. The delay a server can impose is itself capped (`retry.max_retry_after_seconds`, default 300 s) — a hostile or broken server cannot park a notebook for hours.
- `oauth1` signs each request with HMAC over the method, URL and query params, with a fresh nonce and timestamp per request; credentials are never sent on the wire.

## Writes

- Schema and table names from the configuration are validated (`[A-Za-z_][A-Za-z0-9_]*`) before being used in a path — a value like `../../Files/x` is rejected, so a malformed or malicious configuration cannot write outside `Tables/<schema>/`.
- **Writes are append-only by default, and only by default.** `write.mode` is `append` unless the configuration says otherwise. `"mode": "replace_where"` deletes the rows matching its predicate before writing, and `"mode": "overwrite"` replaces the **entire** target table. Both are deliberate features — a backfill has to be replayable — but they mean the configuration decides whether a run adds to a table or empties it. That is the same trust boundary as the rest of the configuration, restated here because the blast radius is larger than sending a token to the wrong host: read the `Trusted` section above as covering data destruction too. Delta time travel is the recovery path, until a `VACUUM` removes it.
- Watermark reads quote SQL string literals (single-quote escaping) before interpolation into the local delta-rs query engine. Verified against the engine rather than assumed: a backslash carries no escaping meaning in a DataFusion string literal, so doubling the quote is sufficient there.

## Reporting a vulnerability

Open a [private security advisory](https://github.com/Y0hannH/flume-lib/security/advisories/new) on GitHub. Please do not open public issues for exploitable findings.
