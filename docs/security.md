# Security

Threat model and security posture of flume-lib. Audience: anyone deploying the library in a client workspace, and contributors reviewing changes.

## Threat model

**Trusted:**

- The **source configuration** (`sources.json` or equivalent). It decides where tokens are sent (`base_url`, `token_url`) and where data is written. **Anyone who can edit the configuration can redirect an API token to a server they control.** Store it with the same access restrictions as code: workspace write access should be limited, and changes to `Files/conf/` should be deliberate acts.
- The Fabric runtime (`notebookutils`), the notebook identity, and the lakehouse it is attached to.
- The wheels uploaded to `Files/libs/<version>/` (see [Supply chain](#supply-chain)).

**Untrusted:**

- Remote API responses (data, headers, pagination fields). They are parsed defensively: pagination stops are bounded by explicit conditions, record structures are serialized, and nothing from a response is ever executed or used to build a filesystem path. The one value that travels back out to the API is the watermark, and it is validated before it does (see [Request bodies](#request-bodies)).
- GitHub and PyPI **at runtime** — by design they are not on the execution path when using the recommended install.

## Supply chain

- **Recommended install is fully offline**: wheels (library + all dependencies, resolved at release time) are uploaded to the lakehouse and installed with `pip --no-index`. Nothing is downloaded at runtime; a compromise of GitHub or PyPI after the release cannot affect running notebooks.
- **The wheels folder is the trust boundary**: whoever can write to it decides what runs in every notebook installing from it. Restrict write access to `Files/libs/` the same way you would restrict access to code, and pin the version (`flume-lib==X.Y.Z`) so an added batch cannot silently upgrade notebooks. `SHA256SUMS.txt` ships with each batch for verification.
- **Git installs must pin a full commit SHA**, never a tag: tags are mutable, SHAs are not.
- **Repository hardening**: rulesets forbid force-pushes and deletion on `main`, and forbid update/deletion of `v*` tags. Modifying a ruleset requires web-authenticated admin access (2FA), not just a git credential.

## Secrets

- Secrets are **never stored in the configuration** — only references: `{"env_var": ...}` or `{"keyvault_url": ..., "secret_name": ...}`. Literal strings are for non-sensitive values only.
- Key Vault resolution uses the notebook identity via `notebookutils.credentials.getSecret` (or `DefaultAzureCredential` outside Fabric). Grant that identity **Get on secrets only**, ideally scoped per secret.
- Resolved secret values are stripped of leading/trailing whitespace, held in memory for the duration of the run, and sent only to the configured `base_url`/`token_url` over TLS.
- Secrets never appear in logs or error messages: `RunResult.error_message` and the `log_runs` table contain exception types, HTTP status codes, URLs and reference *names* (env var name, secret name) — never resolved values. Response bodies are not logged.
- **URLs in error messages are stripped of their query string.** `log_runs` is a Delta table readable by everyone with lakehouse access — a wider audience than the configuration file — and a full URL would copy the query params into it. `RunResult.rows_loaded` still shows how far the run got.
- `token_endpoint` refuses secret references in the request body when `method` is `GET` — query parameters end up in server and proxy logs. Use `POST`.

## Request bodies

`incremental.inject: "body_template"` interpolates the watermark into strings of `body` — typically into an SQL `WHERE` clause. Two things bound that:

- **The template comes from the configuration**, which is already trusted at the same level as code. The library never builds a query from a response.
- **The interpolated value comes from a response**, and is therefore untrusted. It is rejected if it contains `'`, `"`, `;`, `--`, `/*`, a backslash, a newline or a NUL byte — so a value cannot close a literal or start a new clause.
- **`value_format` is mandatory** with `body_template`. Character filtering alone only protects a placeholder inside quotes: a bare one (`WHERE id > {last_id}`) accepts `0 OR 1=1`, which contains no forbidden character. Declaring `numeric`, `iso_date` or `iso_datetime` constrains the value to a shape with no room for syntax.

A rejected watermark fails the run before any HTTP call, and the failure lands in `log_runs`.

## Network

- TLS certificate verification is always on. There is deliberately **no option to disable it**.
- Every HTTP call has a timeout (data calls: `timeout_seconds`, default 60 s; token calls: default 30 s).
- Retries are bounded (`max_attempts`, default 3) and only for network errors, HTTP 429 and 5xx. Auth failures (401/403) fail immediately and are never retried, so a misconfiguration cannot hammer an IdP.
- A `Retry-After` header from the server takes precedence over the local backoff, so the library does not retry earlier than it was told to. The delay a server can impose is itself capped (`retry.max_retry_after_seconds`, default 300 s) — a hostile or broken server cannot park a notebook for hours.
- `oauth1` signs each request with HMAC over the method, URL and query params, with a fresh nonce and timestamp per request; credentials are never sent on the wire.

## Writes

- Schema and table names from the configuration are validated (`[A-Za-z_][A-Za-z0-9_]*`) before being used in a path — a value like `../../Files/x` is rejected, so a malformed or malicious configuration cannot write outside `Tables/<schema>/`.
- Writes are append-only Delta commits; the library never deletes or overwrites tables.
- Watermark reads quote SQL string literals (single-quote escaping) before interpolation into the local delta-rs query engine.

## Reporting a vulnerability

Open a [private security advisory](https://github.com/Y0hannH/flume-lib/security/advisories/new) on GitHub. Please do not open public issues for exploitable findings.
