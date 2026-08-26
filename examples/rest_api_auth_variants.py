# Fabric Python notebook (non-Spark) — the auth shapes, side by side.
#
# Same fictional API as rest_api_paginated.py, read six different ways. Only
# the `auth` block changes; nothing else in a source config depends on how the
# credential is obtained. Copy the one that matches the client, delete the rest.
#
# The rule that holds across all of them: a credential is a **reference**
# resolved at run time, never a literal in the config. `Files/conf/` is
# readable by everyone with lakehouse access — a token pasted there is a token
# published.
#
#   {"keyvault_url": "https://mykv.vault.azure.net", "secret_name": "…"}
#   {"env_var": "SOME_TOKEN"}
#
# A literal string is accepted only for values that are not secret: a public
# username, a scope, a grant type.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.15.0

from flume_lib import run_source

API = "https://api.example.com/v1"
KEYVAULT = "https://mykv.vault.azure.net"


def secret(name: str) -> dict:
    return {"keyvault_url": KEYVAULT, "secret_name": name}


# ---------------------------------------------------------------------------
# 0. Where a credential can come from
#
# Every credential field below accepts the same three forms, and they can be
# mixed inside one auth block:
#
#   secret("name")                       Key Vault, resolved at run time
#   {"env_var": "EXAMPLE_API_TOKEN"}     an environment variable
#   "not-a-secret"                       a literal — public values only
#
# Key Vault is the form to use in Fabric: `notebookutils` resolves it with the
# notebook's own identity, nothing is stored in the config, and rotating the
# secret changes nothing here. An env var is the practical form for a local run
# or a CI job, where no vault is reachable.
#
# Each type also accepts a **historical** spelling — `token_env_var`,
# `key_env_var`, `username_env_var`, `client_secret_env_var`, and so on — which
# names an environment variable directly. It is equivalent to the `env_var`
# reference and still supported, so configs written against older versions keep
# working; prefer the reference form in anything new, because it is the only
# one that can also point at a vault.
# ---------------------------------------------------------------------------

BEARER_FROM_ENV = {
    "type": "bearer_token",
    "token": {"env_var": "EXAMPLE_API_TOKEN"},
}

BEARER_FROM_ENV_LEGACY = {
    "type": "bearer_token",
    # Exactly the same thing as above, in the older spelling.
    "token_env_var": "EXAMPLE_API_TOKEN",
}


# ---------------------------------------------------------------------------
# 1. `bearer_token` — a static token in a header.
#
# `Authorization: Bearer <token>` by default. Both halves are overridable, which
# covers most of the APIs that invented their own spelling: GitHub-style
# `Authorization: token <t>`, or a vendor header entirely of their own.
# ---------------------------------------------------------------------------

BEARER = {
    "type": "bearer_token",
    "token": secret("example-api-token"),
}

BEARER_CUSTOM = {
    "type": "bearer_token",
    "token": secret("example-api-token"),
    "header_name": "X-Auth-Token",
    # Trailing space included when there is a prefix — the value is a plain
    # concatenation. `""` sends the bare token.
    "value_prefix": "",
}


# ---------------------------------------------------------------------------
# 2. `api_key_header` — a key in a named header.
#
# The same thing as a bare `bearer_token`, kept separate because that is how
# the APIs describe it. Azure API Management is the canonical case.
# ---------------------------------------------------------------------------

API_KEY = {
    "type": "api_key_header",
    "header_name": "Ocp-Apim-Subscription-Key",
    "key": secret("apim-subscription-key"),
}

API_KEY_FROM_ENV = {
    "type": "api_key_header",
    "header_name": "Ocp-Apim-Subscription-Key",
    "key_env_var": "APIM_SUBSCRIPTION_KEY",
}

# An API key expected in the *query string* has no auth type: it is not a
# header, and `params` would carry it into `base_url`'s query — where every
# proxy and server log on the way records it. Ask the vendor for the header
# form. If there is genuinely none, the key belongs in an env var read by the
# notebook and injected into `params` there, with the config file kept clean:
#
#   config["params"] = {**config["params"], "api_key": os.environ["EXAMPLE_KEY"]}


# ---------------------------------------------------------------------------
# 3. `basic` — HTTP Basic.
#
# Base64 of `user:password` in the `Authorization` header. Base64 is encoding,
# not encryption: this is only ever acceptable over HTTPS.
#
# The username is often not a secret (an integration account name); it may be a
# literal. The password never is.
# ---------------------------------------------------------------------------

BASIC = {
    "type": "basic",
    "username": "svc_fabric_ingest",
    "password": secret("example-api-password"),
}

# Both halves from the environment. Useful when the same notebook runs locally
# against a sandbox and in Fabric against production: only the resolution
# changes, the rest of the config is identical.
BASIC_FROM_ENV = {
    "type": "basic",
    "username_env_var": "EXAMPLE_API_USER",
    "password_env_var": "EXAMPLE_API_PASSWORD",
}


# ---------------------------------------------------------------------------
# 4. `token_endpoint` — the vendor's own login call.
#
# The catch-all for every API that says "POST your credentials here, get a
# token back". The login request is described field by field, and the token is
# extracted from the response by dotted path.
#
# Renewed mid-run. An API handing out 30-minute tokens no longer breaks a
# two-hour backfill: on a 401 the library logs in again and replays the page,
# once per page. If the response carries the token lifetime, point
# `expires_in_json_path` at it and the renewal happens before expiry rather
# than after a refused call.
# ---------------------------------------------------------------------------

LOGIN = {
    "type": "token_endpoint",
    "token_url": f"{API}/auth/login",
    "method": "POST",
    # Values may be literals or secret references, mixed freely.
    "body": {
        "username": "svc_fabric_ingest",
        "password": secret("example-api-password"),
        "grant_type": "password",
    },
    # `json` (default) posts a JSON object; `form` sends
    # application/x-www-form-urlencoded, which about half of these endpoints
    # want instead.
    "body_format": "json",
    # Where the token sits in the response — `access_token` by default.
    "token_json_path": "data.access_token",
    # The lifetime, if the response carries one. With it, the token is renewed
    # a little before it expires; without it, renewal only happens reactively,
    # after a call has already come back 401. Both work — this one wastes no
    # call. The value is read as seconds, the shape almost every API uses.
    "expires_in_json_path": "data.expires_in",
    # How it is then sent on the data calls, same keys as `bearer_token`.
    "header_name": "Authorization",
    "value_prefix": "Bearer ",
    # Timeout of the *login* call only, independent of the data calls'
    # `timeout_seconds`. A login endpoint that hangs would otherwise block the
    # run for the full data timeout before anything is even fetched.
    "timeout_seconds": 30,
}

# A login declared as GET with a secret reference in `body` is rejected by
# validation: those values would go out as query params, into the server logs
# and every proxy in between. It is a POST or it is nothing.


# ---------------------------------------------------------------------------
# 5. `oauth2_client_credentials` — the standard machine-to-machine flow.
#
# Two forms of the same thing. `token_url` for any IdP (Auth0, Okta, Keycloak,
# a vendor's own); `tenant_id` as a shortcut for Entra ID, which builds
# `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token` for you — the
# form to use for any Microsoft API (Graph, Fabric, Azure Management), see
# microsoft_graph_odata.py.
#
# Renewed mid-run like `token_endpoint`, and better: the OAuth2 `expires_in`
# is standard, so the token is refreshed before it expires rather than after a
# 401.
# ---------------------------------------------------------------------------

CLIENT_CREDENTIALS = {
    "type": "oauth2_client_credentials",
    "token_url": "https://idp.example.com/oauth2/token",
    "client_id": secret("example-client-id"),
    "client_secret": secret("example-client-secret"),
    "scope": "read:invoices read:customers",
}

ENTRA_SERVICE_PRINCIPAL = {
    "type": "oauth2_client_credentials",
    "tenant_id": "00000000-0000-0000-0000-000000000000",
    "client_id": secret("sp-client-id"),
    "client_secret": secret("sp-client-secret"),
    "scope": "https://graph.microsoft.com/.default",
}

# The same flow, with the credentials in an `Authorization: Basic` header
# instead of the form body. RFC 6749 allows both and recommends this one, so a
# vendor requiring it is not an edge case: Cisco Umbrella and Secure Access
# both do. The tell is in the vendor's own example — `curl --user
# '<key>:<secret>'` means `basic`, `-d 'client_id=...'` means the default.
#
# Send it the wrong way and the credentials simply are not there, so the token
# call answers 401 — the same 401 as an expired secret, on credentials that are
# perfectly valid. That is why the error now carries the response body: the
# vendor names the reason, the status code alone never does.
UMBRELLA = {
    "type": "oauth2_client_credentials",
    "token_url": "https://api.umbrella.com/auth/v2/token",
    "client_auth": "basic",
    "client_id": secret("umbrella-api-key"),
    "client_secret": secret("umbrella-api-secret"),
    # No `scope`: Umbrella derives the permissions from the key itself.
    # `expires_in: 3600` comes back on its own, so the token is renewed at
    # T-60s and a backfill longer than an hour survives.
}

# The same flow with both credentials from the environment, and a timeout on
# the token call. The client id is rarely secret — a literal is acceptable for
# it; the client secret never is, whichever form it takes.
CLIENT_CREDENTIALS_FROM_ENV = {
    "type": "oauth2_client_credentials",
    "token_url": "https://idp.example.com/oauth2/token",
    "client_id_env_var": "EXAMPLE_CLIENT_ID",
    "client_secret_env_var": "EXAMPLE_CLIENT_SECRET",
    "scope": "read:invoices",
    "timeout_seconds": 30,
}


# ---------------------------------------------------------------------------
# 6. No auth at all — a public endpoint.
#
# Omitting `auth` and writing `{"type": "none"}` are the same thing. Write it
# when the absence is deliberate: the next reader should not have to wonder
# whether the block was forgotten.
# ---------------------------------------------------------------------------

NO_AUTH = {"type": "none"}


# ---------------------------------------------------------------------------
# 7. `oauth1` — request signing (RFC 5849)
#
# The odd one out: it produces no reusable header. Every request is signed
# individually over its URL, its query params and a nonce, so the signature is
# recomputed page after page — which is also why nothing expires mid-run with
# it, and why a long backfill is easier here than with any OAuth 2.0 flow.
#
# Four credentials in the three-legged form below. The token pair is optional:
# omitting both gives the "two-legged" variant, where the consumer credentials
# stand alone. They go together — one without the other is rejected at
# validation rather than producing an unexplained 401.
#
# `signature_method` defaults to HMAC-SHA256; older APIs still require
# HMAC-SHA1, and `realm`, when the API asks for one, is sent in the header
# outside the signature. A worked source, end to end: sql_over_rest_api.py.
# ---------------------------------------------------------------------------

SIGNED = {
    "type": "oauth1",
    "realm": "1234567",
    "signature_method": "HMAC-SHA256",
    "consumer_key": secret("api-consumer-key"),
    "consumer_secret": secret("api-consumer-secret"),
    "token": secret("api-token-id"),
    "token_secret": secret("api-token-secret"),
}

# Same, from the environment, on an API still on SHA-1.
SIGNED_LEGACY = {
    "type": "oauth1",
    "signature_method": "HMAC-SHA1",
    "consumer_key_env_var": "EXAMPLE_CONSUMER_KEY",
    "consumer_secret_env_var": "EXAMPLE_CONSUMER_SECRET",
    "token_env_var": "EXAMPLE_TOKEN_ID",
    "token_secret_env_var": "EXAMPLE_TOKEN_SECRET",
}

# Two-legged: no token pair at all.
SIGNED_TWO_LEGGED = {
    "type": "oauth1",
    "consumer_key": secret("api-consumer-key"),
    "consumer_secret": secret("api-consumer-secret"),
}

# One caveat, and it costs an afternoon when it bites: the signature covers the
# request URL, and `requests` replays the original `Authorization` header on a
# same-host redirect instead of re-signing. The API then sees a signature for
# the wrong URL and answers 401. Point `base_url` at the final URL.


# ---------------------------------------------------------------------------
# Checking credentials without writing anything.
#
# Rotating a secret, onboarding a client, debugging a 401: a dry run validates
# the config, performs the login (so a wrong client secret fails here) and
# really calls the data endpoint, but writes neither data nor watermark nor
# log_runs. The cheapest possible smoke test.
# ---------------------------------------------------------------------------

AUTH_VARIANTS = {
    "bearer": BEARER,
    "bearer_custom": BEARER_CUSTOM,
    "bearer_env": BEARER_FROM_ENV,
    "api_key": API_KEY,
    "basic": BASIC,
    "login": LOGIN,
    "client_credentials": CLIENT_CREDENTIALS,
    "entra": ENTRA_SERVICE_PRINCIPAL,
    "signed": SIGNED,
    "none": NO_AUTH,
}


def probe(auth: dict, name: str = "auth_probe"):
    """One call against a cheap endpoint, nothing written."""
    config = {
        "name": name,
        "base_url": f"{API}/customers",
        "auth": auth,
        "params": {"limit": 1},
        "pagination": {"type": "none", "items_field": "data"},
        # Required by validation even in dry run — nothing is written to them.
        "target_schema": "bronze",
        "target_table": "customers",
    }
    result = run_source(config, dry_run=True)
    status = "ok" if result.status == "success" else result.error_message
    print(f"{name}: {status}")
    return result


def probe_all():
    for name, auth in AUTH_VARIANTS.items():
        probe(auth, name=f"probe_{name}")


# A failing probe reads as a `failed` RunResult, not an exception — run_source
# never raises. The message names the layer that broke:
#
#   AuthError: ...                 the credential itself (Key Vault, login call)
#   HTTPError: HTTP 401 sur ...    the credential was obtained and refused
#   HTTPError: HTTP 403 sur ...    accepted, but the scope/role is short
#   ConfigError: ...               the config never reached the network
#
# Query strings are stripped from those messages before they reach `log_runs`:
# the table is readable by the whole lakehouse, and a config that put a token in
# the URL would otherwise republish it there.
#
# probe_all()
