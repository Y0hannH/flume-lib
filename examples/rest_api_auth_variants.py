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
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.10.1

from flume_lib import run_source

API = "https://api.example.com/v1"
KEYVAULT = "https://mykv.vault.azure.net"


def secret(name: str) -> dict:
    return {"keyvault_url": KEYVAULT, "secret_name": name}


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
    # How it is then sent on the data calls, same keys as `bearer_token`.
    "header_name": "Authorization",
    "value_prefix": "Bearer ",
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


# ---------------------------------------------------------------------------
# 6. No auth at all — a public endpoint.
#
# Omitting `auth` and writing `{"type": "none"}` are the same thing. Write it
# when the absence is deliberate: the next reader should not have to wonder
# whether the block was forgotten.
# ---------------------------------------------------------------------------

NO_AUTH = {"type": "none"}


# `oauth1` is the seventh, and does not belong on this list: it signs every
# request individually rather than producing a header once, which is also why
# nothing expires mid-run with it. NetSuite Token-Based Authentication is the
# case you will actually meet — see netsuite_suiteql.py.


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
    "api_key": API_KEY,
    "basic": BASIC,
    "login": LOGIN,
    "client_credentials": CLIENT_CREDENTIALS,
    "entra": ENTRA_SERVICE_PRINCIPAL,
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
