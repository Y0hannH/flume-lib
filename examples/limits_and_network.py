# Fabric Python notebook (non-Spark) — what keeps a run from hurting the API,
# from being hurt by it, and from reaching it at all.
#
# Everything below is a top-level key of the source configuration (or of its
# `retry` block). None is required; the defaults are the right ones for a
# well-behaved API reached directly.
#
#   1. limits on a run         max_response_bytes, max_run_seconds
#   2. one run at a time       single_instance, lock_ttl_seconds
#   3. rate and retries        max_requests_per_second, retry.jitter
#   4. a corporate proxy       proxy, ca_bundle
#   5. two escape hatches      allow_insecure_http, allow_literal_secrets
#   6. two request shapes      allow_body_on_get, fail_on_empty
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.20.0

from flume_lib import ConfigError, validate_config

KEYVAULT = "https://mykv.vault.azure.net"

BASE = {
    "base_url": "https://api.example.com/v1/events",
    "auth": {
        "type": "bearer_token",
        "token": {"keyvault_url": KEYVAULT, "secret_name": "api-token"},
    },
    "pagination": {"type": "offset", "limit": 500, "items_field": "events"},
    "target_schema": "bronze",
}


# ---------------------------------------------------------------------------
# 1. Limits on a run
#
# `timeout_seconds` bounds the silence *between two bytes*, not the total: a
# server that sends one byte every 50 seconds keeps a request open for as long
# as it likes. These two close that gap.
#
#   max_response_bytes   one data response, streamed: a Content-Length above it
#                        is refused unread, a body without one is cut off when
#                        it passes the cap. Default 100 MiB — it only ever trips
#                        on an API that does not paginate.
#   max_run_seconds      wall clock for the whole run, checked before every
#                        request (retries included) and after every chunk. Rows
#                        already written stay, and with `checkpoint` the next run
#                        resumes.
# ---------------------------------------------------------------------------

BOUNDED = {
    **BASE,
    "name": "events_bounded",
    "target_table": "events",
    "max_response_bytes": 20 * 1024 * 1024,
    "max_run_seconds": 2 * 3600,
}


# ---------------------------------------------------------------------------
# 2. One run at a time
#
# Two overlapping runs of one source duplicate its window. `single_instance`
# takes an advisory lock (a row in `flume.run_locks`): the second run fails at
# once with `RunLockedError`, before it reads the watermark.
#
# The lock expires after `lock_ttl_seconds` (default 6 h) so that a run that died
# does not block the source forever. Set it longer than your longest run — and
# validation requires it to exceed `max_run_seconds`, which is why the two sit
# together. Delta has no conditional create, so the lock is taken in two steps:
# two simultaneous runs cannot both pass, but both may be refused.
# ---------------------------------------------------------------------------

EXCLUSIVE = {
    **BOUNDED,
    "name": "events_exclusive",
    "single_instance": True,
    "lock_ttl_seconds": 3 * 3600,
}


# ---------------------------------------------------------------------------
# 3. Rate and retries
#
# The 429 is handled *after* the limit has been crossed, and on an API with
# strict governance it is the crossing that gets a client banned.
# `max_requests_per_second` spaces the requests instead: at most that many start
# per second, the first at once, replays included, no burst after a pause.
# Set it below the API's documented quota. It is per run — sources running in
# parallel under `run_sources` each have their own.
#
# The exponential backoff is dispersed (`retry.jitter`, on by default): between
# half the delay and all of it, so sources that meet a 429 together do not come
# back together. A server's `Retry-After` is never dispersed. `false` gives the
# exact delay.
# ---------------------------------------------------------------------------

POLITE = {
    **BASE,
    "name": "events_polite",
    "target_table": "events_polite",
    "max_requests_per_second": 2,
    "retry": {
        "max_attempts": 5,
        "backoff_multiplier": 2,
        "max_retry_after_seconds": 120,
        "jitter": True,
    },
}


# ---------------------------------------------------------------------------
# 4. Behind a corporate proxy
#
# `proxy` is applied to the data calls *and* to the token calls (the IdP sits
# behind the same proxy). A proxy that needs a login carries it in the URL, and
# a password does not belong in the configuration: a literal URL with
# `user:password@` is refused, so the whole URL is given as a secret reference.
#
# A proxy that inspects TLS re-issues every certificate with its own authority.
# `ca_bundle` is the PEM file of that authority, typically uploaded to the
# lakehouse Files. There is no option to turn certificate verification off.
# ---------------------------------------------------------------------------

BEHIND_A_PROXY = {
    **BASE,
    "name": "events_proxied",
    "target_table": "events_proxied",
    "proxy": {"keyvault_url": KEYVAULT, "secret_name": "corp-proxy-url"},
    "ca_bundle": "/lakehouse/default/Files/certs/corp-ca.pem",
}

# the same without a login: a literal URL is fine
BEHIND_AN_OPEN_PROXY = {**BEHIND_A_PROXY, "name": "events_open_proxy", "proxy": "http://proxy.corp:3128"}


# ---------------------------------------------------------------------------
# 5. Two escape hatches — for a sandbox, never for production
#
# `allow_insecure_http`  accepts a plain http:// base_url. localhost is always
#                        accepted; this is for a trusted internal network.
# `allow_literal_secrets` accepts a token typed in the configuration. It exists
#                        for a throwaway local test: the value then sits in the
#                        notebook, in the repository, in every copy of the file.
# ---------------------------------------------------------------------------

INTERNAL_SANDBOX = {
    "name": "events_sandbox",
    "base_url": "http://sandbox.internal.corp/v1/events",
    "allow_insecure_http": True,
    "auth": {"type": "bearer_token", "token": "throwaway-sandbox-token"},
    "allow_literal_secrets": True,
    "pagination": {"type": "none", "items_field": "events"},
    "target_schema": "bronze",
    "target_table": "events_sandbox",
}


# ---------------------------------------------------------------------------
# 6. Two request shapes
#
# `allow_body_on_get`  a GET that carries a body has no defined meaning in HTTP
#                      and most APIs ignore it, so it is refused — unless the
#                      API's filter lives there. Declare the exception.
# `fail_on_empty`      a run that loads no row fails *before* the watermark moves.
#                      For a source that must never be silent.
# ---------------------------------------------------------------------------

FILTER_IN_A_GET_BODY = {
    **BASE,
    "name": "events_get_body",
    "target_table": "events_get_body",
    "method": "GET",
    "allow_body_on_get": True,
    "body": {"filter": {"type": "audit"}},
    "fail_on_empty": True,
}


ALL_SOURCES = [
    BOUNDED, EXCLUSIVE, POLITE, BEHIND_A_PROXY, BEHIND_AN_OPEN_PROXY, INTERNAL_SANDBOX,
    FILTER_IN_A_GET_BODY,
]


def validate_all():
    for config in ALL_SOURCES:
        try:
            validate_config(config)
            print(f"ok   {config['name']}")
        except ConfigError as exc:
            print(f"KO   {config['name']}: {exc}")


# validate_all()
