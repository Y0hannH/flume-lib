# Fabric Python notebook (non-Spark) — the ordinary REST API through flume-lib.
#
# The reference example: one fictional vendor API, `https://api.example.com/v1`,
# read with each of the five pagination shapes the library supports. Most client
# APIs are one of these five and need nothing more than the block below. The
# SQL-over-REST and GraphQL examples exist because those two shapes are *not*
# ordinary.
#
# Read this one first; the others assume it.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.11.0

from flume_lib import ConfigError, run_source, validate_config

API = "https://api.example.com/v1"
KEYVAULT = "https://mykv.vault.azure.net"

# A static token in Key Vault: the most common case by far. The credential is a
# reference, never a literal — the config file itself carries no secret and can
# live in `Files/conf/` beside everything else.
TOKEN = {
    "type": "bearer_token",
    "token": {"keyvault_url": KEYVAULT, "secret_name": "example-api-token"},
}

BASE = {
    "auth": TOKEN,
    "target_schema": "bronze",
    # Defaults are 3 attempts / multiplier 1. Raised here because this API
    # rate-limits with a 429 and a `Retry-After`, which the library honors in
    # place of the exponential backoff.
    "retry": {"max_attempts": 5, "backoff_multiplier": 2},
}


# ---------------------------------------------------------------------------
# 1. `offset` — `?limit=100&offset=200`. The default shape.
#
# Stops on an empty page, or on a page shorter than `limit`. That second
# condition is what makes it cheap: a source of 250 records costs 3 calls, not
# 4. It also makes it wrong for an API that returns short-but-not-last pages
# (server-side filtering applied after the page is cut) — such an API needs a
# `has_more` flag, so `cursor`, not `offset`.
# ---------------------------------------------------------------------------

OFFSET_SOURCE = {
    **BASE,
    "name": "example_customers",
    "base_url": f"{API}/customers",
    "target_table": "customers",
    # Fixed filters go in `params`; they are sent on every call, beside the
    # pagination params.
    "params": {"status": "active"},
    "pagination": {
        "type": "offset",
        "limit": 500,
        # Rename them when the API disagrees with `limit`/`offset` — `top`/`skip`
        # (OData-flavored), `count`/`start`, `per_page`/`from`…
        "limit_param": "limit",
        "offset_param": "offset",
        # Where the records are. Omitted, the library probes `data`, `items`,
        # `results`, `value`, and fails explicitly if none of them holds a list —
        # name it anyway: an API that renames the envelope in v2 then fails the
        # run instead of silently loading nothing.
        "items_field": "data",
    },
}


# ---------------------------------------------------------------------------
# 2. `page` — `?page=3&per_page=100`.
#
# Two stop conditions, and the choice matters. With `total_pages_header` the
# count is read from the first response and the loop stops exactly there —
# one call per page, no probing call at the end. Without it, the loop stops on
# an empty or partial page, which costs one extra call whenever the last page
# happens to be full.
#
# Set `start_page: 0` for the zero-based APIs.
# ---------------------------------------------------------------------------

PAGE_SOURCE = {
    **BASE,
    "name": "example_invoices",
    "base_url": f"{API}/invoices",
    "target_table": "invoices",
    "pagination": {
        "type": "page",
        "page_param": "page",
        "start_page": 1,
        # `size_param` is only sent when `page_size` is set too — the pair goes
        # together, one without the other is silently no size at all.
        "size_param": "per_page",
        "page_size": 200,
        # A header that is absent, or not a number, fails the run: the loop has
        # no fallback once it has been told to trust a total.
        "total_pages_header": "X-Total-Pages",
        "items_field": "results",
    },
}


# ---------------------------------------------------------------------------
# 3. `next_link` — the response carries the full URL of the next page.
#
# The `params` above are sent on the first call only; the next-page URL already
# embeds its own query string, and re-appending ours would double the filters.
#
# Caveat worth knowing before pointing this at a third party: the library
# requests whatever URL the response provides, on the same authenticated
# session — a hostile endpoint can therefore aim the next page at a host of its
# choosing and receive the `Authorization` header. There is no allowlist. See
# docs/security.md, "Values that come back from a response". `offset`, `page`
# and `cursor` only ever call `base_url`.
# ---------------------------------------------------------------------------

NEXT_LINK_SOURCE = {
    **BASE,
    "name": "example_events",
    "base_url": f"{API}/events",
    "target_table": "events",
    "pagination": {
        "type": "next_link",
        # Unlike `items_field` and `cursor_field`, this one is a top-level key,
        # not a dotted path: `@odata.nextLink` works because that *is* the key,
        # dot included, whereas a URL nested under `links.next` is out of reach.
        # When the nested value is a token rather than a whole URL, it is a
        # `cursor` source (below); when it really is a nested URL, the endpoint
        # has to be read with whichever of `offset`/`page` it also supports.
        "next_field": "next",
        "items_field": "data",
    },
}


# ---------------------------------------------------------------------------
# 4. `cursor` — an opaque token, without GraphQL.
#
# The REST flavor of what graphql_cursor_api.py does over a GraphQL
# connection: same strategy, flat response, params in the query string. The first request
# goes without a cursor.
#
# `has_more_field` is what makes this shape safe on a filtered endpoint: an
# empty page in the *middle* of the results reads as the end to every
# stop-on-empty heuristic, and this API filters server-side after cutting the
# page. Set it whenever the API provides it.
# ---------------------------------------------------------------------------

CURSOR_SOURCE = {
    **BASE,
    "name": "example_transactions",
    "base_url": f"{API}/transactions",
    "target_table": "transactions",
    "pagination": {
        "type": "cursor",
        "cursor_param": "cursor",
        "cursor_field": "paging.next_cursor",
        "has_more_field": "paging.has_more",
        "limit": 1000,
        "limit_param": "page_size",
        "items_field": "data",
    },
}


# ---------------------------------------------------------------------------
# 5. `none` — one call, no loop.
#
# Reference data small enough to fit in a single response. This endpoint answers
# with a bare JSON array (`[{"code": "EUR", …}, …]`), which needs no
# `items_field`: a response that is already a list is used as-is.
# ---------------------------------------------------------------------------

SINGLE_CALL_SOURCE = {
    **BASE,
    "name": "example_currencies",
    "base_url": f"{API}/reference/currencies",
    "target_table": "currencies",
    # `{"type": "none"}` and no `pagination` at all are the same thing. State it
    # when the absence is a decision rather than an oversight.
    "pagination": {"type": "none"},
}


# ---------------------------------------------------------------------------
# Incremental: only what changed since the last successful run.
#
# The watermark is read from `flume.watermark` at run start and sent as one
# query param; after a successful run, the max of `field` over the loaded
# records is written back. `initial_value` is the floor of the very first run —
# without it, the first run sends no filter at all and loads everything, which
# is usually what you want, but say so on purpose.
#
# The catch of `inject: "query_param"`: it sends the watermark as the *whole*
# value of one param. It fits `?updated_since=2026-08-01T00:00:00Z` and cannot
# build `?$filter=updated ge 2026-08-01T00:00:00Z` — a composed expression needs
# the watermark inside a string, which is `inject: "body_template"` and
# therefore a POST body (see sql_over_rest_api.py, and the note in
# microsoft_graph_odata.py for the GET case).
# ---------------------------------------------------------------------------


def incremental(config: dict, field: str, param_name: str) -> dict:
    return {
        **config,
        "incremental": {
            "enabled": True,
            # Field of the *record*, not of the request — its max becomes the
            # next watermark. Comparison is a Python max(), so the values must
            # sort: ISO 8601 and numerics do, "24/08/2026" does not.
            "field": field,
            "param_name": param_name,
            "initial_value": "1970-01-01T00:00:00Z",
            "value_format": "iso_datetime",
        },
    }


SOURCES = [
    incremental(OFFSET_SOURCE, "updated_at", "updated_since"),
    incremental(PAGE_SOURCE, "updated_at", "modified_after"),
    incremental(NEXT_LINK_SOURCE, "occurred_at", "since"),
    incremental(CURSOR_SOURCE, "updated_at", "updated_since"),
    # Reference data: a few dozen rows, reloaded whole every time. An
    # incremental watermark on a table this size buys nothing and adds a
    # failure mode.
    SINGLE_CALL_SOURCE,
]


# ---------------------------------------------------------------------------
# Running them.
# ---------------------------------------------------------------------------


def check_configs():
    """Validate the whole list before calling anything. Cheap, offline, and it
    catches the typo that would otherwise cost a full run: an unknown key is a
    `ConfigError` with a "did you mean…", never a silent no-op."""
    for config in SOURCES:
        try:
            validate_config(config)
        except ConfigError as exc:
            print(f"{config.get('name')}: {exc}")


def run_all():
    for config in SOURCES:
        result = run_source(config)
        print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
        if result.status == "failed":
            # run_source never raises — a failed source does not stop the loop,
            # so the next one still runs and the whole batch is reported at once.
            print(f"  {result.error_message}")


def preview(config: dict):
    """Dry run: validates, really calls the API — so credentials, pagination and
    the response shape are all exercised — and writes nothing. Neither data, nor
    watermark, nor log_runs. The first records come back raw in `sample`, which
    is how you check `items_field` before committing to it."""
    result = run_source(config, dry_run=True)
    print(result.status, result.rows_loaded, result.error_message)
    for record in result.sample or []:
        print(record)


# check_configs()
# preview(SOURCES[0])
# run_all()
