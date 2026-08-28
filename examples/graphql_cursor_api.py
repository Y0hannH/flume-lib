# Fabric Python notebook (non-Spark) — a GraphQL cursor API through flume-lib.
#
# There is no GraphQL source type. A GraphQL endpoint is a POST of
# `{query, variables}` to one URL, and the connection shape most of them use —
# records under `edges`, each wrapped in a `{cursor, node}`, with a `pageInfo`
# carrying the next cursor — is reached with the generic `cursor` pagination
# (dotted `cursor_field`/`has_more_field`), `record_field` to unwrap the edges,
# `params_path` to put the page params where GraphQL expects them, and `errors`
# to catch what the API answers with an HTTP 200.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.17.0

from flume_lib import run_source

# The API version lives in the URL and is supported for a limited window. Pin
# it, and review it before it goes unsupported — an unpinned endpoint changes
# its response shape under you between two nightly runs.
API_VERSION = "2026-07"
KEYVAULT = "https://mykv.vault.azure.net"

GRAPHQL_URL = f"https://api.example.com/admin/{API_VERSION}/graphql.json"

# A long-lived access token in a single header: no token endpoint, no expiry,
# so the whole backfill runs on it. For a token that *does* expire mid-run, see
# the `token_endpoint` and `oauth2_client_credentials` blocks in
# rest_api_auth_variants.py — those renew themselves without failing the run.
ACCESS_TOKEN = {
    "type": "api_key_header",
    "header_name": "X-Api-Access-Token",
    "key": {"keyvault_url": KEYVAULT, "secret_name": "graphql-admin-token"},
}

# GraphQL answers 200 even when it failed: `errors` sits next to `data`, and a
# partial failure — a field refused by a missing scope — returns usable rows
# AND an error. Without this block that run would be reported `success`,
# silently short of a column.
#
# `retryable_codes` is the other half: cost-based limiters announce throttling
# in the body rather than with a 429, so `THROTTLED` has to be named here to be
# replayed instead of failing the run.
GRAPHQL_ERRORS = {
    "path": "errors",
    "code_field": "extensions.code",
    "message_field": "message",
    "retryable_codes": ["THROTTLED"],
}

BASE = {
    "base_url": GRAPHQL_URL,
    "method": "POST",
    "auth": ACCESS_TOKEN,
    "errors": GRAPHQL_ERRORS,
    # A leaky bucket that refills at ~50 points/s is worth waiting out rather
    # than failing the run: more attempts, slower backoff.
    #
    # `max_retry_after_seconds` caps what a `Retry-After` header can impose. A
    # server asking for 30 minutes gets honoured up to 10, then the attempt
    # fails — a run marked `failed` in log_runs is a more useful signal than a
    # notebook blocked half an hour on a session that may already be dead.
    "retry": {
        "max_attempts": 6,
        "backoff_multiplier": 2,
        "max_retry_after_seconds": 600,
    },
    "target_schema": "commerce",
}

# `first`/`after` are injected into `variables` by the pagination config, so the
# query only has to declare them. Sorting on UPDATED_AT keeps the watermark
# meaningful — an unsorted connection would advance it past records it never
# returned. Written with spaces around braces out of habit; `template_paths`
# already keeps the templating away from the query.
ORDERS_QUERY = """
query Orders($first: Int!, $after: String, $q: String) {
  orders(first: $first, after: $after, query: $q, sortKey: UPDATED_AT) {
    edges {
      node {
        id
        name
        createdAt
        updatedAt
        displayFinancialStatus
        displayFulfillmentStatus
        currentTotalPriceSet { shopMoney { amount currencyCode } }
        customer { id email }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

PRODUCTS_QUERY = """
query Products($first: Int!, $after: String, $q: String) {
  products(first: $first, after: $after, query: $q, sortKey: UPDATED_AT) {
    edges {
      node {
        id
        title
        handle
        status
        productType
        vendor
        createdAt
        updatedAt
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def connection_source(name: str, table: str, root: str, query: str, variables: dict):
    """Config for one connection. `root` is the connection field name —
    everything the pagination needs hangs off `data.<root>`."""
    return {
        **BASE,
        "name": name,
        "target_table": table,
        "body": {"query": query, "variables": variables},
        # The query is full of braces; without this, a compact `{id}` would be
        # read as a placeholder and fail the run. Only `variables` is templated.
        "template_paths": ["variables"],
        "pagination": {
            "type": "cursor",
            "items_field": f"data.{root}.edges",
            "record_field": "node",
            "cursor_field": f"data.{root}.pageInfo.endCursor",
            "has_more_field": f"data.{root}.pageInfo.hasNextPage",
            "cursor_param": "after",
            "limit": 250,  # a common hard maximum per page
            "limit_param": "first",
            # GraphQL wants `first`/`after` beside `query`, inside `variables`.
            "params_in": "body",
            "params_path": "variables",
        },
    }


def incremental_source(name: str, table: str, root: str, query: str):
    """Same, filtered on the watermark. The API's search syntax carries the
    bound; the placeholder is substituted inside `variables` only.

    `placeholder` renames the marker from the default `{watermark}` to
    `{updated_after}`. Nothing forces it — it is worth it when the query is
    read by someone who does not know this library and would otherwise wonder
    what a "watermark" is doing in a search filter.
    """
    config = connection_source(
        name, table, root, query, {"q": "updated_at:>'{updated_after}'"}
    )
    config["incremental"] = {
        "enabled": True,
        "field": "updatedAt",
        "inject": "body_template",
        "placeholder": "updated_after",
        "initial_value": "1970-01-01T00:00:00Z",
        # UTC ISO 8601 ("2026-08-22T09:00:00Z"), so the Python max() that
        # advances the watermark sorts correctly.
        "value_format": "iso_datetime",
    }
    return config


# ---------------------------------------------------------------------------
# Steady state: one incremental run per source.
#
# Scopes matter here: a token granted read access to recent orders only will
# answer 200 with an ACCESS_DENIED error on older ones, which the `errors`
# block turns into a failed run instead of a quietly truncated one.
# ---------------------------------------------------------------------------

INCREMENTAL_SOURCES = [
    incremental_source("commerce_orders", "orders", "orders", ORDERS_QUERY),
    incremental_source("commerce_products", "products", "products", PRODUCTS_QUERY),
]


def run_incremental():
    for config in INCREMENTAL_SOURCES:
        result = run_source(config)
        print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
        for warning in result.warnings:
            print(f"  ! {warning}")
        if result.status == "failed":
            print(f"  {result.error_message}")


# ---------------------------------------------------------------------------
# Backfill: bounded slices instead of one long run.
#
# A cursor has no offset ceiling, and memory is no longer a constraint either
# — `batch_size` bounds it. Monthly slices remain useful for a different
# reason: each one is independently restartable, so a failure costs one month
# rather than the whole backfill. Incremental is off: the bounds are explicit,
# so no watermark is read or written.
#
# Each slice replaces its own month rather than appending to it, which makes it
# rerunnable. The predicate is written against the **Delta table**, not the
# API: `updatedAt` is the column name the records carry once written, and it
# holds ISO 8601 strings, so a string comparison over it orders correctly. The
# API-side bound and the predicate must describe the same window — a mismatch
# is refused at commit rather than silently replacing the wrong month.
# ---------------------------------------------------------------------------


def backfill_orders(first_year: int, last_year: int):
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            end_year, end_month = year + (month == 12), month % 12 + 1
            start = f"{year}-{month:02d}-01"
            end = f"{end_year}-{end_month:02d}-01"
            config = connection_source(
                f"commerce_orders_backfill_{year}_{month:02d}",
                "orders",
                "orders",
                ORDERS_QUERY,
                {"q": f"updated_at:>='{start}' updated_at:<'{end}'"},
            )
            config["write"] = {
                "mode": "replace_where",
                "replace_where": f"updatedAt >= '{start}' AND updatedAt < '{end}'",
            }
            result = run_source(config)
            print(f"{year}-{month:02d}: {result.status} ({result.rows_loaded} rows)")
            for warning in result.warnings:
                print(f"  ! {warning}")
            if result.status == "failed":
                # Each slice is independent and rerunnable: note it and rerun
                # just this one.
                print(f"  {result.error_message}")


# Nested selections (`currentTotalPriceSet`, `customer`) land in Delta as JSON
# strings — one column each, parsed downstream.
#
# Validate the token, the scopes and the query shape without writing anything:
#   run_source(INCREMENTAL_SOURCES[0], dry_run=True)
#
# If THROTTLED keeps coming back, lower `limit`: these buckets are charged by
# query cost, and 250 heavy nodes can cost more than one call is allowed.
#
# backfill_orders(2020, 2026)
# run_incremental()
