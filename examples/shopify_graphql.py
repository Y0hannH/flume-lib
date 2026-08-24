# Fabric Python notebook (non-Spark) — Shopify Admin GraphQL through flume-lib.
#
# Nothing here is Shopify-specific library code. A Relay connection is reached
# with the generic `cursor` pagination (dotted `cursor_field`/`has_more_field`),
# `record_field` to unwrap the edges, `params_path` to put the page params where
# GraphQL expects them, and `errors` to catch what Shopify answers with a 200.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.9.0

from flume_lib import run_source

SHOP = "my-shop"  # <SHOP>.myshopify.com
# The version lives in the URL and is supported for 12 months. Pin it, and
# review it before it goes unsupported: https://shopify.dev/docs/api/usage/versioning
API_VERSION = "2026-07"
KEYVAULT = "https://mykv.vault.azure.net"

GRAPHQL_URL = f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/graphql.json"

# Custom-app Admin API access token: a single header, no token endpoint, no
# expiry — the whole backfill runs on it.
ACCESS_TOKEN = {
    "type": "api_key_header",
    "header_name": "X-Shopify-Access-Token",
    "key": {"keyvault_url": KEYVAULT, "secret_name": "shopify-admin-token"},
}

# GraphQL answers 200 even when it failed: `errors` sits next to `data`, and a
# partial failure (a field refused by a missing scope) returns usable rows AND
# an error. Without this block that run would be reported `success`, silently
# short of a column. `THROTTLED` is retried — Shopify's cost-based limiter
# announces itself in the body, not with a 429.
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
    # The leaky bucket refills at ~50 points/s: a throttled call is worth
    # waiting out rather than failing the run.
    "retry": {"max_attempts": 6, "backoff_multiplier": 2},
    "target_schema": "shopify",
}

# `first`/`after` are injected into `variables` by the pagination config, so the
# query only has to declare them. Sorting on UPDATED_AT keeps the watermark
# meaningful. Written with spaces around braces out of habit — `template_paths`
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
    """Config for one Relay connection. `root` is the connection field name —
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
            "limit": 250,  # Shopify's hard maximum per page
            "limit_param": "first",
            # GraphQL wants `first`/`after` beside `query`, inside `variables`.
            "params_in": "body",
            "params_path": "variables",
        },
    }


def incremental_source(name: str, table: str, root: str, query: str):
    """Same, filtered on the watermark. Shopify's search syntax carries the
    bound; the placeholder is substituted inside `variables` only."""
    config = connection_source(
        name, table, root, query, {"q": "updated_at:>'{watermark}'"}
    )
    config["incremental"] = {
        "enabled": True,
        "field": "updatedAt",
        "inject": "body_template",
        "initial_value": "1970-01-01T00:00:00Z",
        # Shopify returns UTC ISO 8601 ("2026-08-22T09:00:00Z"), so the Python
        # max() that advances the watermark sorts correctly.
        "value_format": "iso_datetime",
    }
    return config


# ---------------------------------------------------------------------------
# Steady state: one incremental run per source.
#
# Scopes matter here: `read_orders` only reaches the last 60 days. Older orders
# need `read_all_orders`, granted by Shopify on request — without it the API
# answers 200 with an ACCESS_DENIED error, which the `errors` block turns into a
# failed run instead of a quietly truncated one.
# ---------------------------------------------------------------------------

INCREMENTAL_SOURCES = [
    incremental_source("shopify_orders", "orders", "orders", ORDERS_QUERY),
    incremental_source("shopify_products", "products", "products", PRODUCTS_QUERY),
]


def run_incremental():
    for config in INCREMENTAL_SOURCES:
        result = run_source(config)
        print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
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
# ---------------------------------------------------------------------------


def backfill_orders(first_year: int, last_year: int):
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            end_year, end_month = year + (month == 12), month % 12 + 1
            bounds = (
                f"updated_at:>='{year}-{month:02d}-01' "
                f"updated_at:<'{end_year}-{end_month:02d}-01'"
            )
            config = connection_source(
                f"shopify_orders_backfill_{year}_{month:02d}",
                "orders",
                "orders",
                ORDERS_QUERY,
                {"q": bounds},
            )
            result = run_source(config)
            print(f"{year}-{month:02d}: {result.status} ({result.rows_loaded} rows)")
            if result.status == "failed":
                # Each slice is independent: note it and rerun just this one.
                print(f"  {result.error_message}")


# Nested selections (`currentTotalPriceSet`, `customer`) land in Delta as JSON
# strings — one column each, parsed downstream.
#
# Validate the token, the scopes and the query shape without writing anything:
#   run_source(INCREMENTAL_SOURCES[0], dry_run=True)
#
# If THROTTLED keeps coming back, lower `limit`: the bucket is charged by query
# cost, and 250 heavy nodes can cost more than one call is allowed.
#
# backfill_orders(2020, 2026)
# run_incremental()
