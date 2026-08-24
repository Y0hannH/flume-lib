# Fabric Python notebook (non-Spark) — NetSuite SuiteQL through flume-lib.
#
# Nothing here is NetSuite-specific library code: SuiteQL is a plain REST POST
# endpoint, reached with the generic `oauth1`, `headers`, `body` templating and
# `offset` pagination options.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.9.0

from datetime import date

from flume_lib import run_source

ACCOUNT_ID = "1234567"  # sandbox accounts look like 1234567_SB1
KEYVAULT = "https://mykv.vault.azure.net"
SUITEQL_URL = (
    f"https://{ACCOUNT_ID.lower().replace('_', '-')}"
    ".suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
)

# Token-Based Authentication: four credentials, all held in Key Vault. TBA
# tokens do not expire, which matters here — a full backfill runs longer than
# the 60-minute lifetime of a NetSuite OAuth 2.0 access token.
TBA = {
    "type": "oauth1",
    "realm": ACCOUNT_ID,
    "consumer_key": {"keyvault_url": KEYVAULT, "secret_name": "netsuite-consumer-key"},
    "consumer_secret": {"keyvault_url": KEYVAULT, "secret_name": "netsuite-consumer-secret"},
    "token": {"keyvault_url": KEYVAULT, "secret_name": "netsuite-token-id"},
    "token_secret": {"keyvault_url": KEYVAULT, "secret_name": "netsuite-token-secret"},
}

# `Prefer: transient` is required by the SuiteQL endpoint. `limit`/`offset` go
# to the query string (the default `params_in`), the query itself in the body.
BASE = {
    "base_url": SUITEQL_URL,
    "method": "POST",
    "headers": {"Prefer": "transient"},
    "auth": TBA,
    "pagination": {"type": "offset", "limit": 1000, "items_field": "items"},
    "target_schema": "netsuite",
}

# Project the watermark column through TO_CHAR: the incremental comparison is a
# Python max() over the returned values, so it needs a lexicographically
# sortable string, not NetSuite's display format.
TRANSACTIONS_SELECT = (
    "SELECT id, tranid, type, entity, trandate, foreigntotal, "
    "TO_CHAR(lastmodifieddate, 'YYYY-MM-DD HH24:MI:SS') AS lastmodified "
    "FROM transaction"
)


# ---------------------------------------------------------------------------
# Steady state: one incremental run per source.
# ---------------------------------------------------------------------------

INCREMENTAL_SOURCES = [
    {
        **BASE,
        "name": "netsuite_transactions",
        "target_table": "transactions",
        "body": {
            "q": TRANSACTIONS_SELECT + " WHERE lastmodifieddate >= "
            "TO_TIMESTAMP('{watermark}', 'YYYY-MM-DD HH24:MI:SS') ORDER BY id"
        },
        "incremental": {
            "enabled": True,
            "field": "lastmodified",
            "inject": "body_template",
            "initial_value": "1970-01-01 00:00:00",
            "value_format": "iso_datetime",
        },
    },
    {
        **BASE,
        "name": "netsuite_items",
        "target_table": "items",
        "body": {"q": "SELECT id, itemid, displayname, itemtype FROM item ORDER BY id"},
    },
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
# Two hard limits make a single-pass backfill impossible on a large table:
# NetSuite refuses an `offset` beyond 100 000, and run_source accumulates every
# page in memory before writing. One run per month keeps both well within
# bounds and makes each slice independently restartable. Incremental is off —
# the bounds are explicit, so no watermark is read or written.
# ---------------------------------------------------------------------------


def month_bounds(year: int, month: int) -> tuple[str, str]:
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start.isoformat(), end.isoformat()


def backfill_transactions(first_year: int, last_year: int):
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            start, end = month_bounds(year, month)
            config = {
                **BASE,
                "name": f"netsuite_transactions_backfill_{year}_{month:02d}",
                "target_table": "transactions",
                "body": {
                    "q": TRANSACTIONS_SELECT
                    + f" WHERE trandate >= TO_DATE('{start}', 'YYYY-MM-DD')"
                    + f" AND trandate < TO_DATE('{end}', 'YYYY-MM-DD')"
                    + " ORDER BY id"
                },
            }
            result = run_source(config)
            print(f"{year}-{month:02d}: {result.status} ({result.rows_loaded} rows)")
            if result.status == "failed":
                # Each slice is independent: note it and rerun just this one.
                print(f"  {result.error_message}")


# Validate credentials and the query shape without writing anything first:
#   run_source(INCREMENTAL_SOURCES[0], dry_run=True)
#
# backfill_transactions(2020, 2026)
# run_incremental()
