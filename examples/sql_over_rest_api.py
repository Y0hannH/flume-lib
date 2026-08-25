# Fabric Python notebook (non-Spark) — a SQL-over-REST API through flume-lib.
#
# Some systems — ERPs in particular — expose their data as a single REST
# endpoint that takes an SQL query in the request body and answers with a page
# of JSON. There is no library code specific to any of them: it is a plain
# POST, reached with the generic `oauth1`, `headers`, `body` templating and
# `offset` / `keyset` pagination options.
#
# What makes this shape worth its own example is that the *filter lives inside
# the query*. A watermark, or a pagination key, has to be substituted into the
# SQL rather than appended to the URL — which is what `body_template` does.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.13.0

from datetime import date

from flume_lib import run_source

ACCOUNT = "1234567"  # the tenant identifier this API keys everything on
KEYVAULT = "https://mykv.vault.azure.net"
SQL_URL = "https://api.example.com/services/rest/query/v1/sql"

# OAuth 1.0a request signing: four credentials, all held in Key Vault. Unlike
# OAuth 2.0 client credentials, these tokens do not expire — which matters
# here, because a full backfill runs longer than the 60-minute lifetime of a
# typical access token, and nothing has to be renewed mid-run.
#
# `realm` is sent in the header, outside the signature; APIs that use it expect
# the account identifier there. `signature_method` defaults to HMAC-SHA256 and
# is spelled out below because older APIs still require HMAC-SHA1, and the
# failure mode is a bare 401 with nothing explaining it.
SIGNED_AUTH = {
    "type": "oauth1",
    "realm": ACCOUNT,
    "signature_method": "HMAC-SHA256",
    "consumer_key": {"keyvault_url": KEYVAULT, "secret_name": "api-consumer-key"},
    "consumer_secret": {"keyvault_url": KEYVAULT, "secret_name": "api-consumer-secret"},
    "token": {"keyvault_url": KEYVAULT, "secret_name": "api-token-id"},
    "token_secret": {"keyvault_url": KEYVAULT, "secret_name": "api-token-secret"},
}

# Three things happen in this block.
#
# `headers` carries what the endpoint requires of every call — here `Prefer:
# transient` (RFC 7240), asking for a result set that is not persisted
# server-side. Literal strings only: a credential belongs in `auth`.
#
# `limit`/`offset` go to the query string (the default `params_in`) while the
# query itself travels in the body. Both channels of the same request are used,
# which is the norm for this API shape.
#
# `timeout_seconds` bounds one HTTP call, not the run. The default is 60 s,
# raised here because an aggregate over a large table can legitimately sit a
# while before the first byte comes back.
BASE = {
    "base_url": SQL_URL,
    "method": "POST",
    "headers": {"Prefer": "transient"},
    "auth": SIGNED_AUTH,
    "pagination": {"type": "offset", "limit": 1000, "items_field": "items"},
    "target_schema": "erp",
    "timeout_seconds": 180,
}

# Project both date columns through the dialect's date formatter. The library
# writes dates as strings — it does no temporal typing — so every comparison
# downstream is a string comparison, and only an ISO-ordered format sorts
# correctly.
#
# `lastmodified` needs it because the incremental comparison is a Python max()
# over the returned values. `trandate_iso` needs it because the monthly
# backfill below filters the Delta table on that column: a display format would
# put '3/1/2026' before '3/10/2026' before '3/2/2026', and the window replaced
# would not be the window loaded.
TRANSACTIONS_SELECT = (
    "SELECT id, tranid, type, entity, trandate, foreigntotal, "
    "TO_CHAR(trandate, 'YYYY-MM-DD') AS trandate_iso, "
    "TO_CHAR(lastmodifieddate, 'YYYY-MM-DD HH24:MI:SS') AS lastmodified "
    "FROM transaction"
)


# ---------------------------------------------------------------------------
# Steady state: one incremental run per source.
#
# The watermark is substituted into the SQL through `{watermark}`, which is
# what "inject": "body_template" means. `value_format` is mandatory in that
# mode and is not a formality: the value comes back from the API and lands
# inside a query, so it is constrained to a shape with no room for syntax.
#
# `checkpoint` is on for the transactions source. It commits the watermark
# after every batch instead of once at the end, so a run interrupted after four
# million rows resumes where it stopped rather than replaying from the start.
# It is only correct because the query is ORDER BY the watermark column — the
# library verifies this and fails the run if a batch goes backwards, rather
# than advancing a watermark that would skip rows for good.
# ---------------------------------------------------------------------------

INCREMENTAL_SOURCES = [
    {
        **BASE,
        "name": "erp_transactions",
        "target_table": "transactions",
        "body": {
            "q": TRANSACTIONS_SELECT + " WHERE lastmodifieddate >= "
            "TO_TIMESTAMP('{watermark}', 'YYYY-MM-DD HH24:MI:SS') "
            "ORDER BY lastmodifieddate"
        },
        "incremental": {
            "enabled": True,
            "field": "lastmodified",
            "inject": "body_template",
            "initial_value": "1970-01-01 00:00:00",
            "value_format": "iso_datetime",
            "checkpoint": True,
        },
    },
    {
        # A small reference table, reloaded whole on every run: the source is
        # the truth and no history is kept. Without "mode": "overwrite" this
        # would append a full copy of the catalogue every night.
        **BASE,
        "name": "erp_items",
        "target_table": "items",
        "body": {"q": "SELECT id, itemid, displayname, itemtype FROM item ORDER BY id"},
        "write": {"mode": "overwrite"},
    },
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
# Backfill: monthly slices.
#
# This API refuses an `offset` beyond 100 000, which used to make a single-pass
# backfill impossible on a large table. Memory is no longer a constraint —
# `batch_size` bounds it — so the choice is now between two shapes:
#
#   - monthly slices, below, each independently restartable and easy to rerun
#     one at a time after a failure;
#   - a single keyset run (see BACKFILL_KEYSET further down), which walks the
#     whole table in one pass without ever paying an offset.
#
# Slices remain the safer default when the table is being written to while you
# read it. Incremental is off in both: the bounds are explicit, so no watermark
# is read or written.
#
# Each slice is written with "mode": "replace_where", which makes it
# **rerunnable**: the month is replaced rather than appended to, so a slice
# that failed halfway — or one you simply want to reload — costs a rerun and
# nothing else. Appending would have left the first attempt's rows in place and
# duplicated the rest, with a manual dedup as the only way out.
#
# The predicate must describe **exactly** the window the query returns. Both
# come from the same `month_bounds()` call for that reason: delta-rs refuses to
# commit rows that fall outside the predicate, so a mismatch fails the run
# instead of quietly replacing the wrong month.
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
                "name": f"erp_transactions_backfill_{year}_{month:02d}",
                "target_table": "transactions",
                "body": {
                    "q": TRANSACTIONS_SELECT
                    + f" WHERE trandate >= TO_DATE('{start}', 'YYYY-MM-DD')"
                    + f" AND trandate < TO_DATE('{end}', 'YYYY-MM-DD')"
                    + " ORDER BY id"
                },
                "write": {
                    "mode": "replace_where",
                    "replace_where": (
                        f"trandate_iso >= '{start}' AND trandate_iso < '{end}'"
                    ),
                },
            }
            result = run_source(config)
            print(f"{year}-{month:02d}: {result.status} ({result.rows_loaded} rows)")
            for warning in result.warnings:
                # A month that returns nothing replaces nothing: the warning is
                # the only sign that the slice left the table untouched.
                print(f"  ! {warning}")
            if result.status == "failed":
                # Each slice is independent and rerunnable: note it and rerun
                # just this one.
                print(f"  {result.error_message}")


# ---------------------------------------------------------------------------
# Backfill in one pass: keyset pagination.
#
# `offset` pays for every row it skips, and this API stops honouring it past
# 100 000. `keyset` filters on the last id seen instead — `WHERE id > {last_id}
# ORDER BY id` — so page 3 000 costs exactly what page 1 costs, and no ceiling
# applies. The whole table is reachable in a single run.
#
# Three conditions, all met here:
#   - the query is ORDER BY id, and ids are unique;
#   - the key lives inside the SQL, hence "params_in": "body_template", which
#     substitutes it into the {last_id} marker of the body;
#   - the key comes back from the API, so "value_format": "numeric" constrains
#     it to a shape with no room for syntax — mandatory in this mode.
#
# Both channels of the request are used at once: `{last_id}` is substituted
# into the body because its placeholder is there, while `limit` — whose
# placeholder is not — goes to the query string where this API expects it (see
# BASE above). A param is never dropped: it lands in whichever channel can
# carry it.
# ---------------------------------------------------------------------------

BACKFILL_KEYSET = {
    **BASE,
    "name": "erp_transactions_backfill",
    "target_table": "transactions",
    "body": {"q": TRANSACTIONS_SELECT + " WHERE id > {last_id} ORDER BY id"},
    "pagination": {
        "type": "keyset",
        "key_field": "id",
        "key_param": "last_id",
        "params_in": "body_template",
        "value_format": "numeric",
        # first page: every id is greater than 0
        "initial_value": 0,
        "items_field": "items",
        # 1 000 is this API's maximum as well as its default; sent as a query
        # param, so the library knows the real page size and recognises the
        # last, shorter page instead of calling once more to find it empty.
        "limit": 1000,
        "limit_param": "limit",
        # Two safety nets, not targets: 2.8 M rows in 1 000-row pages is
        # ~2 800 calls. Reaching either bound fails the run rather than
        # truncating it silently — a `success` short of half its data is the
        # failure mode this library exists to avoid. `max_rows` is the one that
        # catches a query whose WHERE clause was lost in an edit.
        "max_pages": 5000,
        "max_rows": 10_000_000,
    },
    # 50 000 rows per Delta commit is the default; lowering it shortens what a
    # failed run has to redo, at the cost of more commits.
    "batch_size": 50000,
}


def backfill_keyset():
    """One run for the whole table. Rows already written stay written if it
    fails: rerun after deleting them by `_flume_run_id`, or narrow the query
    with the last id actually loaded."""
    result = run_source(BACKFILL_KEYSET)
    print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
    for warning in result.warnings:
        print(f"  ! {warning}")
    if result.status == "failed":
        print(f"  {result.error_message}")
    return result


# Validate credentials and the query shape without writing anything first:
#   run_source(INCREMENTAL_SOURCES[0], dry_run=True)
#
# backfill_transactions(2020, 2026)
# run_incremental()
