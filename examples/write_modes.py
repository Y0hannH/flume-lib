# Fabric Python notebook (non-Spark) — every write mode, side by side.
#
# By default a run **appends**: rows are added to the target table and nothing
# is ever removed. That is the safe behaviour and it stays the default, but it
# makes a reload a problem — rerunning a backfill slice that failed halfway
# adds a second copy of its rows, and de-duplicating them by hand is the only
# way back.
#
# The `write` block covers the three shapes an ingestion actually needs:
#
#   append          add rows, remove nothing                  (default)
#   overwrite       replace the whole table                   (reference data)
#   replace_where   replace one window, keep the rest         (backfills)
#
# Every source below hits the same fictional API. The point is not the API, it
# is what lands in Delta and what a **rerun** does to it.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.13.0

from datetime import date, timedelta

from flume_lib import ConfigError, run_source, validate_config

KEYVAULT = "https://mykv.vault.azure.net"

BASE = {
    "base_url": "https://api.example.com/v1/orders",
    "auth": {
        "type": "bearer_token",
        "token": {"keyvault_url": KEYVAULT, "secret_name": "api-token"},
    },
    "pagination": {"type": "page", "page_size": 500, "items_field": "data"},
    "target_schema": "bronze",
}


# ---------------------------------------------------------------------------
# 1. append — the default
#
# No `write` block at all. Every run adds its rows to whatever is already
# there. Run this twice on the same data and the table holds two copies: the
# library makes no attempt to recognise a row it has already seen.
#
# That is the right default for an append-only stream (events, log lines, an
# incremental source whose watermark guarantees each row is returned once) and
# the wrong one for anything you might want to reload.
#
# Rows of one run all carry the same `_flume_run_id`, so a run that failed
# halfway can be undone afterwards:
#
#   DELETE FROM bronze.orders_events WHERE _flume_run_id = '<the run_id>'
# ---------------------------------------------------------------------------

APPEND_IMPLICIT = {
    **BASE,
    "name": "orders_events",
    "target_table": "orders_events",
}

# Spelling it out changes nothing and documents the intent — worth it in a
# source list where the other entries do replace data.
APPEND_EXPLICIT = {
    **BASE,
    "name": "orders_events_explicit",
    "target_table": "orders_events",
    "write": {"mode": "append"},
}


# ---------------------------------------------------------------------------
# 2. overwrite — the whole table, every run
#
# For reference data: a product catalogue, a currency table, a list of cost
# centres. The source is the truth, the table is a mirror of it, and no history
# is kept. Without this, a nightly full reload appends a complete copy of the
# catalogue every night and the table grows without bound.
#
# The replacement is committed with the **first batch** of the run, so a table
# smaller than `batch_size` (50 000 rows by default) is replaced atomically:
# the old contents and the new land in one Delta commit, all or nothing.
#
# What `overwrite` will not do is protect you from an empty answer — see
# section 6. A catalogue endpoint that returns nothing leaves the previous
# catalogue in place, on purpose.
# ---------------------------------------------------------------------------

REFERENCE_TABLE = {
    **BASE,
    "base_url": "https://api.example.com/v1/products",
    "name": "products_snapshot",
    "target_table": "products",
    "write": {"mode": "overwrite"},
}


# ---------------------------------------------------------------------------
# 3. replace_where — one window, rerunnable
#
# The predicate is plain SQL evaluated against the **target table**. The rows
# it matches are deleted and the run's rows written in their place, in one
# commit. Run it as many times as you like: one copy of the window remains.
#
# Three rules make it work.
#
#   a. The predicate must describe **exactly** what the query returns. delta-rs
#      validates this and refuses to commit a row falling outside it — see
#      section 7 for what that looks like when it goes wrong.
#
#   b. It can only use columns already in the target table, plus the
#      `_flume_*` lineage columns. It is evaluated by the Delta engine, not by
#      the API.
#
#   c. Dates are written as **strings** — this library does no temporal typing
#      — so `order_date >= '2026-01-01'` is a string comparison. It behaves
#      only if the column holds an ISO-ordered format. A `03/01/2026` column
#      sorts as text and the window replaced will not be the window loaded.
#
# `replace_where` is not templated: a literal `{month}` left in the string
# would match no row and the run would replace nothing, so the library refuses
# the config outright. Build the string here, one window per run.
# ---------------------------------------------------------------------------


def month_window(year: int, month: int) -> tuple[str, str]:
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start.isoformat(), end.isoformat()


def monthly_slice(year: int, month: int) -> dict:
    """One month of orders, replaceable in place. Both the API filter and the
    Delta predicate come from the same bounds — that is the invariant to
    preserve when adapting this."""
    start, end = month_window(year, month)
    return {
        **BASE,
        "name": f"orders_backfill_{year}_{month:02d}",
        "target_table": "orders",
        "params": {"updated_from": start, "updated_to": end},
        "write": {
            "mode": "replace_where",
            "replace_where": f"order_date >= '{start}' AND order_date < '{end}'",
        },
    }


def backfill_by_month(first_year: int, last_year: int):
    """Rerun the whole loop, or any single month, as often as needed."""
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            result = run_source(monthly_slice(year, month))
            print(f"{year}-{month:02d}: {result.status} ({result.rows_loaded} rows)")
            for warning in result.warnings:
                print(f"  ! {warning}")
            if result.status == "failed":
                print(f"  {result.error_message}")


# ---------------------------------------------------------------------------
# 4. replace_where over an id range
#
# A window does not have to be a date. Any expression the target table can
# evaluate works — an id range, a shop identifier, a country code, a
# combination. This one reloads a block of ids, which is what you want when
# the source has no usable date column but does have a monotonic key.
#
# Note the closed/open bounds (`>=` and `<`), used everywhere in this file:
# adjacent windows must not overlap. Two windows sharing a boundary row would
# each delete the other's copy of it, and the row would survive or not
# depending on the order the slices ran in.
# ---------------------------------------------------------------------------


def id_range_slice(first_id: int, last_id: int) -> dict:
    return {
        **BASE,
        "name": f"orders_reload_{first_id}_{last_id}",
        "target_table": "orders",
        "params": {"id_from": first_id, "id_to": last_id},
        "write": {
            "mode": "replace_where",
            "replace_where": f"id >= {first_id} AND id < {last_id}",
        },
    }


# ---------------------------------------------------------------------------
# 5. partition_by
#
# Partition columns are fixed **when the table is created**. Passing them for
# an existing unpartitioned table fails with an explicit message rather than
# silently doing nothing — changing them means rewriting the table whole, which
# this library does not do. Decide before the first run.
#
# Partitioning on a column the predicate filters on is the combination worth
# having: the replacement then rewrites only the matching partitions instead of
# scanning the table. Partition on a **low-cardinality** column — a month, a
# country — never on an id or a timestamp, or the table ends up with one
# directory per row.
#
# The library performs no OPTIMIZE and no VACUUM. A table written batch after
# batch accumulates small files, and the files a replacement supersedes stay on
# disk until vacuumed. Schedule both out of band if a table grows enough to
# need them.
# ---------------------------------------------------------------------------

PARTITIONED = {
    **BASE,
    "name": "orders_partitioned",
    "target_table": "orders_by_month",
    "write": {
        "mode": "replace_where",
        "replace_where": "order_month = '2026-01'",
        "partition_by": ["order_month"],
    },
}


# ---------------------------------------------------------------------------
# 6. An empty source replaces nothing
#
# If a run loads zero rows, no replacement happens: the target keeps its
# previous contents. This is deliberate. An API that is down, a filter that is
# too narrow, an expired scope and a genuinely empty month all answer "0 rows",
# and emptying a window on that signal would destroy data without anything
# having failed.
#
# Because the natural expectation is the opposite, the run says so:
# `RunResult.warnings` carries an explicit message and the run is still
# `success`. A success with a warning is worth reading — this is the one case
# where ignoring `warnings` loses information you cannot recover elsewhere,
# since the warning does not reach the `log_runs` table.
#
# If a window that comes back empty *should* be emptied in Delta, do it
# explicitly — the library will not guess:
#
#   DELETE FROM bronze.orders
#    WHERE order_date >= '2026-01-01' AND order_date < '2026-02-01'
# ---------------------------------------------------------------------------


def load_month_reporting_emptiness(year: int, month: int) -> bool:
    """Returns True when the month was actually replaced."""
    result = run_source(monthly_slice(year, month))
    if result.status != "success":
        print(f"failed: {result.error_message}")
        return False
    if result.rows_loaded == 0:
        # Same information as the warning, decided on rather than printed.
        print(f"{year}-{month:02d}: source empty, table left untouched")
        return False
    return True


# ---------------------------------------------------------------------------
# 7. When the predicate and the query disagree
#
# This is the mistake worth rehearsing. The config below asks for February from
# the API and tells Delta to replace January:
#
#   "params": {"updated_from": "2026-02-01", "updated_to": "2026-03-01"},
#   "write": {"mode": "replace_where",
#             "replace_where": "order_date >= '2026-01-01'"
#                              " AND order_date < '2026-02-01'"}
#
# Without validation this would delete January and write February into its
# place. delta-rs refuses instead: every row being written is checked against
# the predicate, and the commit is rejected as a whole. The run fails, the
# table is untouched, and the message names the predicate:
#
#   replace_where : des lignes écrites ne satisfont pas le prédicat
#   "order_date >= '2026-01-01' AND order_date < '2026-02-01'" [...]
#
# The library reports it through `RunResult.error_message` like any other
# failure — `run_source` never raises. Two other predicate mistakes surface the
# same way: a column the target table does not have, and a `partition_by` that
# disagrees with the existing table.
#
# Deriving both bounds from one function, as `monthly_slice()` does, is what
# stops this from happening in the first place.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 8. What a failed run leaves behind
#
# The replacement lands with the first batch, so a run that breaks at batch 3
# of 10 leaves the window holding those three batches — and what they replaced
# is gone from the live table. Delta keeps the previous version (`RESTORE`, or
# a time-travel read, recovers it until it is vacuumed), but the table is short
# of the rest until the run is replayed.
#
# Replaying it is exactly what this mode makes safe: rerun the same config and
# the window is rebuilt whole. A source under `batch_size` never sees this at
# all — replacement and data land in a single commit.
#
# This is also why a replacing mode cannot be combined with
# `incremental.checkpoint`: resuming mid-run would restart from the watermark
# and replace the window a second time, erasing what the interrupted run had
# already written into it. A backfill is replayed from the start of its window,
# not from its middle. The library refuses the combination at validation —
# below is what that refusal looks like.
# ---------------------------------------------------------------------------

REFUSED_COMBINATION = {
    **BASE,
    "name": "orders_refused",
    "target_table": "orders",
    "write": {"mode": "replace_where", "replace_where": "order_date >= '2026-01-01'"},
    "incremental": {
        "enabled": True,
        "field": "updated_at",
        "param_name": "updated_since",
        "checkpoint": True,
    },
}


def show_refusal():
    try:
        validate_config(REFUSED_COMBINATION)
    except ConfigError as exc:
        print(f"rejected as expected: {exc}")


# ---------------------------------------------------------------------------
# 9. Incremental *and* replacing: the rolling window
#
# These are compatible as long as `checkpoint` stays off. The pattern below
# reloads a fixed trailing window on every run — useful against a source that
# keeps editing recent records, where an incremental watermark alone would miss
# every correction made to a row it already read.
#
# The watermark is not what bounds the load here: the window is. Incremental is
# off entirely, and the window moves because it is computed at run time.
# ---------------------------------------------------------------------------


def rolling_window(days: int = 7) -> dict:
    end = date.today() + timedelta(days=1)  # include today
    start = end - timedelta(days=days)
    return {
        **BASE,
        "name": "orders_rolling",
        "target_table": "orders",
        "params": {"updated_from": start.isoformat(), "updated_to": end.isoformat()},
        "write": {
            "mode": "replace_where",
            "replace_where": (
                f"order_date >= '{start.isoformat()}' "
                f"AND order_date < '{end.isoformat()}'"
            ),
        },
    }


# ---------------------------------------------------------------------------
# Validate the whole set before running anything — a typo in `write` is caught
# here rather than after the first HTTP call.
# ---------------------------------------------------------------------------

ALL_SOURCES = [
    APPEND_IMPLICIT,
    APPEND_EXPLICIT,
    REFERENCE_TABLE,
    monthly_slice(2026, 1),
    id_range_slice(0, 100_000),
    PARTITIONED,
    rolling_window(),
]


def validate_all():
    for config in ALL_SOURCES:
        try:
            validate_config(config)
            print(f"ok   {config['name']}")
        except ConfigError as exc:
            print(f"KO   {config['name']}: {exc}")


# validate_all()
# show_refusal()
# backfill_by_month(2020, 2026)
# run_source(REFERENCE_TABLE)
