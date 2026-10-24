# Fabric Python notebook (non-Spark) — incremental loading, in the shapes the
# simple one does not cover: a window of dates, a source that edits its past,
# and objects that come back changed.
#
# The plain incremental block (`field` + `param_name`) assumes the API is
# filtered on a field you can see and that each row is returned once. Three
# situations break that:
#
#   1. the filter is a *window* ("from … to …") on a date the response does not
#      carry — the watermark must follow the window you asked for, not the rows
#      that came back (sections 1 and 2);
#   2. rows are recorded late with a date before the watermark (lookback, 3);
#   3. an object reappears modified — an invoice that gets paid — and must
#      replace its earlier version rather than sit beside it (merge, 4).
#
# The full reasoning is in docs/cookbook.md, section 4.3.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.20.0

from datetime import date

from flume_lib import ConfigError, run_source_windows, validate_config

KEYVAULT = "https://mykv.vault.azure.net"

AUTH = {
    "type": "bearer_token",
    "token": {"keyvault_url": KEYVAULT, "secret_name": "api-token"},
}


# ---------------------------------------------------------------------------
# 1. A window whose upper bound the watermark follows
#
# `window` says which parameter carries the upper bound and what it is;
# `advance` says what the watermark becomes at the end of a successful run:
#
#   field_max          the largest `field` seen (the default; unsafe on a window)
#   field_max_capped   min(largest `field`, upper_value) — one outlier row dated
#                      beyond the window cannot carry the watermark past a stretch
#                      nobody requested
#   upper_bound        the window's upper bound, with rows or without
#
# `upper_param` must be a key of `params` holding exactly `upper_value`:
# validation refuses a bound that is sent but not followed.
# ---------------------------------------------------------------------------


def window_config(window_end: str) -> dict:
    return {
        "name": "invoices_by_window",
        "base_url": "https://api.example.com/reports/invoices",
        "auth": AUTH,
        "target_schema": "bronze",
        "target_table": "invoices",
        "params": {"format": "json", "Date_To": window_end},
        "pagination": {"type": "none", "items_field": "Report_Entry"},
        "incremental": {
            "enabled": True,
            "field": "Last_Activity_Date",
            "param_name": "Date_From",
            "value_format": "iso_date",
            "initial_value": "2024-01-01",
            "window": {"upper_param": "Date_To", "upper_value": window_end},
            "advance": "upper_bound",
            # sections 2 and 3 below: late events, and a field that may go back
            "lookback": 7,
        },
        # section 4: an invoice comes back whenever its status changes
        "write": {
            "mode": "merge",
            "merge_keys": ["Invoice_ID"],
            "when_matched": "update",
            # amounts are exact, identifiers are text (never `double`: above 2^53 a
            # double rounds an integer), and `double` fixes a column whose first
            # batch holds only round values
            "column_types": {
                "Amount": "decimal(18,2)",
                "Margin_Ratio": "double",
                "Invoice_ID": "string",
                "Issued_On": "date",
            },
        },
        # a window that returns nothing is an anomaly for this source: fail
        # before the watermark moves instead of recording "nothing happened"
        "fail_on_empty": True,
    }


def catch_up():
    """Window after window until today. Raises `WindowRunError` — carrying the
    `RunResult`s so far in `.results` — if a run fails, if a run does not advance
    the watermark, or after `max_iterations` windows. Rerunning resumes."""
    results = run_source_windows(
        window_config, until=date.today().isoformat(), max_span_days=31
    )
    for r in results:
        print(r.window_end, r.rows_loaded, r.rows_inserted, r.rows_updated, r.warnings)


# ---------------------------------------------------------------------------
# 2. The watermark of a field that can legitimately go backwards
#
# By default the watermark only moves forward: a run whose largest `field` is
# below the stored value used to *lower* it, and the next run reloaded — and in
# append mode duplicated — everything in between. A source whose field really
# does restart (a counter reset, a re-keyed system) opts out explicitly.
#
# `normalize: "utc_iso"` reforms a timestamp the API returned in a local zone
# ("2026-08-25T14:57:44.000+02:00") into the UTC form its filter accepts.
# ---------------------------------------------------------------------------

REKEYED_COUNTER = {
    "name": "ledger_by_sequence",
    "base_url": "https://api.example.com/v1/ledger",
    "auth": AUTH,
    "target_schema": "bronze",
    "target_table": "ledger",
    "pagination": {"type": "offset", "limit": 1000, "items_field": "entries"},
    "incremental": {
        "enabled": True,
        "field": "sequence",
        "param_name": "after_sequence",
        "value_format": "numeric",
        "initial_value": "0",
        "allow_regression": True,
    },
}

LOCAL_TIMESTAMPS = {
    "name": "tickets_local_time",
    "base_url": "https://api.example.com/v1/tickets",
    "auth": AUTH,
    "target_schema": "bronze",
    "target_table": "tickets",
    "pagination": {"type": "page", "page_size": 100, "items_field": "tickets"},
    "incremental": {
        "enabled": True,
        "field": "updated_at",
        "param_name": "updated_from",
        "value_format": "iso_datetime",
        "initial_value": "2024-01-01T00:00:00Z",
        "normalize": "utc_iso",
    },
}


# ---------------------------------------------------------------------------
# 3. Reading what a run did to the watermark
#
# With an inclusive lower bound the boundary day comes back on every run, so
# `rows_loaded > 0` says nothing about progress. Compare the two positions:
# equal means the run did not advance the source.
# ---------------------------------------------------------------------------


def advanced(result) -> bool:
    return result.watermark_before != result.watermark_after


# ---------------------------------------------------------------------------
# 4. merge — one row per object, in its latest version
#
# `merge_keys` identify a row. A key that exists is updated (lineage columns
# included), a new one is inserted, and replaying the same batch changes nothing
# — which is why `merge` is the one replacing mode compatible with `checkpoint`.
# `RunResult.rows_inserted` and `rows_updated` count each; `rows_loaded` stays
# the number of rows processed.
#
# Its cost follows the size of the *table*, not of the batch: measured on a
# million rows, one batch rewrote every file. Keep tables that are merged into
# to a size that makes sense, and `compact_data_tables` them (run_options.py).
# ---------------------------------------------------------------------------

COMPOSITE_KEY = {
    "name": "invoice_lines",
    "base_url": "https://api.example.com/v1/invoice-lines",
    "auth": AUTH,
    "target_schema": "bronze",
    "target_table": "invoice_lines",
    "pagination": {"type": "offset", "limit": 500, "items_field": "lines"},
    "incremental": {
        "enabled": True,
        "field": "modified_at",
        "param_name": "modified_since",
        "value_format": "iso_datetime",
        "initial_value": "2024-01-01T00:00:00Z",
        "checkpoint": True,
    },
    "write": {"mode": "merge", "merge_keys": ["invoice_id", "line_no"]},
}


# ---------------------------------------------------------------------------
# Validate the whole set before running anything.
# ---------------------------------------------------------------------------

ALL_SOURCES = [window_config("2026-01-31"), REKEYED_COUNTER, LOCAL_TIMESTAMPS, COMPOSITE_KEY]


def validate_all():
    for config in ALL_SOURCES:
        try:
            validate_config(config)
            print(f"ok   {config['name']}")
        except ConfigError as exc:
            print(f"KO   {config['name']}: {exc}")


# validate_all()
# catch_up()
