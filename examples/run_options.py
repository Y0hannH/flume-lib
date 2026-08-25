# Fabric Python notebook (non-Spark) — the `run_source` parameters themselves.
#
# The other examples are about the source *configuration*: what to call, how to
# authenticate, how to paginate. This one is about the four parameters that sit
# beside it and decide **where** the run writes, and what it does before it
# writes anything at all.
#
#   run_source(config,
#              lakehouse_tables_path=...,   which lakehouse
#              storage_options=...,         how to authenticate to storage
#              log_schema=...,              where the technical tables live
#              dry_run=...)                 write nothing, call everything
#
# None of them is required. The defaults target the notebook's own default
# lakehouse, which is what a Fabric notebook wants nine times out of ten.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.11.1

import os
from datetime import date, timedelta

from flume_lib import ConfigError, run_source, validate_config

KEYVAULT = "https://mykv.vault.azure.net"

SOURCE = {
    "name": "invoices",
    "base_url": "https://api.example.com/v1/invoices",
    "auth": {
        "type": "bearer_token",
        "token": {"keyvault_url": KEYVAULT, "secret_name": "api-token"},
    },
    "pagination": {"type": "offset", "limit": 500, "items_field": "data"},
    "target_schema": "bronze",
    "target_table": "invoices",
}


# ---------------------------------------------------------------------------
# 1. dry_run — call everything, write nothing
#
# Validates the config, resolves the credentials, performs the login if there
# is one, and really calls the data endpoint. It writes neither the data, nor
# the watermark, nor a `log_runs` row. Nothing accumulates in memory either:
# pages are counted and dropped.
#
# `RunResult.rows_loaded` counts what *would* have been written, and
# `RunResult.sample` carries the first few raw records — the fastest way to see
# what the API actually returns, as opposed to what its documentation claims.
#
# This is the smoke test to run after rotating a secret, onboarding a client,
# or editing a query. It costs one HTTP call per page and changes nothing.
# ---------------------------------------------------------------------------


def inspect(config: dict):
    result = run_source(config, dry_run=True)
    if result.status != "success":
        print(f"{result.source_name}: {result.error_message}")
        return result
    print(f"{result.source_name}: {result.rows_loaded} rows would be written")
    for record in result.sample or []:
        # The field names here are the ones that will become Delta columns.
        print(f"  fields: {sorted(record)}")
    return result


# ---------------------------------------------------------------------------
# 2. Validate a whole source list before running any of it
#
# Configuration is validated strictly: an unknown key is an error with a "did
# you mean…" suggestion, never a silent no-op. The reason is a real failure —
# a typo such as `pagintaion` used to disable pagination silently, producing a
# run reported `success` with most of the data missing.
#
# `validate_config` performs no I/O, so a list of forty sources is checked in
# milliseconds. Do it before a nightly batch rather than discovering the typo
# on source thirty-eight.
# ---------------------------------------------------------------------------


def validate_all(sources: list[dict]) -> list[dict]:
    """Returns the sources that are safe to run, reports the others."""
    ok = []
    for config in sources:
        try:
            validate_config(config)
            ok.append(config)
        except ConfigError as exc:
            print(f"{config.get('name', '<unnamed>')}: {exc}")
    return ok


# ---------------------------------------------------------------------------
# 3. lakehouse_tables_path — writing somewhere other than the default lakehouse
#
# The default is `/lakehouse/default/Tables`, the notebook's own attached
# lakehouse. In Fabric the library rewrites that path to the OneLake ABFSS URI
# of that lakehouse before writing — the local mount cannot perform the atomic
# rename a Delta commit needs, and a write through it leaves the table without
# a valid `_delta_log`. Nothing to configure; it happens on its own.
#
# To target a *different* lakehouse, pass its ABFSS URI directly. This is the
# form to use for a shared bronze lakehouse written by several workspaces.
# ---------------------------------------------------------------------------

WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"
LAKEHOUSE_ID = "11111111-1111-1111-1111-111111111111"
SHARED_TABLES = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables"
)


def run_into_shared_lakehouse():
    return run_source(SOURCE, lakehouse_tables_path=SHARED_TABLES)


# ---------------------------------------------------------------------------
# 4. storage_options — outside Fabric, or with a specific identity
#
# Passed straight through to delta-rs. Inside Fabric it can be left alone: the
# library obtains a storage token from `notebookutils` for OneLake URIs on its
# own. Anywhere else — a local run, a CI job, plain Azure storage — this is how
# the writer authenticates.
#
# Never hardcode these values. They are credentials like any other; read them
# from the environment or a vault.
# ---------------------------------------------------------------------------

ADLS_TABLES = "abfss://container@account.dfs.core.windows.net/tables"

ADLS_OPTIONS = {
    "account_name": "account",
    "client_id": os.environ.get("AZURE_CLIENT_ID", ""),
    "client_secret": os.environ.get("AZURE_CLIENT_SECRET", ""),
    "tenant_id": os.environ.get("AZURE_TENANT_ID", ""),
}


def run_outside_fabric():
    return run_source(
        SOURCE, lakehouse_tables_path=ADLS_TABLES, storage_options=ADLS_OPTIONS
    )


def run_on_a_local_folder():
    """A plain directory works too — handy to try a config end to end without
    touching a lakehouse. The tables appear as `./sandbox/bronze/invoices`."""
    return run_source(SOURCE, lakehouse_tables_path="./sandbox")


# ---------------------------------------------------------------------------
# 5. log_schema — where the technical tables live
#
# Two Delta tables are created automatically, in the `flume` schema by default:
#
#   flume.watermark   source_name, last_value, updated_ts
#   flume.log_runs    run_id, source_name, start_ts, end_ts, status,
#                     rows_loaded, error_message   (one row per run)
#
# They are the run history, not data — keeping them out of the schema analysts
# browse is the usual reason to move them. The other is isolation: two
# pipelines writing sources of the same name into one lakehouse would otherwise
# share a watermark row, and each would resume where the other left off.
#
# Change it consistently. A source whose watermark was written under `flume`
# and is then run against `flume_prod` reads no watermark, falls back to
# `initial_value`, and reloads the source from the beginning.
# ---------------------------------------------------------------------------


def run_with_isolated_history():
    return run_source(SOURCE, log_schema="flume_prod")


# ---------------------------------------------------------------------------
# 6. Reading back what the runs recorded
#
# `log_runs` is a Delta table like any other — query it with whatever the
# lakehouse offers. A failed run leaves its message there, and its `run_id` is
# what identifies the rows it wrote:
#
#   SELECT * FROM flume.log_runs
#    WHERE status = 'failed' AND start_ts >= '2026-08-01'
#    ORDER BY start_ts DESC
#
#   DELETE FROM bronze.invoices WHERE _flume_run_id = '<the failed run_id>'
#
# What `log_runs` does **not** carry is `RunResult.warnings` — a degraded
# column, or a window left unreplaced because the source was empty, is visible
# in the notebook and nowhere else. Print them, or act on them, at run time.
# ---------------------------------------------------------------------------


def run_and_report(config: dict, **kwargs):
    result = run_source(config, **kwargs)
    print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
    print(f"  run_id: {result.run_id}")
    for warning in result.warnings:
        print(f"  ! {warning}")
    if result.status == "failed":
        print(f"  {result.error_message}")
    return result


# ---------------------------------------------------------------------------
# 7. `value_format`, the two remaining shapes
#
# `value_format` constrains the watermark before it is used. It is **mandatory**
# when the value is substituted into a body (`inject: "body_template"`), because
# the value comes back from the API and lands inside a query — the constraint is
# what stops a returned `0 OR 1=1` from being interpreted. It is optional when
# the watermark goes out as a whole query param, where it cannot break out of
# anything.
#
#   any            no constraint (the default)
#   numeric        digits, sign, decimal point
#   iso_date       YYYY-MM-DD
#   iso_datetime   ISO 8601 with a time part
#
# `iso_date` is the one to use for a daily source: it also documents that the
# column has no time part, so the string comparison that advances the watermark
# behaves. `any` is honest for an opaque token that is not a date at all — a
# sequence number, a vendor-specific revision string — as long as it stays in
# the query string.
# ---------------------------------------------------------------------------

DAILY_INCREMENTAL = {
    **SOURCE,
    "name": "invoices_daily",
    "incremental": {
        "enabled": True,
        "field": "invoice_date",
        "param_name": "invoiced_since",
        # First run only: without a floor, no param is sent and the source is
        # read whole. Two years back rather than 1970 keeps that first run
        # bounded.
        "initial_value": (date.today() - timedelta(days=730)).isoformat(),
        "value_format": "iso_date",
    },
}

OPAQUE_INCREMENTAL = {
    **SOURCE,
    "name": "invoices_by_revision",
    "incremental": {
        "enabled": True,
        "field": "revision",
        "param_name": "since_revision",
        # The API's own revision string. Nothing to validate beyond it being a
        # value — hence `any`, the default, written out because the choice is
        # deliberate here rather than an omission.
        "value_format": "any",
    },
}


# ---------------------------------------------------------------------------
# Putting it together: validate, dry run, then run for real.
# ---------------------------------------------------------------------------

SOURCES = [SOURCE, DAILY_INCREMENTAL, OPAQUE_INCREMENTAL]


def nightly():
    for config in validate_all(SOURCES):
        run_and_report(config, log_schema="flume_prod")


# validate_all(SOURCES)
# inspect(SOURCE)
# nightly()
