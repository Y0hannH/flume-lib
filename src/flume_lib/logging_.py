"""Écriture des runs dans la table Delta `log_runs` (colonnes : run_id,
source_name, start_ts, end_ts, status, rows_loaded, error_message)."""

from flume_lib._delta import append_records, table_uri

LOG_RUNS_TABLE = "log_runs"


def write_log_run(
    lakehouse_tables_path: str,
    run_id: str,
    source_name: str,
    start_ts: str,
    end_ts: str,
    status: str,
    rows_loaded: int,
    error_message: str | None,
) -> None:
    uri = table_uri(lakehouse_tables_path, LOG_RUNS_TABLE)
    append_records(
        uri,
        [
            {
                "run_id": run_id,
                "source_name": source_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "status": status,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
            }
        ],
    )
