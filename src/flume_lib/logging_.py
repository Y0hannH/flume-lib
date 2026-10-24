"""Écriture des runs dans la table Delta `log_runs`.

Sept colonnes d'origine (run_id, source_name, start_ts, end_ts, status,
rows_loaded, error_message), puis celles de `LOG_RUNS_DETAIL_TYPES`, ajoutées
par `schema_mode=merge` à la première écriture qui les porte : une table créée
par une version antérieure les acquiert d'elle-même, ses anciennes lignes les
ont à NULL."""

import arro3.core as ac

from flume_lib._delta import table_uri, write_records

LOG_RUNS_TABLE = "log_runs"

# Type de chaque colonne de détail, fixé ici et non inféré. Une colonne qui ne
# vaut NULL que pour les premiers runs (`rows_inserted` hors mode merge, par
# exemple) serait typée texte par l'inférence, et ce type resterait celui de la
# table : les entiers suivants y seraient coercés en texte, sans erreur.
LOG_RUNS_DETAIL_TYPES = {
    "warnings": ac.DataType.string(),
    "pages": ac.DataType.int64(),
    "retries": ac.DataType.int64(),
    "token_renewals": ac.DataType.int64(),
    "rows_inserted": ac.DataType.int64(),
    "rows_updated": ac.DataType.int64(),
    "duration_seconds": ac.DataType.float64(),
    "watermark_before": ac.DataType.string(),
    "watermark_after": ac.DataType.string(),
    "window_end": ac.DataType.string(),
    "target_table": ac.DataType.string(),
    "write_mode": ac.DataType.string(),
    "lib_version": ac.DataType.string(),
}
_LOG_RUNS_TYPES = {
    "rows_loaded": ac.DataType.int64(),
    **LOG_RUNS_DETAIL_TYPES,
}


def write_log_run(
    lakehouse_tables_path: str,
    run_id: str,
    source_name: str,
    start_ts: str,
    end_ts: str,
    status: str,
    rows_loaded: int,
    error_message: str | None,
    schema: str = "flume",
    storage_options: dict | None = None,
    details: dict | None = None,
) -> None:
    """Ajoute la ligne d'un run. `details` porte les colonnes de
    `LOG_RUNS_DETAIL_TYPES` ; une clé absente vaut NULL, une clé inconnue est
    ignorée (elle ne doit pas ajouter de colonne à la table sans que ce
    module la type)."""
    details = details or {}
    uri = table_uri(lakehouse_tables_path, schema, LOG_RUNS_TABLE)
    write_records(
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
                **{name: details.get(name) for name in LOG_RUNS_DETAIL_TYPES},
            }
        ],
        storage_options=storage_options,
        known_types=_LOG_RUNS_TYPES,
    )
