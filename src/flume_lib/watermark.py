"""Lecture/écriture du watermark incrémental dans la table Delta `watermark`
(colonnes : source_name, last_value, updated_ts)."""

from datetime import datetime, timezone

from flume_lib._delta import query_table, sql_quote, table_uri, write_records

WATERMARK_TABLE = "watermark"


def read_watermark(
    lakehouse_tables_path: str,
    source_name: str,
    schema: str,
    storage_options: dict | None = None,
) -> str | None:
    uri = table_uri(lakehouse_tables_path, schema, WATERMARK_TABLE)
    rows = query_table(
        uri,
        "select last_value from wm "
        f"where source_name = {sql_quote(source_name)} "
        "order by updated_ts desc limit 1",
        alias="wm",
        storage_options=storage_options,
    )
    return rows[0]["last_value"] if rows else None


def write_watermark(
    lakehouse_tables_path: str,
    source_name: str,
    last_value: str,
    schema: str,
    storage_options: dict | None = None,
) -> None:
    uri = table_uri(lakehouse_tables_path, schema, WATERMARK_TABLE)
    write_records(
        uri,
        [
            {
                "source_name": source_name,
                "last_value": str(last_value),
                "updated_ts": datetime.now(timezone.utc).isoformat(),
            }
        ],
        storage_options=storage_options,
    )
