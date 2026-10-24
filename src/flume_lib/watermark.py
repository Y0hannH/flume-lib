"""Lecture/écriture du watermark incrémental dans la table Delta `watermark`
(colonnes : source_name, last_value, updated_ts).

Les trois fonctions sont publiques (`from flume_lib import read_watermark…`).
Elles visent par défaut la même table que `run_source` : même chemin, résolu
de la même façon dans Fabric, même schéma. `source_name` n'a pas de défaut,
mais il reste après `lakehouse_tables_path` dans la signature : l'ordre
historique `(chemin, source, …)` est conservé pour ne casser aucun appel
positionnel, ce qui interdit de donner un défaut au premier seul — d'où le
contrôle explicite plus bas. Appeler par mots-clés : `read_watermark(
source_name="s")`."""

from datetime import datetime, timezone

from flume_lib._delta import (
    DEFAULT_LAKEHOUSE_TABLES_PATH,
    DEFAULT_LOG_SCHEMA,
    delete_rows,
    query_table,
    resolve_lakehouse_tables_path,
    sql_quote,
    table_uri,
    write_records,
)

WATERMARK_TABLE = "watermark"


def _watermark_uri(lakehouse_tables_path: str, schema: str) -> str:
    # idempotent : un chemin déjà résolu (ou non Fabric) ressort inchangé
    resolved = resolve_lakehouse_tables_path(lakehouse_tables_path)
    return table_uri(resolved, schema, WATERMARK_TABLE)


def _require(name: str, value: str | None) -> str:
    if value is None:
        raise TypeError(f"{name} is required")
    return value


def read_watermark(
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    source_name: str | None = None,
    schema: str = DEFAULT_LOG_SCHEMA,
    storage_options: dict | None = None,
) -> str | None:
    """Watermark courant d'une source, ou None s'il n'y en a pas."""
    source_name = _require("source_name", source_name)
    uri = _watermark_uri(lakehouse_tables_path, schema)
    rows = query_table(
        uri,
        # la valeur est citée par `sql_quote` ; le reste est une constante
        "select last_value from wm "  # nosec B608
        f"where source_name = {sql_quote(source_name)} "
        "order by updated_ts desc limit 1",
        alias="wm",
        storage_options=storage_options,
    )
    return rows[0]["last_value"] if rows else None


def write_watermark(
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    source_name: str | None = None,
    last_value: str | None = None,
    schema: str = DEFAULT_LOG_SCHEMA,
    storage_options: dict | None = None,
) -> None:
    """Ajoute une ligne : le watermark courant est la plus récente par source.
    L'ajout, plutôt qu'un upsert, laisse deux sources s'écrire en parallèle
    sans conflit de commit — au prix d'une table qui grossit, voir
    `reset_watermark` pour repartir d'une page blanche."""
    _require("source_name", source_name)
    _require("last_value", last_value)
    uri = _watermark_uri(lakehouse_tables_path, schema)
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


def reset_watermark(
    source_name: str,
    schema: str = DEFAULT_LOG_SCHEMA,
    storage_options: dict | None = None,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
) -> int:
    """Oublie le watermark d'une source : le run suivant repart de
    `incremental.initial_value`. Retourne le nombre de lignes supprimées (0 si
    la source n'en avait pas, ou si la table n'existe pas encore).

    Ne touche qu'aux lignes de `source_name` — jamais à celles des autres
    sources. Les données déjà chargées restent en place : un rerun les
    recharge. À exécuter entre deux runs, pas pendant un."""
    if not isinstance(source_name, str) or not source_name:
        raise ValueError("reset_watermark: source_name must be a non-empty string")
    uri = _watermark_uri(lakehouse_tables_path, schema)
    return delete_rows(
        uri, f"source_name = {sql_quote(source_name)}", storage_options=storage_options
    )
