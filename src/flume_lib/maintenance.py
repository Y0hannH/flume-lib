"""Entretien : tables techniques `watermark`, `log_runs` et `run_locks`,
fichiers des tables de données, et retypage de leurs colonnes.

Chacune reçoit une ligne par run, écrite en ajout : un fichier Parquet de
quelques octets et un commit de plus à chaque fois. Au bout de quelques milliers
de runs, le journal Delta est long à relire et le dossier compte autant de petits
fichiers — ce qui ralentit **chaque** run, puisque chacun relit le watermark.
`watermark` grossit en plus de lignes devenues inutiles : seule la plus récente
de chaque source compte.

La lib n'entretient rien d'elle-même (ce serait écrire pendant un run, et deux
runs en parallèle se gêneraient). Cette fonction est à lancer **hors des runs**,
de temps en temps — une fois par semaine suffit largement.
"""

from datetime import datetime, timedelta, timezone

from flume_lib._delta import (
    DEFAULT_LAKEHOUSE_TABLES_PATH,
    DEFAULT_LOG_SCHEMA,
    compact_table,
    delete_rows,
    query_table,
    read_table_types,
    resolve_lakehouse_tables_path,
    retype_table_columns,
    sql_quote,
    table_uri,
    vacuum_table,
    validate_identifier,
)
from flume_lib.locks import LOCKS_TABLE, stamp
from flume_lib.logging_ import LOG_RUNS_TABLE
from flume_lib.watermark import WATERMARK_TABLE

# Une ligne de verrou expirée depuis plus longtemps que cela ne servira plus à
# personne : un verrou actif est par définition non expiré.
LOCK_ROWS_KEPT_FOR = timedelta(days=1)


def _prune_watermarks(uri: str, storage_options) -> int:
    """Supprime, pour chaque source, tout sauf sa ligne la plus récente — celle
    que `read_watermark` lit. Les horodatages sont du texte ISO 8601 UTC : les
    comparer comme du texte revient à les comparer comme des instants."""
    latest = query_table(
        uri,
        "select source_name, max(updated_ts) as latest from wm group by source_name",
        alias="wm",
        storage_options=storage_options,
    )
    return sum(
        delete_rows(
            uri,
            f"source_name = {sql_quote(row['source_name'])} "
            f"AND updated_ts < {sql_quote(row['latest'])}",
            storage_options=storage_options,
        )
        for row in latest
    )


def compact_technical_tables(
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    schema: str = DEFAULT_LOG_SCHEMA,
    storage_options: dict | None = None,
    vacuum: bool = False,
) -> dict[str, dict]:
    """Élague `watermark` et `run_locks`, puis regroupe les petits fichiers des
    trois tables techniques.

    - `watermark` : une ligne par source est conservée, la plus récente. Le
      watermark courant ne change pas ; l'historique de ses avancées disparaît.
    - `run_locks` : les lignes expirées depuis plus d'un jour disparaissent.
    - `log_runs` : jamais élagué — c'est l'historique des runs. Seulement compacté.
    - `vacuum=True` supprime aussi les fichiers devenus inutiles, ce qui interdit
      ensuite de remonter la table dans le temps au-delà de sept jours.

    Retourne, par table, `rows_pruned` et les métriques du compactage
    (`files_removed`, `files_added`) ; une table absente donne zéro partout.
    À lancer entre deux runs, pas pendant."""
    resolved = resolve_lakehouse_tables_path(lakehouse_tables_path)
    expired_before = stamp(datetime.now(timezone.utc) - LOCK_ROWS_KEPT_FOR)
    report = {}
    for name in (WATERMARK_TABLE, LOCKS_TABLE, LOG_RUNS_TABLE):
        uri = table_uri(resolved, schema, name)
        if name == WATERMARK_TABLE:
            pruned = _prune_watermarks(uri, storage_options)
        elif name == LOCKS_TABLE:
            pruned = delete_rows(
                uri, f"expires_ts < {sql_quote(expired_before)}", storage_options
            )
        else:
            pruned = 0
        metrics = compact_table(uri, storage_options) or {}
        report[name] = {
            "rows_pruned": pruned,
            "files_removed": int(metrics.get("numFilesRemoved", 0)),
            "files_added": int(metrics.get("numFilesAdded", 0)),
        }
        if vacuum:
            report[name]["files_vacuumed"] = vacuum_table(uri, storage_options)
    return report


def retype_columns(
    target_schema: str,
    target_table: str,
    column_types: dict[str, str],
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
) -> int:
    """Change le type de colonnes d'une table de données déjà écrite, et
    retourne le nombre de lignes réécrites.

    Pour sortir d'une table dont un premier lot a figé le mauvais type — une
    colonne de montants créée entière parce que le premier mois n'avait que des
    valeurs rondes. `column_types` est de la même forme que `write.column_types`
    (`double`, `string`, `decimal(p,s)`, `date`). Une valeur qui ne tient pas
    dans le type visé arrête tout **avant** d'écrire : un décimal qui serait
    arrondi, un entier au-delà de 2^53 vers un `double`, un texte illisible.

    La table est réécrite en un commit atomique ; l'ancienne version reste
    lisible par voyage dans le temps tant que rien n'est purgé. Réécrire coûte
    la taille de la table. À lancer entre deux runs, pas pendant : un run qui
    écrit en même temps verrait son commit entrer en conflit."""
    resolved = resolve_lakehouse_tables_path(lakehouse_tables_path)
    uri = table_uri(resolved, target_schema, target_table)
    return retype_table_columns(uri, column_types, storage_options)


# En deçà, delta-rs refuse de purger : un lecteur qui a commencé sa lecture
# avant une purge plus courte verrait ses fichiers disparaître sous ses pieds.
MIN_VACUUM_RETENTION_HOURS = 168


def _split_table(spec) -> tuple[str, str]:
    """`"bronze.orders"` ou `("bronze", "orders")` → (schéma, table), validés."""
    if isinstance(spec, str) and spec.count(".") == 1:
        schema, table = spec.split(".")
    elif isinstance(spec, (tuple, list)) and len(spec) == 2:
        schema, table = spec
    else:
        raise ValueError(
            f"{spec!r} is not a table: expected 'schema.table' or (schema, table)"
        )
    return validate_identifier(schema, "schema"), validate_identifier(table, "table")


def compact_data_tables(
    tables: list,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
    vacuum: bool = False,
    retention_hours: int = MIN_VACUUM_RETENTION_HOURS,
) -> dict[str, dict]:
    """Regroupe les petits fichiers de tables de données, et purge si on le
    demande.

    Un run écrit un commit par lot, donc au moins un fichier par lot : une
    source rechargée tous les jours accumule des milliers de petits fichiers
    que chaque lecture doit ouvrir. `replace_where` et `overwrite` laissent en
    plus sur le disque les fichiers qu'ils remplacent, tant que rien ne les
    purge. La bibliothèque ne le fait pas d'elle-même — ce serait de longues
    écritures au milieu d'un run — d'où cette fonction, à lancer entre deux
    runs.

    `tables` : des `"schema.table"` ou des `(schema, table)`. Une table absente
    est une erreur (`ValueError`, avant de toucher à quoi que ce soit) : sur une
    table de données, c'est presque toujours une faute de frappe, alors que les
    tables techniques peuvent légitimement ne pas exister encore.

    `vacuum=True` supprime les fichiers que la table n'utilise plus et que plus
    aucune lecture récente ne peut réclamer, ce qui interdit ensuite de remonter
    la table dans le temps au-delà de `retention_hours` (7 jours au minimum).

    Retourne, par table, `files_removed` et `files_added` (le compactage), et
    `files_vacuumed` si la purge a eu lieu."""
    if not isinstance(tables, (list, tuple)) or not tables:
        raise ValueError("tables must be a non-empty list of 'schema.table'")
    if (
        isinstance(retention_hours, bool)
        or not isinstance(retention_hours, int)
        or retention_hours < MIN_VACUUM_RETENTION_HOURS
    ):
        raise ValueError(
            f"retention_hours must be an integer of at least {MIN_VACUUM_RETENTION_HOURS} "
            "(7 days): Delta refuses a shorter purge"
        )
    resolved = resolve_lakehouse_tables_path(lakehouse_tables_path)
    targets = {}
    for spec in tables:
        schema, table = _split_table(spec)
        targets[f"{schema}.{table}"] = table_uri(resolved, schema, table)

    absent = [
        name for name, uri in targets.items() if read_table_types(uri, storage_options) is None
    ]
    if absent:
        raise ValueError(f"table(s) not found: {', '.join(absent)} — nothing was compacted")

    report = {}
    for name, uri in targets.items():
        metrics = compact_table(uri, storage_options) or {}
        report[name] = {
            "files_removed": int(metrics.get("numFilesRemoved", 0)),
            "files_added": int(metrics.get("numFilesAdded", 0)),
        }
        if vacuum:
            report[name]["files_vacuumed"] = vacuum_table(uri, storage_options, retention_hours)
    return report
