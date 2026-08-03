"""Helpers internes d'écriture/lecture Delta en Python pur (delta-rs + arro3,
sans pyspark ni pyarrow)."""

import json

import arro3.core as ac
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError


ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"


def table_uri(lakehouse_tables_path: str, schema: str, table_name: str) -> str:
    """URI d'une table dans un lakehouse avec schémas : Tables/<schema>/<table>."""
    return f"{lakehouse_tables_path.rstrip('/')}/{schema}/{table_name}"


def resolve_lakehouse_tables_path(path: str) -> str:
    """Dans Fabric, convertit le chemin local du lakehouse par défaut vers son
    URI ABFSS OneLake : le montage local ne supporte pas le rename atomique
    requis par le commit du transaction log delta-rs (os error 1)."""
    if not path.startswith("/lakehouse/default/"):
        return path
    try:
        import notebookutils  # préinstallé dans les notebooks Fabric

        context = notebookutils.runtime.context
        workspace_id = context.get("currentWorkspaceId")
        lakehouse_id = context.get("defaultLakehouseId")
        if workspace_id and lakehouse_id:
            suffix = path.removeprefix("/lakehouse/default/").strip("/")
            return f"abfss://{workspace_id}@{ONELAKE_HOST}/{lakehouse_id}/{suffix}"
    except Exception:  # noqa: BLE001 — hors Fabric ou contexte indisponible
        pass
    return path


def _fabric_storage_options() -> dict | None:
    try:
        import notebookutils

        token = notebookutils.credentials.getToken("storage")
        return {"bearer_token": token, "use_fabric_endpoint": "true"}
    except Exception:  # noqa: BLE001
        return None


def storage_options_for(uri: str, storage_options: dict | None) -> dict | None:
    """Options de stockage passées à delta-rs : celles fournies par l'appelant,
    ou, pour une URI OneLake dans Fabric, un bearer token obtenu au runtime."""
    if storage_options:
        return storage_options
    if uri.startswith("abfss://"):
        return _fabric_storage_options()
    return None


def _infer_type(values: list) -> ac.DataType:
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return ac.DataType.bool()
        if isinstance(v, int):
            return ac.DataType.int64()
        if isinstance(v, float):
            return ac.DataType.float64()
        return ac.DataType.string()
    return ac.DataType.string()


def _normalize(value, dtype: ac.DataType):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if dtype == ac.DataType.string() and not isinstance(value, str):
        return str(value)
    return value


def records_to_table(records: list[dict]) -> ac.Table:
    """Convertit une liste de dicts JSON en table Arrow. Types scalaires
    inférés par colonne ; les structures imbriquées sont sérialisées en JSON."""
    columns: dict[str, ac.Array] = {}
    keys: list[str] = []
    for record in records:
        for key in record:
            if key not in keys:
                keys.append(key)
    for key in keys:
        values = [r.get(key) for r in records]
        if any(isinstance(v, (dict, list)) for v in values):
            dtype = ac.DataType.string()
        else:
            dtype = _infer_type(values)
        normalized = [_normalize(v, dtype) for v in values]
        try:
            columns[key] = ac.Array(normalized, type=dtype)
        except Exception:
            # types hétérogènes dans la colonne : repli sur string
            columns[key] = ac.Array(
                [None if v is None else str(v) for v in normalized],
                type=ac.DataType.string(),
            )
    return ac.Table.from_pydict(columns)


def append_records(
    uri: str,
    records: list[dict],
    schema_mode: str = "merge",
    storage_options: dict | None = None,
) -> None:
    write_deltalake(
        uri,
        records_to_table(records),
        mode="append",
        schema_mode=schema_mode,
        storage_options=storage_options_for(uri, storage_options),
    )


def query_table(
    uri: str, sql: str, alias: str = "t", storage_options: dict | None = None
) -> list[dict]:
    """Exécute une requête SQL sur une table Delta. Retourne [] si la table
    n'existe pas encore."""
    try:
        dt = DeltaTable(uri, storage_options=storage_options_for(uri, storage_options))
    except TableNotFoundError:
        return []
    qb = QueryBuilder()
    qb.register(alias, dt)
    result = qb.execute(sql)
    return ac.Table.from_arrow(result).to_struct_array().to_pylist()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
