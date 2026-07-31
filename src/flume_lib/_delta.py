"""Helpers internes d'écriture/lecture Delta en Python pur (delta-rs + arro3,
sans pyspark ni pyarrow)."""

import json

import arro3.core as ac
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError


def table_uri(lakehouse_tables_path: str, table_name: str) -> str:
    return f"{lakehouse_tables_path.rstrip('/')}/{table_name}"


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


def append_records(uri: str, records: list[dict], schema_mode: str = "merge") -> None:
    write_deltalake(uri, records_to_table(records), mode="append", schema_mode=schema_mode)


def query_table(uri: str, sql: str, alias: str = "t") -> list[dict]:
    """Exécute une requête SQL sur une table Delta. Retourne [] si la table
    n'existe pas encore."""
    try:
        dt = DeltaTable(uri)
    except TableNotFoundError:
        return []
    qb = QueryBuilder()
    qb.register(alias, dt)
    result = qb.execute(sql)
    return ac.Table.from_arrow(result).to_struct_array().to_pylist()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
