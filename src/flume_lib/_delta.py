"""Helpers internes d'écriture/lecture Delta en Python pur (delta-rs + arro3,
sans pyspark ni pyarrow)."""

import json
import re

import arro3.core as ac
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError


ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name, kind: str) -> str:
    """Valide un nom de schéma/table avant concaténation dans un chemin :
    bloque toute traversée (../, /, caractères spéciaux) depuis la config."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"{kind} invalide : {name!r} — uniquement lettres, chiffres et "
            "underscore, sans commencer par un chiffre"
        )
    return name


def table_uri(lakehouse_tables_path: str, schema: str, table_name: str) -> str:
    """URI d'une table dans un lakehouse avec schémas : Tables/<schema>/<table>."""
    return (
        f"{lakehouse_tables_path.rstrip('/')}/"
        f"{validate_identifier(schema, 'schéma')}/"
        f"{validate_identifier(table_name, 'table')}"
    )


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
    """Type Arrow d'une colonne, déduit de **toutes** ses valeurs.

    Ne pas s'arrêter à la première non nulle est le point important : une
    colonne de montants commençant par un entier (`[1, 2.5]`) était inférée
    int64, la construction de l'Array échouait, et le repli écrivait la
    colonne entière en texte — sans le moindre signal. Un mélange
    entier/flottant est un flottant, pas du texte.
    """
    kinds = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            kinds.add("bool")
        elif isinstance(value, int):
            kinds.add("int")
        elif isinstance(value, float):
            kinds.add("float")
        else:
            # str, dict, list : rien de scalaire ne les recouvre
            return ac.DataType.string()
    if kinds == {"bool"}:
        return ac.DataType.bool()
    if kinds == {"int"}:
        return ac.DataType.int64()
    if kinds and kinds <= {"int", "float"}:
        return ac.DataType.float64()
    # colonne vide, ou booléens mêlés à des nombres
    return ac.DataType.string()


def _normalize(value, dtype: ac.DataType):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if dtype == ac.DataType.string() and not isinstance(value, str):
        return str(value)
    if dtype == ac.DataType.float64() and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    return value


def _build_column(
    name: str, values: list, known_types: dict, fallbacks: list[str]
) -> ac.Array:
    """Construit une colonne Arrow. Le type retenu pour un lot précédent prime
    sur l'inférence : deux lots d'un même run typant différemment la même
    colonne produiraient un conflit de schéma au commit Delta."""
    expected = known_types.get(name)
    dtype = expected if expected is not None else _infer_type(values)
    try:
        return ac.Array([_normalize(v, dtype) for v in values], type=dtype)
    except Exception:  # noqa: BLE001 — le type retenu ne recouvre pas les valeurs
        inferred = _infer_type(values)
        if expected is not None and inferred != expected:
            try:
                array = ac.Array(
                    [_normalize(v, inferred) for v in values], type=inferred
                )
                fallbacks.append(
                    f"colonne '{name}' : {expected} sur le lot précédent, "
                    f"{inferred} sur celui-ci — le commit Delta refusera "
                    "probablement ce changement de type"
                )
                return array
            except Exception:  # noqa: BLE001
                pass
        fallbacks.append(
            f"colonne '{name}' : valeurs non représentables par {dtype}, "
            "écrite en texte"
        )
        return ac.Array(
            [_normalize(v, ac.DataType.string()) for v in values],
            type=ac.DataType.string(),
        )


def records_to_table(
    records: list[dict], known_types: dict | None = None
) -> tuple[ac.Table, list[str]]:
    """Convertit une liste de dicts JSON en table Arrow. Types scalaires
    inférés par colonne ; les structures imbriquées sont sérialisées en JSON.

    `known_types` fixe le type de colonnes déjà écrites par un lot précédent.
    Retourne la table et la liste des dégradations subies — une colonne
    repliée sur du texte n'est plus silencieuse.
    """
    known_types = known_types or {}
    fallbacks: list[str] = []
    columns: dict[str, ac.Array] = {}
    keys: list[str] = []
    for record in records:
        for key in record:
            if key not in keys:
                keys.append(key)
    for key in keys:
        values = [r.get(key) for r in records]
        columns[key] = _build_column(key, values, known_types, fallbacks)
    return ac.Table.from_pydict(columns), fallbacks


def append_records(
    uri: str,
    records: list[dict],
    schema_mode: str = "merge",
    storage_options: dict | None = None,
    known_types: dict | None = None,
) -> tuple[dict, list[str]]:
    """Ajoute des enregistrements à une table Delta. Retourne les types Arrow
    retenus par colonne — à repasser en `known_types` pour le lot suivant du
    même run — et les dégradations de type subies."""
    table, fallbacks = records_to_table(records, known_types)
    write_deltalake(
        uri,
        table,
        mode="append",
        schema_mode=schema_mode,
        storage_options=storage_options_for(uri, storage_options),
    )
    types = {field.name: field.type for field in table.schema}
    return types, fallbacks


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
