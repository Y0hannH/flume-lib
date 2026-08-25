"""Helpers internes d'écriture/lecture Delta en Python pur (delta-rs + arro3,
sans pyspark ni pyarrow)."""

import json
import re

import arro3.core as ac
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

# Le workspace à mettre dans l'URI est celui du lakehouse attaché, pas celui du
# notebook : les deux diffèrent dès qu'on attache un lakehouse d'un autre
# workspace, et l'URI assemblée avec `currentWorkspaceId` désigne alors un
# chemin qui n'existe pas — 404 dès la première lecture de `_delta_log`, sans
# que le token soit en cause. `currentWorkspaceId` reste le repli : il est
# correct dans le cas courant, et les clés du contexte varient selon le runtime.
WORKSPACE_CONTEXT_KEYS = ("defaultLakehouseWorkspaceId", "currentWorkspaceId")

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
    requis par le commit du transaction log delta-rs (os error 1).

    Le lakehouse par défaut peut vivre dans un autre workspace que le notebook ;
    l'URI porte le workspace du lakehouse (voir WORKSPACE_CONTEXT_KEYS). Si le
    contexte ne l'expose pas, le repli sur le workspace du notebook produira un
    chemin inexistant — passer `lakehouse_tables_path` explicitement."""
    if not path.startswith("/lakehouse/default/"):
        return path
    try:
        import notebookutils  # préinstallé dans les notebooks Fabric

        context = notebookutils.runtime.context
        workspace_id = next(
            (context.get(key) for key in WORKSPACE_CONTEXT_KEYS if context.get(key)),
            None,
        )
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


class DeltaWriteError(Exception):
    """Écriture Delta refusée. Le message de delta-rs est conservé, précédé de
    ce qu'il signifie pour la config qui l'a provoqué."""


# Bornes du message d'origine recopié dans l'explication : il finit dans
# log_runs, et delta-rs joint volontiers un aperçu tabulaire des lignes fautives.
MAX_DELTA_ERROR_CHARS = 300


def _explain_write_error(exc: Exception, mode: str, predicate: str | None,
                         partition_by: list | None) -> str:
    """Traduit les refus de delta-rs qu'une config peut réellement provoquer.
    Les autres passent tels quels — inventer une explication à une erreur
    qu'on n'a pas reconnue serait pire que de la recopier."""
    raw = str(exc)
    detail = raw if len(raw) <= MAX_DELTA_ERROR_CHARS else raw[:MAX_DELTA_ERROR_CHARS] + "…"

    if "failed validation check" in raw and predicate:
        return (
            f"replace_where : des lignes écrites ne satisfont pas le prédicat "
            f"{predicate!r}. delta-rs refuse le commit plutôt que de remplacer "
            "une fenêtre par des lignes qui n'en font pas partie — le prédicat "
            "doit décrire exactement ce que la source renvoie, sans quoi la "
            f"fenêtre remplacée et les lignes reçues divergent. Détail : {detail}"
        )
    if "No field named" in raw and predicate:
        return (
            f"replace_where : le prédicat {predicate!r} référence une colonne "
            "absente de la table cible. Les colonnes disponibles sont celles "
            "écrites par les runs précédents, plus les colonnes de traçabilité "
            f"_flume_*. Détail : {detail}"
        )
    if "does not match table partitioning" in raw:
        return (
            f"partition_by={partition_by!r} ne correspond pas au partitionnement "
            "de la table existante. Les colonnes de partition sont figées à la "
            "création : les changer impose de réécrire la table entière, ce que "
            f"la lib ne fait pas. Détail : {detail}"
        )
    return f"écriture Delta refusée (mode={mode}) : {detail}"


def write_records(
    uri: str,
    records: list[dict],
    mode: str = "append",
    predicate: str | None = None,
    partition_by: list | None = None,
    schema_mode: str = "merge",
    storage_options: dict | None = None,
    known_types: dict | None = None,
) -> tuple[dict, list[str]]:
    """Écrit des enregistrements dans une table Delta.

    `mode="append"` ajoute. `mode="overwrite"` remplace : la table entière, ou
    seulement les lignes qui satisfont `predicate` (le `replaceWhere` de
    Delta). Un `predicate` sur une fenêtre absente de la table écrit sans rien
    supprimer — un backfill d'une fenêtre neuve et le rejeu d'une fenêtre déjà
    chargée empruntent donc le même chemin.

    Retourne les types Arrow retenus par colonne — à repasser en `known_types`
    pour le lot suivant du même run — et les dégradations de type subies.
    """
    if predicate is not None and mode != "overwrite":
        raise ValueError(
            f"predicate n'a de sens qu'avec mode='overwrite', pas '{mode}'"
        )
    table, fallbacks = records_to_table(records, known_types)
    try:
        write_deltalake(
            uri,
            table,
            mode=mode,
            predicate=predicate,
            partition_by=partition_by,
            schema_mode=schema_mode,
            storage_options=storage_options_for(uri, storage_options),
        )
    except Exception as exc:  # noqa: BLE001 — retraduit, puis relevé
        raise DeltaWriteError(
            _explain_write_error(exc, mode, predicate, partition_by)
        ) from exc
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
