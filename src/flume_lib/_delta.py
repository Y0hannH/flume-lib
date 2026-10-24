"""Helpers internes d'écriture/lecture Delta en Python pur (delta-rs + arro3,
sans pyspark ni pyarrow)."""

import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import arro3.core as ac
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

# Défauts partagés par `run_source` et par les fonctions publiques du
# watermark : un appelant qui lit ou réinitialise un watermark doit viser la
# même table que le run, sans avoir à recopier ces deux valeurs.
DEFAULT_LAKEHOUSE_TABLES_PATH = "/lakehouse/default/Tables"
DEFAULT_LOG_SCHEMA = "flume"

# Le workspace à mettre dans l'URI est celui du lakehouse attaché, pas celui du
# notebook : les deux diffèrent dès qu'on attache un lakehouse d'un autre
# workspace, et l'URI assemblée avec `currentWorkspaceId` désigne alors un
# chemin qui n'existe pas — 404 dès la première lecture de `_delta_log`, sans
# que le token soit en cause. `currentWorkspaceId` reste le repli : il est
# correct dans le cas courant, et les clés du contexte varient selon le runtime.
WORKSPACE_CONTEXT_KEYS = ("defaultLakehouseWorkspaceId", "currentWorkspaceId")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Types qu'une config peut imposer à une colonne (`write.column_types`).
# Délibérément courts : ce sont ceux que l'inférence ne peut pas deviner —
# un montant est un décimal, pas un flottant ; une date reste du texte dans le
# JSON. `timestamp` en est absent : le cast d'arro3 vers un fuseau exige la base
# tz du système, absente de certains environnements.
_ISO_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DECIMAL_TYPE_RE = re.compile(r"^decimal\(\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)$")
COLUMN_TYPE_HELP = "decimal(precision,scale), date or string"
MAX_DECIMAL_PRECISION = 38


def parse_column_type(spec) -> tuple[str, int | None, int | None]:
    """Décode un type de colonne déclaré : (genre, précision, échelle).
    Lève ValueError si le texte n'en est pas un."""
    if isinstance(spec, str):
        text = spec.strip().lower()
        if text in ("date", "string"):
            return text, None, None
        match = _DECIMAL_TYPE_RE.match(text)
        if match:
            precision, scale = int(match.group(1)), int(match.group(2))
            if 1 <= precision <= MAX_DECIMAL_PRECISION and 0 <= scale <= precision:
                return "decimal", precision, scale
    raise ValueError(
        f"unknown column type {spec!r} — expected {COLUMN_TYPE_HELP} "
        f"(precision 1-{MAX_DECIMAL_PRECISION}, scale not above precision)"
    )


def validate_identifier(name, kind: str) -> str:
    """Valide un nom de schéma/table avant concaténation dans un chemin :
    bloque toute traversée (../, /, caractères spéciaux) depuis la config."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"invalid {kind}: {name!r} — letters, digits and underscore only, "
            "and must not start with a digit"
        )
    return name


def table_uri(lakehouse_tables_path: str, schema: str, table_name: str) -> str:
    """URI d'une table dans un lakehouse avec schémas : Tables/<schema>/<table>."""
    return (
        f"{lakehouse_tables_path.rstrip('/')}/"
        f"{validate_identifier(schema, 'schema')}/"
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


def _type_name(dtype: ac.DataType) -> str:
    """Nom lisible d'un type Arrow. `str()` d'un DataType arro3 rend
    `arro3.core.DataType<Float64>` suivi d'un retour à la ligne — le recopier
    tel quel dans un message qui finit dans `RunResult.warnings` et dans
    `log_runs` le rend illisible."""
    rendered = str(dtype).strip()
    _, _, inner = rendered.partition("<")
    return inner.rstrip(">") if inner else rendered


def _normalizer(dtype: ac.DataType):
    """Fonction de normalisation d'une colonne, décidée une fois pour toutes.

    La version par valeur comparait le type Arrow à `string()` puis à
    `float64()` pour **chaque** valeur. Ces comparaisons traversent la
    frontière Python/Rust — 0,65 µs pièce — et il y en avait deux par valeur :
    sur un lot de 50 000 lignes à 120 colonnes, elles pesaient 11 s des 13 s
    de la conversion. Le type d'une colonne ne change pas d'une valeur à
    l'autre : la question se tranche une fois par colonne.
    """
    to_string = dtype == ac.DataType.string()
    to_float = dtype == ac.DataType.float64()

    def normalize(value):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        if to_string and not isinstance(value, str):
            return str(value)
        if to_float and isinstance(value, int) and not isinstance(value, bool):
            return float(value)
        return value

    return normalize


def _declared_text(name: str, index: int, value, kind: str, precision, scale) -> str:
    """Forme texte, validée, d'une valeur destinée à un type déclaré.

    Validée en Python parce que le cast d'Arrow ne proteste pas : une valeur
    illisible, trop large ou en notation exponentielle devient un **nul**, sans
    erreur — pour un montant, une perte silencieuse. Le message donne la
    colonne, le rang de l'enregistrement dans le lot et le type Python, jamais
    la valeur : il part dans log_runs."""

    def refuse(reason: str):
        return DeltaWriteError(
            f"column '{name}' is declared {_declared_label(kind, precision, scale)} "
            f"but record {index} of the batch {reason} "
            f"(received a {type(value).__name__})"
        )

    if isinstance(value, bool) or isinstance(value, (dict, list)):
        raise refuse("holds a value that is not a scalar number or text")
    if kind == "decimal":
        try:
            number = Decimal(repr(value) if isinstance(value, float) else str(value).strip())
        except InvalidOperation:
            raise refuse("is not a decimal number") from None
        if not number.is_finite():
            raise refuse("is not a finite number")
        exponent = int(number.normalize().as_tuple().exponent)
        if -exponent > scale:
            raise refuse(f"has more than {scale} decimals — it would be rounded")
        if number.adjusted() + 1 > precision - scale and number != 0:
            raise refuse(f"does not fit in {precision - scale} integer digits")
        return format(number, "f")
    text = str(value).strip()
    if kind == "date":
        # un instant ISO (2026-01-02T03:04:05Z) donne son jour
        day = text[:10] if len(text) > 10 and text[10] in "T " else text
        # le motif d'abord : `fromisoformat` accepte « 20260102 » à partir de
        # Python 3.11 et pas avant, et un entier n'est pas une date
        if not isinstance(value, str) or not _ISO_DAY_RE.match(day):
            raise refuse("is not a yyyy-MM-dd date")
        try:
            date.fromisoformat(day)
        except ValueError:
            raise refuse("is not a yyyy-MM-dd date") from None
        return day
    return text


def _declared_label(kind: str, precision, scale) -> str:
    return f"decimal({precision},{scale})" if kind == "decimal" else kind


def _declared_array(name: str, values: list, spec: str) -> ac.Array:
    """Colonne au type imposé par la config. Un échec est une erreur, pas un
    repli sur du texte : le type a été demandé, et le dégrader en silence
    reviendrait à l'ignorer."""
    kind, precision, scale = parse_column_type(spec)
    if kind == "string":
        to_text = _normalizer(ac.DataType.string())
        return ac.Array([to_text(v) for v in values], type=ac.DataType.string())
    texts = [
        None if v is None else _declared_text(name, i, v, kind, precision, scale)
        for i, v in enumerate(values, start=1)
    ]
    source = ac.Array(texts, type=ac.DataType.string())
    if kind == "decimal" and precision is not None and scale is not None:
        target = ac.DataType.decimal128(precision, scale)
    else:
        target = ac.DataType.date32()
    converted = source.cast(target)
    if converted.null_count != source.null_count:
        # défense : la validation ci-dessus doit rendre ce cas impossible
        raise DeltaWriteError(
            f"column '{name}': the conversion to "
            f"{_declared_label(kind, precision, scale)} lost values"
        )
    return converted


def _build_column(
    name: str, values: list, known_types: dict, fallbacks: list[str]
) -> ac.Array:
    """Construit une colonne Arrow. Le type retenu pour un lot précédent prime
    sur l'inférence : deux lots d'un même run typant différemment la même
    colonne produiraient un conflit de schéma au commit Delta."""
    expected = known_types.get(name)
    dtype = expected if expected is not None else _infer_type(values)
    if expected == ac.DataType.string():
        # Une colonne figée en texte par un lot précédent absorbe sans broncher
        # les valeurs scalaires des suivants : `_normalize` les convertit, la
        # construction réussit, et personne n'apprend que la colonne de
        # montants est du texte. Le cas courant est un premier lot où la
        # colonne n'a que des nuls — elle est alors typée texte faute de
        # mieux, et la décision vaut pour tout le run.
        inferred = _infer_type(values)
        if inferred != ac.DataType.string():
            fallbacks.append(
                f"column '{name}': text according to a previous batch, but "
                f"this one holds {_type_name(inferred)} values — they are written as "
                "text. A column holding only nulls in the first batch is "
                "typed text by default, and that choice is frozen for the "
                "rest of the run."
            )
    try:
        normalize = _normalizer(dtype)
        return ac.Array([normalize(v) for v in values], type=dtype)
    except Exception:  # noqa: BLE001 — le type retenu ne recouvre pas les valeurs
        inferred = _infer_type(values)
        if expected is not None and inferred != expected:
            try:
                to_inferred = _normalizer(inferred)
                array = ac.Array(
                    [to_inferred(v) for v in values], type=inferred
                )
                fallbacks.append(
                    f"column '{name}': {_type_name(expected)} on the previous "
                    f"batch, {_type_name(inferred)} on this one — the Delta "
                    f"commit will probably "
                    "refuse this type change"
                )
                return array
            except Exception:  # noqa: BLE001
                pass
        fallbacks.append(
            f"column '{name}': values not representable as {_type_name(dtype)}, "
            "written as text"
        )
        to_text = _normalizer(ac.DataType.string())
        return ac.Array(
            [to_text(v) for v in values],
            type=ac.DataType.string(),
        )


def records_to_table(
    records: list[dict],
    known_types: dict | None = None,
    column_types: dict | None = None,
) -> tuple[ac.Table, list[str]]:
    """Convertit une liste de dicts JSON en table Arrow. Types scalaires
    inférés par colonne ; les structures imbriquées sont sérialisées en JSON.

    `known_types` fixe le type de colonnes déjà écrites par un lot précédent.
    `column_types` impose le type de colonnes nommées (`decimal(p,s)`, `date`,
    `string`) : il prime sur l'inférence et sur `known_types`, et une valeur
    qui n'y rentre pas fait échouer l'écriture.

    Retourne la table et la liste des dégradations subies — une colonne
    repliée sur du texte n'est plus silencieuse.
    """
    known_types = known_types or {}
    column_types = column_types or {}
    fallbacks: list[str] = []
    columns: dict[str, Any] = {}
    # `seen` à côté de `keys` : le `key not in keys` d'origine balayait la
    # liste pour chaque clé de chaque enregistrement, soit O(n·k²). Sur un lot
    # de 50 000 lignes, cette seule boucle coûtait 3 s à 120 colonnes, avant
    # la moindre conversion. La liste reste, elle porte l'ordre des colonnes.
    keys: list[str] = []
    seen: set = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                keys.append(key)
    for key in keys:
        values = [r.get(key) for r in records]
        if key in column_types:
            columns[key] = _declared_array(key, values, column_types[key])
        else:
            columns[key] = _build_column(key, values, known_types, fallbacks)
    return ac.Table.from_pydict(columns), fallbacks


def _schema_types(schema) -> dict:
    """Type Arrow de chaque colonne d'un schéma. Les stubs d'arro3 ne déclarent
    pas l'itération d'un `Schema`, que l'exécution supporte."""
    return {field.name: field.type for field in schema}


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
            f"replace_where: some rows being written do not satisfy predicate "
            f"{predicate!r}. delta-rs refuses the commit rather than replacing "
            "a window with rows that are not part of it — the predicate must "
            "describe exactly what the source returns, otherwise the replaced "
            f"window and the received rows diverge. Detail: {detail}"
        )
    if "No field named" in raw and predicate:
        return (
            f"replace_where: predicate {predicate!r} references a column "
            "missing from the target table. The available columns are those "
            "written by previous runs, plus the _flume_* lineage columns. "
            f"Detail: {detail}"
        )
    if "does not match table partitioning" in raw:
        return (
            f"partition_by={partition_by!r} does not match the partitioning of "
            "the existing table. Partition columns are frozen at creation: "
            "changing them requires rewriting the whole table, which the "
            f"library does not do. Detail: {detail}"
        )
    return f"Delta write refused (mode={mode}): {detail}"


def write_records(
    uri: str,
    records: list[dict],
    mode: str = "append",
    predicate: str | None = None,
    partition_by: list | None = None,
    schema_mode: str = "merge",
    storage_options: dict | None = None,
    known_types: dict | None = None,
    column_types: dict | None = None,
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
            f"predicate only makes sense with mode='overwrite', not '{mode}'"
        )
    table, fallbacks = records_to_table(records, known_types, column_types)
    try:
        # `mode` et `predicate` sont des chaînes ici ; le couple valide est
        # contrôlé plus haut, les surcharges typées de deltalake ne le savent pas
        write_deltalake(  # type: ignore[call-overload]
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
    return _schema_types(table.schema), fallbacks


def _merge_key(record: dict, merge_keys: list[str]) -> tuple:
    """Clé d'identité d'un enregistrement. Une clé absente ou nulle est une
    erreur, pas une ligne de plus : `NULL = NULL` n'est jamais vrai en SQL, une
    telle ligne ne correspondrait à rien et serait réinsérée à chaque run."""
    parts = []
    for key in merge_keys:
        value = record.get(key)
        if value is None:
            raise DeltaWriteError(
                f"merge: a record has no value for the key '{key}' "
                f"(keys: {', '.join(merge_keys)}) — a row that cannot be "
                "identified can be neither matched nor updated. Check "
                "write.merge_keys against the fields the source returns."
            )
        parts.append(
            json.dumps(value, sort_keys=True, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
        )
    return tuple(parts)


def dedupe_on_keys(records: list[dict], merge_keys: list[str]) -> list[dict]:
    """Un seul enregistrement par clé, le dernier reçu l'emportant. delta-rs
    refuse un MERGE où plusieurs lignes source correspondent à une même ligne
    cible ; une API qui renvoie deux versions d'un objet dans un même lot est
    exactement ce cas."""
    latest: dict[tuple, dict] = {}
    for record in records:
        latest[_merge_key(record, merge_keys)] = record
    return list(latest.values())


def _quote_column(name: str) -> str:
    if "`" in name:
        raise ValueError(f"invalid merge key {name!r}: a backtick cannot be quoted")
    return f"`{name}`"


def _explain_merge_error(exc: Exception, merge_keys: list[str]) -> str:
    raw = str(exc)
    detail = raw if len(raw) <= MAX_DELTA_ERROR_CHARS else raw[:MAX_DELTA_ERROR_CHARS] + "…"
    if "No field named" in raw:
        return (
            f"merge: a key among {merge_keys!r} is missing from the target "
            "table. The available columns are those written by previous runs, "
            f"plus the _flume_* lineage columns. Detail: {detail}"
        )
    return f"Delta merge refused: {detail}"


def merge_records(
    uri: str,
    records: list[dict],
    merge_keys: list[str],
    partition_by: list | None = None,
    storage_options: dict | None = None,
    known_types: dict | None = None,
    column_types: dict | None = None,
) -> tuple[dict, list[str], int, int]:
    """Fusionne des enregistrements dans une table Delta : une ligne dont la
    clé existe est mise à jour (toutes ses colonnes, lignage compris), les
    autres sont insérées. Rejouer le même lot ne change donc rien.

    Les doublons de clé *dans* le lot sont résolus avant l'envoi (le dernier
    gagne). La table est créée par une écriture ordinaire si elle n'existe pas
    encore — même chemin, mêmes types, mêmes dégradations signalées. Les
    colonnes existantes gardent le type de la table, ce qui évite à un lot
    d'entiers de se heurter à une colonne de flottants.

    Retourne les types Arrow retenus, les dégradations de type, et les nombres
    de lignes insérées et mises à jour rapportés par delta-rs."""
    # le prédicat d'abord : un nom de clé inquotable est refusé avant tout le reste
    predicate = " AND ".join(
        f"t.{_quote_column(key)} = s.{_quote_column(key)}" for key in merge_keys
    )
    records = dedupe_on_keys(records, merge_keys)
    try:
        dt = DeltaTable(uri, storage_options=storage_options_for(uri, storage_options))
    except TableNotFoundError:
        types, fallbacks = write_records(
            uri,
            records,
            partition_by=partition_by,
            storage_options=storage_options,
            known_types=known_types,
            column_types=column_types,
        )
        return types, fallbacks, len(records), 0

    target_types = _schema_types(dt.schema().to_arrow())
    table, fallbacks = records_to_table(
        records, {**(known_types or {}), **target_types}, column_types
    )
    try:
        metrics = (
            dt.merge(
                source=table,
                predicate=predicate,
                source_alias="s",
                target_alias="t",
                merge_schema=True,
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception as exc:  # noqa: BLE001 — retraduit, puis relevé
        raise DeltaWriteError(_explain_merge_error(exc, merge_keys)) from exc
    return (
        _schema_types(table.schema),
        fallbacks,
        int(metrics.get("num_target_rows_inserted", 0)),
        int(metrics.get("num_target_rows_updated", 0)),
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


def delete_rows(uri: str, predicate: str, storage_options: dict | None = None) -> int:
    """Supprime les lignes qui satisfont `predicate`. Retourne leur nombre, 0 si
    la table n'existe pas. Le prédicat est obligatoire : un `delete()` sans
    argument vide la table pour tout le monde."""
    if not predicate or not predicate.strip():
        raise ValueError("delete_rows: a predicate is required")
    try:
        dt = DeltaTable(uri, storage_options=storage_options_for(uri, storage_options))
    except TableNotFoundError:
        return 0
    metrics = dt.delete(predicate)
    return int(metrics.get("num_deleted_rows", 0))


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
