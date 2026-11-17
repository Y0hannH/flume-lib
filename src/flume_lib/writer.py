"""Écriture d'un run par lots, et avancée du watermark qui l'accompagne.

`_BatchWriter` reçoit les pages de la pagination, écrit par lots bornés
(`batch_size`) dans la table cible et commite le watermark — après chaque lot
avec `checkpoint`, à la fin du run sinon. Ordre délibéré : les données, puis
le watermark (at-least-once)."""

from dataclasses import dataclass
from datetime import datetime, timezone

from flume_lib._delta import (
    merge_records,
    read_table_types,
    write_records,
)
from flume_lib.incremental import compare
from flume_lib.validation import (
    ADVANCE_FIELD_MAX,
    ADVANCE_UPPER_BOUND,
    WRITE_APPEND,
    WRITE_MERGE,
    WRITE_OVERWRITE,
    WRITE_REPLACE_WHERE,
)
from flume_lib.watermark import write_watermark

# Colonnes de traçabilité ajoutées à chaque ligne écrite : elles permettent de
# relier une ligne au run qui l'a produite (dédoublonnage après retry partiel,
# annulation ciblée d'un run). Préfixées pour éviter toute collision avec les
# champs de l'API.
LINEAGE_RUN_ID = "_flume_run_id"


LINEAGE_RUN_ID = "_flume_run_id"
LINEAGE_INGESTED_AT = "_flume_ingested_at"


# Les modes de config se ramènent à deux modes delta-rs (append, overwrite) :
# un remplacement de fenêtre est un overwrite porteur d'un prédicat. `merge`
# ne passe pas par `write_deltalake`. Modes qui détruisent ce qu'ils
# remplacent, au premier lot seulement :
_REPLACING_MODES = (WRITE_OVERWRITE, WRITE_REPLACE_WHERE)


# Nombre de lignes tamponnées avant écriture. Borne la mémoire d'un run : elle
# ne dépend plus du volume de la source. Une source qui tient sous ce seuil
# produit un unique commit Delta, comme avant l'introduction des lots.
DEFAULT_BATCH_SIZE = 50_000


class IncrementalError(Exception):
    """Le watermark ne peut pas être calculé ou avancé sans risque."""


class EmptyResultError(Exception):
    """Le run n'a chargé aucune ligne alors que `fail_on_empty` l'interdit."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RunTarget:
    """Qui écrit, et où : l'identité d'un run et le lakehouse qui reçoit ses
    données et ses tables techniques (`watermark`, `log_runs`, `run_locks`)."""

    source_name: str
    run_id: str
    lakehouse_tables_path: str
    log_schema: str
    storage_options: dict | None


def _max_incremental_value(records: list[dict], field_name: str):
    """Plus grande valeur du champ incrémental d'un lot. Calculé *avant*
    l'écriture du lot : un champ aux types hétérogènes ferait échouer `max()`,
    et le faire après l'append laisserait des lignes écrites derrière un run
    marqué `failed`, sans watermark pour les recouvrir."""
    values = [
        r[field_name]
        for r in records
        if isinstance(r, dict) and r.get(field_name) is not None
    ]
    if not values:
        return None
    try:
        return max(values)
    except TypeError as exc:
        types = ", ".join(sorted({type(v).__name__ for v in values}))
        raise IncrementalError(
            f"incremental: field '{field_name}' mixes types ({types}) — "
            "its maximum cannot be computed"
        ) from exc


def _is_regression(candidate, reference) -> bool:
    """Vrai si `candidate` est strictement inférieur à `reference`. Deux
    valeurs incomparables ne sont pas une régression : l'hétérogénéité de type
    est déjà signalée par `_max_incremental_value`."""
    if reference is None:
        return False
    return compare(candidate, reference) == -1


def _add_lineage(records: list[dict], run_id: str) -> None:
    ingested_at = _utc_now()
    for record in records:
        record[LINEAGE_RUN_ID] = run_id
        record[LINEAGE_INGESTED_AT] = ingested_at


class _BatchWriter:
    """Écrit les enregistrements par lots bornés au lieu d'accumuler tout le
    run en mémoire pour une écriture unique.

    Deux conséquences voulues. La mémoire ne dépend plus du volume de la
    source mais de `batch_size`. Et un run qui casse à la page 900 laisse les
    899 premières écrites au lieu de tout perdre : `rows_written` dit ce qui
    est réellement dans la table, et le watermark — si `checkpoint` est actif
    — dit où reprendre.

    L'ordre écriture-puis-watermark est délibéré : il donne une sémantique
    at-least-once. Si le commit du watermark échoue après celui des données,
    le run suivant refait la fenêtre et duplique — récupérable par
    `_flume_run_id`. L'ordre inverse perdrait les lignes, définitivement.

    En mode `overwrite` ou `replace_where`, le remplacement a lieu **au
    premier lot seulement** ; les suivants s'ajoutent. Faire porter le
    prédicat à chaque lot ferait s'effacer les lots d'un même run les uns les
    autres, et un run de 300 000 lignes ne laisserait que ses 50 000
    dernières.
    """

    def __init__(
        self,
        target: RunTarget,
        uri: str,
        batch_size: int,
        incremental: dict,
        write: dict | None = None,
        stored_watermark=None,
        fail_on_empty: bool = False,
    ):
        self._target = target
        self._uri = uri
        self._batch_size = batch_size
        self._storage_options = target.storage_options

        write = write or {}
        self._write_mode = write.get("mode", WRITE_APPEND)
        self._replace_where = write.get("replace_where")
        self._partition_by = write.get("partition_by")
        self._merge_keys: list[str] = write.get("merge_keys") or []
        self._column_types = write.get("column_types")
        # Consommé par le premier lot : voir _delta_write_args.
        self._replacement_pending = self._write_mode in _REPLACING_MODES

        self._enabled = bool(incremental.get("enabled"))
        # requis par la validation quand `incremental.enabled` ; vide sinon
        self._field: str = incremental.get("field") or ""
        self._checkpoint = bool(incremental.get("checkpoint"))
        self._allow_regression = bool(incremental.get("allow_regression", False))
        self._advance = incremental.get("advance", ADVANCE_FIELD_MAX)
        self._upper = (incremental.get("window") or {}).get("upper_value")
        self._fail_on_empty = fail_on_empty

        self._buffer: list[dict] = []
        self.rows_written = 0
        # Renseignés en mode merge seulement : None dit « sans objet », 0 dit
        # « aucune ligne » — ce ne sont pas la même information.
        self.rows_inserted = 0 if self._write_mode == WRITE_MERGE else None
        self.rows_updated = 0 if self._write_mode == WRITE_MERGE else None
        # Types Arrow retenus par le premier lot : les lots suivants s'y
        # conforment, sans quoi deux lots d'un même run pourraient typer
        # différemment la même colonne et casser le commit Delta.
        self._types: dict = {}
        # Types de la table cible *avant* ce run, lus au premier lot : le
        # contrat que tout lot doit respecter. delta-rs ne le fait pas
        # respecter lui-même (voir `_conform_to_table`).
        self._table_types: dict | None = None
        self._table_types_read = False
        self.warnings: list[str] = []
        # plus grande valeur vue *dans ce run* — elle sert aussi au contrôle de
        # tri de `checkpoint`, qui ne doit pas se comparer au watermark d'un
        # run précédent : un premier lot en deçà n'est pas une source mal triée.
        self._pending = None
        # ce que la table `watermark` contient : la valeur relue au départ,
        # puis la dernière commitée. Le point de comparaison de toute écriture
        # — sans lui, un run dont le max est plus bas ramenait le watermark en
        # arrière et le run suivant rechargeait ce qui l'était déjà.
        self._written = stored_watermark

    @property
    def watermark(self):
        """Valeur actuellement en table (celle de départ si rien n'a avancé)."""
        return self._written

    def add(self, records: list[dict]) -> None:
        self._buffer.extend(records)
        while len(self._buffer) >= self._batch_size:
            batch = self._buffer[: self._batch_size]
            del self._buffer[: self._batch_size]
            self._flush(batch)

    def close(self) -> None:
        if self._buffer:
            batch, self._buffer = self._buffer, []
            self._flush(batch)
        # Avant tout commit de watermark : un run refusé pour être vide ne doit
        # pas avoir avancé la source (`upper_bound` l'aurait fait).
        if self.rows_written == 0 and self._fail_on_empty:
            raise EmptyResultError(
                "the source returned no rows and 'fail_on_empty' is set — "
                "the watermark was not advanced"
            )
        # Une source qui ne renvoie rien ne déclenche aucun lot, donc aucun
        # remplacement : la fenêtre visée garde son contenu précédent. C'est
        # délibéré — une API en panne, un filtre trop étroit ou un token sans
        # droits répondent tous "0 ligne", et vider la cible sur ce signal
        # détruirait des données sans que rien n'ait échoué. Le dire, en
        # revanche, est indispensable : l'attente naturelle est l'inverse.
        if self._replacement_pending:
            self.warnings.append(
                f"write.mode='{self._write_mode}': the source returned no "
                "rows, nothing was replaced — the target keeps its previous "
                "content"
            )
        elif self.rows_written == 0 and self._enabled:
            # Le même « 0 ligne » en append n'a rien à protéger, mais il a
            # une lecture trompeuse : un run `success` qui n'a rien chargé
            # ressemble à une source à jour. Réservé aux sources incrémentales
            # : un rechargement complet d'une table de référence vide est un
            # cas ordinaire, alors qu'ici « rien » veut dire « pas de progrès ».
            self.warnings.append(
                f"write.mode='{self._write_mode}': the source returned no "
                "rows, nothing was written"
            )
        # Hors mode checkpoint le watermark n'est commité qu'ici, une fois le
        # run complet : la reprise n'est pas offerte, mais un run interrompu
        # ne laisse jamais un watermark avancé au-delà d'une fenêtre partielle.
        # `upper_bound` est l'exception qui confirme la règle : avec
        # checkpoint, les lots avancent jusqu'au plafond, et seule la fin du
        # run peut dire que la fenêtre entière a été lue.
        if self._enabled and (not self._checkpoint or self._advance == ADVANCE_UPPER_BOUND):
            self._commit_watermark(final=True)

    def _flush(self, batch: list[dict]) -> None:
        candidate = None
        if self._enabled:
            # avant l'écriture : voir _max_incremental_value
            candidate = _max_incremental_value(batch, self._field)
            if self._checkpoint and _is_regression(candidate, self._pending):
                raise IncrementalError(
                    f"incremental: the next batch drops back to {candidate!r} "
                    f"while the watermark is already at {self._pending!r} — the "
                    f"source does not return its rows sorted by "
                    f"'{self._field}'. Resuming from that watermark would skip "
                    "rows: sort the source, or remove "
                    '"checkpoint": true.'
                )
            if (
                candidate is not None
                and self._advance != ADVANCE_FIELD_MAX
                and compare(candidate, self._upper) is None
            ):
                # même raison que le max() : avant l'écriture du lot
                raise IncrementalError(
                    f"incremental: field '{self._field}' value {candidate!r} "
                    f"cannot be compared with window.upper_value "
                    f"{self._upper!r} — \"advance\": \"{self._advance}\" needs "
                    "both on the same scale"
                )

        _add_lineage(batch, self._target.run_id)
        if self._write_mode != WRITE_MERGE and not self._table_types_read:
            # le merge relit la table à chaque lot de toute façon
            self._table_types = read_table_types(self._uri, self._storage_options)
            self._table_types_read = True
        if self._write_mode == WRITE_MERGE:
            types, fallbacks, inserted, updated = merge_records(
                self._uri,
                batch,
                self._merge_keys,
                partition_by=self._partition_by,
                storage_options=self._storage_options,
                known_types=self._types,
                column_types=self._column_types,
            )
            self.rows_inserted = (self.rows_inserted or 0) + inserted
            self.rows_updated = (self.rows_updated or 0) + updated
        else:
            types, fallbacks = write_records(
                self._uri,
                batch,
                **self._delta_write_args(),
                storage_options=self._storage_options,
                known_types=self._types,
                column_types=self._column_types,
                table_types=self._table_types,
            )
        self._types.update(types)
        for message in fallbacks:
            # une colonne dégradée l'est à chaque lot : ne le dire qu'une fois
            if message not in self.warnings:
                self.warnings.append(message)
        self.rows_written += len(batch)

        if candidate is not None and not _is_regression(candidate, self._pending):
            self._pending = candidate
        if self._checkpoint:
            self._commit_watermark()

    def _delta_write_args(self) -> dict:
        """Arguments d'écriture du lot courant. Le remplacement est un
        événement unique dans un run : il est consommé ici, et tout ce qui
        suit s'ajoute."""
        args = {"partition_by": self._partition_by}
        if not self._replacement_pending:
            return {"mode": WRITE_APPEND, **args}
        self._replacement_pending = False
        return {
            "mode": WRITE_OVERWRITE,
            "predicate": self._replace_where,
            **args,
        }

    def _watermark_target(self, final: bool):
        """Valeur que le watermark devrait prendre, ou None s'il n'a aucune
        raison de bouger. `final` : le run est complet (fin de `close`)."""
        if self._advance == ADVANCE_UPPER_BOUND and final:
            # toute la fenêtre a été lue : même vide, même sans `field`
            return self._upper
        if self._pending is None:
            return None
        if self._advance != ADVANCE_FIELD_MAX and compare(self._pending, self._upper) == 1:
            # un enregistrement au-delà de la borne envoyée (champ approximant
            # la date du filtre) ne prouve pas que la fenêtre a été lue
            # jusque-là : s'y fier ferait sauter définitivement l'intervalle
            return self._upper
        return self._pending

    def _commit_watermark(self, final: bool = False) -> None:
        target = self._watermark_target(final)
        if target is None:
            return
        if self._written is not None:
            if str(target) == str(self._written):
                return
            if not self._allow_regression:
                order = compare(target, self._written)
                if order is None:
                    message = (
                        f"incremental: the new watermark {target!r} cannot be "
                        f"compared with the stored one {self._written!r} "
                        "(different types) — it was not written. Fix the "
                        "stored value, or set \"allow_regression\": true."
                    )
                    if message not in self.warnings:
                        self.warnings.append(message)
                    return
                if order <= 0:
                    return
        write_watermark(
            self._target.lakehouse_tables_path,
            self._target.source_name,
            target,
            schema=self._target.log_schema,
            storage_options=self._storage_options,
        )
        self._written = target

