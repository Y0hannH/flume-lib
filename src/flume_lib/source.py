"""Point d'entrée unique : run_source(config) -> RunResult. Ne lève jamais
d'exception vers l'appelant."""

import time
import uuid
from dataclasses import dataclass, field
from typing import Literal

from flume_lib._delta import (
    DEFAULT_LAKEHOUSE_TABLES_PATH,
    DEFAULT_LOG_SCHEMA,
    resolve_lakehouse_tables_path,
    table_uri,
)
from flume_lib.http_ import PageFetcher, _build_fetch_page
from flume_lib.incremental import compare, shift_days
from flume_lib.locks import RunLock
from flume_lib.logging_ import write_log_run
from flume_lib.pagination import paginate
from flume_lib.secrets_ import redact
from flume_lib.templating import (
    check_value,
    normalize_value,
    render,
)
from flume_lib.validation import (
    WRITE_APPEND,
    ConfigError,
    validate_config,
)
from flume_lib.watermark import read_watermark
from flume_lib.writer import (
    DEFAULT_BATCH_SIZE,
    IncrementalError,
    RunTarget,
    _BatchWriter,
    _utc_now,
)

DRY_RUN_SAMPLE_SIZE = 3


# Préfixe du montage local du lakehouse par défaut : delta-rs ne peut pas y
# commiter. `resolve_lakehouse_tables_path` le remplace par une URI ABFSS ; s'il
# est encore là après la résolution, la résolution a échoué.
LOCAL_LAKEHOUSE_PREFIX = "/lakehouse/default/"


# Plafond du message d'erreur d'un run, tous contenus confondus. Les bornes
# ci-dessus limitent chaque source de texte distante ; celle-ci limite la somme,
# qui part dans log_runs (lisible par tout le lakehouse) et dans RunResult.
MAX_RUN_MESSAGE_CHARS = 2000


@dataclass
class RunResult:
    source_name: str
    status: Literal["success", "failed"]
    rows_loaded: int
    error_message: str | None
    start_ts: str
    end_ts: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    # Renseigné uniquement en dry-run : premiers enregistrements bruts reçus
    sample: list | None = None
    # Dégradations subies sans faire échouer le run — typiquement une colonne
    # dont les valeurs ne rentrent pas dans le type déduit et qui finit en
    # texte. Un run `success` peut en porter : c'est tout l'intérêt.
    warnings: list = field(default_factory=list)
    # Position du watermark autour du run, en texte : celle d'où le run est
    # parti (relue, ou `initial_value` au premier run) et celle d'où le
    # suivant partira. Égales, le run n'a pas fait avancer la source — avec une
    # borne basse inclusive, `rows_loaded > 0` ne dit pas le contraire. None
    # sans `incremental`.
    watermark_before: str | None = None
    watermark_after: str | None = None
    # Borne haute envoyée à l'API (`incremental.window.upper_value`), si la
    # source est lue par fenêtre.
    window_end: str | None = None
    # Renseignés en mode `merge` uniquement ; `rows_loaded` reste le nombre
    # de lignes traitées.
    rows_inserted: int | None = None
    rows_updated: int | None = None
    # Ce que le run a coûté. `pages` : réponses de données reçues, la dernière
    # (vide) comprise. `retries` : requêtes rejouées — erreur réseau, 429, 5xx,
    # erreur applicative annoncée transitoire, rejeu après un 401.
    # `token_renewals` : tokens obtenus de nouveau après le premier.
    pages: int = 0
    retries: int = 0
    token_renewals: int = 0
    duration_seconds: float = 0.0


def _publishable(message: str) -> str:
    """Message prêt à quitter la lib : valeurs secrètes masquées, puis borné.

    Le masquage passe **avant** la troncature : couper un secret en deux
    laisserait un fragment que plus aucune valeur connue ne reconnaît. Il
    couvre ce que les règles sur la forme des messages ne peuvent pas voir —
    une API qui recopie un credential dans le corps d'une erreur, ou un secret
    placé dans le chemin d'une URL. Seules les valeurs que la lib a elle-même
    résolues (variable d'environnement, Key Vault, token obtenu) sont connues."""
    text = redact(str(message))
    if len(text) > MAX_RUN_MESSAGE_CHARS:
        return text[: MAX_RUN_MESSAGE_CHARS - 1] + "…"
    return text


def _check_window_ahead(upper_value, last_value) -> None:
    """La borne haute d'une fenêtre ne peut pas précéder le point de départ :
    la requête serait vide ou incohérente, et — avec `upper_bound` — le run
    ferait reculer le watermark vers une valeur qu'il a déjà dépassée."""
    if last_value is None:
        return
    order = compare(upper_value, last_value)
    if order is None:
        raise ConfigError(
            f"incremental.window: upper_value {upper_value!r} cannot be "
            f"compared with the watermark {last_value!r} — they must be on "
            "the same scale"
        )
    if order < 0:
        raise ConfigError(
            f"incremental.window: upper_value {upper_value!r} is before the "
            f"watermark {last_value!r} — the window would be empty"
        )


def _apply_lookback(watermark, days: int) -> str:
    try:
        return shift_days(str(watermark), days)
    except ValueError as exc:
        raise IncrementalError(f"incremental.lookback: {exc}") from exc


@dataclass
class _Progress:
    """Ce qu'un run a réellement accompli, tenu à jour au fil de l'eau.

    Un run qui casse à la page 900 doit rendre ce qu'il a fait des 899
    premières, pas des zéros : chaque étape écrit ici avant de risquer
    d'échouer, et `run_source` lit le résultat même quand une exception est
    passée par là."""

    rows_loaded: int = 0
    sample: list | None = None
    warnings: list = field(default_factory=list)
    stored_watermark: object = None
    watermark_before: str | None = None
    watermark_after: str | None = None
    window_end: str | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None


class _Reporter:
    """Fait suivre au callback `on_progress` le déroulé d'un run : après chaque
    page, le nombre de pages reçues et de lignes lues jusque-là.

    Un callback qui lève ne fait pas échouer le run — son rôle est de montrer,
    pas de décider : il est abandonné, et le run le dit une fois dans
    `warnings`. (Une interruption, `KeyboardInterrupt`, passe : c'est ainsi
    qu'un notebook annulé depuis le callback s'arrête.)"""

    def __init__(self, callback, stats: dict, warnings: list):
        self._callback = callback
        self._stats = stats
        self._warnings = warnings
        self.rows = 0

    def page(self, rows: int) -> None:
        self.rows += rows
        if self._callback is None:
            return
        try:
            self._callback(self._stats["pages"], self.rows)
        except Exception as exc:  # noqa: BLE001 — voir la docstring
            self._callback = None
            self._warnings.append(
                f"on_progress raised {type(exc).__name__} and was disabled for the "
                "rest of the run"
            )


def _lakehouse_path_warning(path) -> str | None:
    """Avertissement si le chemin du lakehouse n'a pas été résolu vers une URI
    ABFSS : le montage local ne peut pas porter le commit Delta."""
    if isinstance(path, str) and path.startswith(LOCAL_LAKEHOUSE_PREFIX):
        return (
            f"lakehouse path '{path}' was not resolved to an "
            "ABFSS URI (outside Fabric, or the notebook context does not expose "
            "the default lakehouse). The local mount does not support the Delta "
            "commit: writes will probably fail with 'os error 1'. Pass "
            "lakehouse_tables_path explicitly (abfss://…)."
        )
    return None


def _prepare_incremental(
    incremental: dict,
    params: dict,
    variables: dict,
    progress: _Progress,
    target: RunTarget,
) -> None:
    """Relit le watermark et le place dans la requête : en query string
    (`params`) ou dans les placeholders du corps (`variables`). Renseigne
    `progress` à mesure — la borne haute est connue avant que le point de
    départ puisse faire échouer le run, et le résultat d'un run échoué la
    porte."""
    if not incremental.get("enabled"):
        return
    stored_watermark = read_watermark(
        target.lakehouse_tables_path,
        target.source_name,
        schema=target.log_schema,
        storage_options=target.storage_options,
    )
    progress.stored_watermark = stored_watermark
    last_value = stored_watermark
    if last_value is None:
        # Premier run : sans plancher, un placeholder resterait non
        # substitué et partirait tel quel dans la requête.
        last_value = incremental.get("initial_value")
    window = incremental.get("window")
    if window:
        progress.window_end = str(window["upper_value"])
        _check_window_ahead(window["upper_value"], last_value)
    if last_value is None:
        return
    # la position de départ, avant tout décalage ou reformatage :
    # c'est celle que le watermark doit dépasser pour avoir avancé
    progress.watermark_before = progress.watermark_after = str(last_value)
    lookback = incremental.get("lookback")
    if lookback and stored_watermark is not None:
        # Seule la valeur *envoyée* recule : le watermark stocké
        # reste celui du dernier run. `initial_value` est exclu —
        # c'est un plancher que l'utilisateur a fixé, pas un
        # watermark à recouvrir.
        last_value = _apply_lookback(stored_watermark, lookback)
    # Avant toute injection, et donc aussi bien pour la valeur
    # relue que pour 'initial_value' : les deux doivent partir
    # sous la même forme, sans quoi le premier run essaierait un
    # format et les suivants un autre.
    last_value = normalize_value(
        last_value,
        incremental.get("normalize", "none"),
        label="incremental: watermark",
    )
    if incremental.get("inject", "query_param") == "body_template":
        placeholder = incremental.get("placeholder", "watermark")
        variables[placeholder] = check_value(
            last_value,
            incremental.get("value_format", "any"),
            label=f"incremental: watermark '{placeholder}'",
        )
    else:
        params[incremental["param_name"]] = last_value


def _run_dry(pages, progress: _Progress, reporter: _Reporter) -> None:
    """Dry run : lit tout, compte, garde les premiers enregistrements."""
    progress.sample = []
    for page in pages:
        if len(progress.sample) < DRY_RUN_SAMPLE_SIZE:
            progress.sample.extend(page[: DRY_RUN_SAMPLE_SIZE - len(progress.sample)])
        progress.rows_loaded += len(page)
        reporter.page(len(page))


def _run_write(
    pages,
    config: dict,
    incremental: dict,
    progress: _Progress,
    target: RunTarget,
    reporter: _Reporter,
) -> None:
    """Écrit les pages par lots, puis solde le watermark."""
    writer = _BatchWriter(
        target,
        uri=table_uri(
            target.lakehouse_tables_path,
            config["target_schema"],
            config["target_table"],
        ),
        batch_size=config.get("batch_size", DEFAULT_BATCH_SIZE),
        incremental=incremental,
        write=config.get("write"),
        stored_watermark=progress.stored_watermark,
        fail_on_empty=bool(config.get("fail_on_empty", False)),
    )
    try:
        for page in pages:
            # la page est comptée *avant* son écriture : `rows` dit ce que la
            # source a donné, `rows_loaded` ce qui est dans la table
            reporter.page(len(page))
            writer.add(page)
        writer.close()
    finally:
        # Ce que l'appelant voit doit être ce qui est réellement dans
        # la table, y compris quand le run casse en cours de route.
        progress.rows_loaded = writer.rows_written
        progress.rows_inserted = writer.rows_inserted
        progress.rows_updated = writer.rows_updated
        if writer.watermark is not None:
            progress.watermark_after = str(writer.watermark)
        # extend, pas une affectation : celles de la pagination sont
        # déjà dans la liste
        progress.warnings.extend(writer.warnings)


def _release_lock(lock: RunLock, progress: _Progress) -> None:
    """Libère le verrou du run, quoi qu'il soit arrivé. Un échec n'est pas
    levé — il masquerait la cause du run — mais dit : le verrou tombera à son
    échéance (`lock_ttl_seconds`)."""
    try:
        lock.release()
    except Exception as exc:  # noqa: BLE001
        progress.warnings.append(
            f"the run lock could not be released ({type(exc).__name__}: {exc}); it "
            "expires on its own after lock_ttl_seconds"
        )


def _run_details(result: RunResult, config) -> dict:
    """Colonnes de détail de la ligne `log_runs` (voir `LOG_RUNS_DETAIL_TYPES`)."""
    import flume_lib  # à l'appel : le paquet est importé en entier à ce stade

    config = config if isinstance(config, dict) else {}
    target = None
    if config.get("target_schema") and config.get("target_table"):
        target = f"{config['target_schema']}.{config['target_table']}"
    joined = " | ".join(result.warnings)
    return {
        "warnings": (
            joined[: MAX_RUN_MESSAGE_CHARS - 1] + "…"
            if len(joined) > MAX_RUN_MESSAGE_CHARS
            else joined or None
        ),
        "pages": result.pages,
        "retries": result.retries,
        "token_renewals": result.token_renewals,
        "rows_inserted": result.rows_inserted,
        "rows_updated": result.rows_updated,
        "duration_seconds": result.duration_seconds,
        "watermark_before": result.watermark_before,
        "watermark_after": result.watermark_after,
        "window_end": result.window_end,
        "target_table": target,
        "write_mode": (config.get("write") or {}).get("mode", WRITE_APPEND),
        "lib_version": flume_lib.__version__,
    }


def _persist_run(result: RunResult, target: RunTarget, config=None) -> None:
    """Écrit la ligne de `log_runs`. Un échec n'est pas levé : il s'ajoute au
    message du résultat, qui reste la source de vérité de l'appelant."""
    try:
        write_log_run(
            target.lakehouse_tables_path,
            run_id=result.run_id,
            source_name=result.source_name,
            start_ts=result.start_ts,
            end_ts=result.end_ts,
            status=result.status,
            rows_loaded=result.rows_loaded,
            error_message=result.error_message,
            schema=target.log_schema,
            storage_options=target.storage_options,
            details=_run_details(result, config),
        )
    except Exception as exc:  # noqa: BLE001
        log_error = f"log_runs write failed — {type(exc).__name__}: {exc}"
        result.error_message = _publishable(
            f"{result.error_message} | {log_error}" if result.error_message else log_error
        )


def run_source(
    config: dict,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
    log_schema: str = DEFAULT_LOG_SCHEMA,
    dry_run: bool = False,
    on_progress=None,
) -> RunResult:
    """Exécute l'ingestion d'une source d'après sa config. Toute erreur est
    catchée et remontée dans RunResult, jamais levée vers l'appelant.

    Les headers fixes de config['headers'] sont ajoutés à chaque appel, sans
    jamais écraser ceux de l'auth. Quand incremental.inject vaut
    'body_template', le watermark est interpolé dans les placeholders {nom} de
    config['body'] au lieu d'être ajouté en query string — indispensable pour
    les APIs SQL-over-REST où le filtre vit dans la requête elle-même.
    config['template_paths'] restreint ce templating à certaines branches du
    corps, indispensable quand celui-ci contient déjà des accolades (GraphQL).

    config['errors'] déclare l'enveloppe d'erreur applicative des APIs qui
    répondent 200 avec l'erreur dans le corps — sans elle, un tel run finirait
    'success' avec 0 ligne et aucun message.

    Les enregistrements sont écrits par lots de config['batch_size'] lignes
    (défaut 50 000) : la mémoire d'un run ne dépend plus du volume de la
    source. RunResult.rows_loaded compte les lignes réellement écrites, y
    compris quand le run échoue en cours de route. Avec
    incremental.checkpoint, le watermark est commité après chaque lot et un
    run interrompu reprend là où il s'est arrêté — à condition que la source
    renvoie ses lignes triées par incremental.field, ce que la lib vérifie.

    config['write'] pilote la façon dont la table cible est écrite. Par
    défaut les lignes s'ajoutent. "mode": "replace_where" avec un prédicat
    remplace la fenêtre qu'il décrit au lieu de s'y ajouter : un backfill
    devient rejouable, là où le rejouer en append duplique. "mode":
    "overwrite" remplace la table entière — un rechargement complet de table
    de référence. Dans les deux cas, une source qui ne renvoie aucune ligne
    ne remplace rien : la cible garde son contenu et RunResult.warnings le
    dit. "mode": "merge" avec `merge_keys` met à jour les lignes dont la clé
    existe et insère les autres : un objet qui revient modifié n'existe
    qu'une fois, dans sa dernière version.

    Le watermark n'avance jamais en arrière (`incremental.allow_regression`
    pour l'autoriser). Pour une API filtrée par fenêtre de dates,
    `incremental.window` et `advance` le font suivre la fenêtre demandée plutôt
    que les données reçues, et `lookback` rouvre une marge ; RunResult dit où
    le run est parti (`watermark_before`) et où le suivant partira
    (`watermark_after`). `run_source_windows` enchaîne les fenêtres.

    Cible exclusivement des lakehouses avec schémas : les données vont dans
    config['target_schema'] (requis), les tables techniques watermark et
    log_runs dans log_schema (défaut : 'flume'). Chaque ligne écrite porte
    les colonnes de traçabilité _flume_run_id et _flume_ingested_at.

    dry_run=True valide la config, appelle réellement l'API (donc vérifie
    aussi les credentials) et compte les lignes, mais n'écrit rien : ni
    données, ni watermark, ni log_runs. Les premiers enregistrements bruts
    sont renvoyés dans RunResult.sample. Sans accumulation en mémoire.

    `on_progress(pages, rows)`, si fourni, est appelé après chaque page reçue
    avec le nombre de pages et de lignes lues jusque-là — le seul signe de vie
    d'un run d'une heure. Il ne fait pas échouer le run : s'il lève, il est
    abandonné et `warnings` le dit.

    Dans Fabric, le chemin local par défaut est automatiquement résolu vers
    l'URI ABFSS OneLake du lakehouse par défaut (le montage local ne permet
    pas le commit du transaction log delta-rs). storage_options est passé tel
    quel à delta-rs pour un stockage non-Fabric ou une auth spécifique."""
    source_name = config.get("name", "<sans_nom>") if isinstance(config, dict) else "<sans_nom>"
    run_id = str(uuid.uuid4())
    start_ts = _utc_now()
    started = time.monotonic()
    status: Literal["success", "failed"] = "failed"
    error_message = None
    fetch_page: PageFetcher | None = None
    progress = _Progress()
    # Annulation (KeyboardInterrupt de Fabric, SystemExit…) : le run est
    # consigné comme échoué, puis l'interruption est relancée à l'appelant.
    interrupted: BaseException | None = None

    try:
        lakehouse_tables_path = resolve_lakehouse_tables_path(lakehouse_tables_path)
    except Exception:  # noqa: BLE001
        pass
    target = RunTarget(
        source_name, run_id, lakehouse_tables_path, log_schema, storage_options
    )
    path_warning = None if dry_run else _lakehouse_path_warning(lakehouse_tables_path)
    if path_warning:
        progress.warnings.append(path_warning)
    lock = RunLock(
        lakehouse_tables_path, source_name, run_id, log_schema, storage_options,
        enabled=not dry_run,
    )

    try:
        validate_config(config)
        if on_progress is not None and not callable(on_progress):
            raise ConfigError("on_progress must be a function taking (pages, rows)")
        # tôt : refuser un second run avant qu'il ne lise le watermark du premier
        lock.acquire(config)

        # `or {}` : un `incremental: null` vaut « absent », comme à la validation
        incremental = config.get("incremental") or {}
        params = dict(config.get("params", {}))
        # Variables offertes au templating de 'body' et 'params'. Alimentées
        # par le watermark quand il doit atterrir ailleurs qu'en query string.
        variables: dict[str, str] = {}
        run_limit = config.get("max_run_seconds")
        deadline = time.monotonic() + run_limit if run_limit else None
        _prepare_incremental(
            incremental, params, variables, progress,
            target,
        )
        params = render(params, variables)

        # `progress.warnings` est alimentée par la pagination, puis complétée
        # par le writer : les deux signalent des dégradations qui ne font pas
        # échouer le run.
        fetch_page = _build_fetch_page(config, variables, deadline)
        reporter = _Reporter(on_progress, fetch_page.stats, progress.warnings)
        pages = paginate(
            fetch_page,
            config["base_url"],
            params,
            config.get("pagination"),
            progress.warnings,
        )

        if dry_run:
            _run_dry(pages, progress, reporter)
        else:
            _run_write(
                pages, config, incremental, progress, target, reporter,
            )

        status = "success"
    except Exception as exc:  # noqa: BLE001 — contrat : ne jamais lever
        error_message = f"{type(exc).__name__}: {exc}"
    except BaseException as exc:  # noqa: BLE001 — consigné, puis relancé
        # Sans cela, un notebook annulé en cours de route laissait des lots
        # écrits (et, avec `checkpoint`, un watermark avancé) sans la moindre
        # ligne dans log_runs : l'état de la table ne s'expliquait plus.
        interrupted = exc
        error_message = f"{type(exc).__name__}: run interrupted before completion"
    finally:
        if fetch_page is not None:
            fetch_page.session.close()
        _release_lock(lock, progress)

    # lus ici : `fetch_page` n'existe pas si le run a échoué avant sa création
    stats = fetch_page.stats if fetch_page is not None else {}

    # Une seule porte de sortie pour ce qui est persisté ou rendu à l'appelant
    if error_message is not None:
        error_message = _publishable(error_message)

    result = RunResult(
        source_name=source_name,
        status=status,
        rows_loaded=progress.rows_loaded,
        error_message=error_message,
        start_ts=start_ts,
        end_ts=_utc_now(),
        run_id=run_id,
        sample=progress.sample,
        warnings=[_publishable(w) for w in progress.warnings],
        watermark_before=progress.watermark_before,
        watermark_after=progress.watermark_after,
        window_end=progress.window_end,
        rows_inserted=progress.rows_inserted,
        rows_updated=progress.rows_updated,
        pages=stats.get("pages", 0),
        retries=stats.get("retries", 0),
        token_renewals=fetch_page.auth.renewals if fetch_page is not None else 0,
        duration_seconds=round(time.monotonic() - started, 3),
    )

    if not dry_run:
        _persist_run(result, target, config)
    if interrupted is not None:
        raise interrupted
    return result

