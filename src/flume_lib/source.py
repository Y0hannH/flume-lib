"""Point d'entrée unique : run_source(config) -> RunResult. Ne lève jamais
d'exception vers l'appelant."""

import uuid
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from flume_lib._delta import append_records, resolve_lakehouse_tables_path, table_uri
from flume_lib.auth import build_auth
from flume_lib.logging_ import write_log_run
from flume_lib.pagination import paginate
from flume_lib.templating import check_value, render
from flume_lib.validation import validate_config
from flume_lib.watermark import read_watermark, write_watermark

DEFAULT_LAKEHOUSE_TABLES_PATH = "/lakehouse/default/Tables"
DEFAULT_LOG_SCHEMA = "flume"
DEFAULT_TIMEOUT_SECONDS = 60

# Plafond de l'attente dictée par un header Retry-After. Au-delà, l'attente est
# tronquée : la tentative suivante échouera probablement, et c'est le
# comportement voulu — un run marqué `failed` dans log_runs est un signal plus
# exploitable qu'un notebook bloqué une heure.
DEFAULT_MAX_RETRY_AFTER_SECONDS = 300

# Colonnes de traçabilité ajoutées à chaque ligne écrite : elles permettent de
# relier une ligne au run qui l'a produite (dédoublonnage après retry partiel,
# annulation ciblée d'un run). Préfixées pour éviter toute collision avec les
# champs de l'API.
LINEAGE_RUN_ID = "_flume_run_id"
LINEAGE_INGESTED_AT = "_flume_ingested_at"

DRY_RUN_SAMPLE_SIZE = 3


class RetryableHTTPError(Exception):
    def __init__(self, status_code: int, url: str, retry_after: float | None = None):
        detail = f" (Retry-After: {retry_after:g}s)" if retry_after is not None else ""
        super().__init__(f"HTTP {status_code} sur {url}{detail}")
        self.status_code = status_code
        self.retry_after = retry_after


def _safe_url(url: str) -> str:
    """URL sans query string, pour les messages d'erreur. `error_message` est
    persisté dans la table Delta log_runs, lisible par tout le lakehouse : une
    URL complète y recopierait les query params, et donc un secret qu'une
    config mal écrite y aurait placé. La position du run reste lisible via
    RunResult.rows_loaded."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _parse_retry_after(raw: str | None) -> float | None:
    """Décode un header Retry-After : nombre de secondes ou date HTTP
    (RFC 9110 §10.2.3). Retourne None si absent ou illisible."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _build_wait(retry_config: dict):
    """Attente entre deux tentatives : le Retry-After annoncé par le serveur
    s'il y en a un, sinon le backoff exponentiel. Respecter l'indication du
    serveur évite le bannissement sur les APIs à gouvernance stricte."""
    fallback = wait_exponential(multiplier=retry_config.get("backoff_multiplier", 1))
    cap = retry_config.get(
        "max_retry_after_seconds", DEFAULT_MAX_RETRY_AFTER_SECONDS
    )

    def wait(retry_state):
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None else None
        if isinstance(exc, RetryableHTTPError) and exc.retry_after is not None:
            return min(exc.retry_after, cap)
        return fallback(retry_state)

    return wait


_RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    RetryableHTTPError,
)


@dataclass
class RunResult:
    source_name: str
    status: str  # "success" | "failed"
    rows_loaded: int
    error_message: str | None
    start_ts: str
    end_ts: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    # Renseigné uniquement en dry-run : premiers enregistrements bruts reçus
    sample: list | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_fetch_page(config: dict, variables: dict | None = None):
    auth_headers, signer = build_auth(config.get("auth"))
    retry_config = config.get("retry", {})
    timeout = config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    method = str(config.get("method", "GET")).upper()
    base_body = render(config.get("body", {}), variables or {})
    body_format = config.get("body_format", "json")
    params_in = (config.get("pagination") or {}).get("params_in", "query")
    retryer = Retrying(
        stop=stop_after_attempt(retry_config.get("max_attempts", 3)),
        wait=_build_wait(retry_config),
        retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    session = requests.Session()
    # headers de la config d'abord : l'auth ne doit jamais être écrasée par eux
    session.headers.update(config.get("headers", {}))
    session.headers.update(auth_headers)
    if signer is not None:
        session.auth = signer
    body_key = "json" if body_format == "json" else "data"

    def _request(url: str, params: dict):
        kwargs = {"timeout": timeout}
        if method == "GET":
            kwargs["params"] = params
        elif params_in == "body":
            kwargs[body_key] = {**base_body, **params}
        else:
            kwargs["params"] = params
            kwargs[body_key] = dict(base_body)
        response = session.request(method, url, **kwargs)
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHTTPError(
                response.status_code,
                _safe_url(url),
                _parse_retry_after(response.headers.get("Retry-After")),
            )
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {response.status_code} sur {_safe_url(url)}",
                response=response,
            )
        # headers requis par certaines stratégies (ex. total de pages)
        return response.json(), response.headers

    def fetch_page(url: str, params: dict):
        return retryer(_request, url, params)

    return fetch_page


def _max_incremental_value(records: list[dict], field_name: str):
    values = [r[field_name] for r in records if r.get(field_name) is not None]
    return max(values) if values else None


def _add_lineage(records: list[dict], run_id: str) -> None:
    ingested_at = _utc_now()
    for record in records:
        record[LINEAGE_RUN_ID] = run_id
        record[LINEAGE_INGESTED_AT] = ingested_at


def run_source(
    config: dict,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
    log_schema: str = DEFAULT_LOG_SCHEMA,
    dry_run: bool = False,
) -> RunResult:
    """Exécute l'ingestion d'une source d'après sa config. Toute erreur est
    catchée et remontée dans RunResult, jamais levée vers l'appelant.

    Les headers fixes de config['headers'] sont ajoutés à chaque appel, sans
    jamais écraser ceux de l'auth. Quand incremental.inject vaut
    'body_template', le watermark est interpolé dans les placeholders {nom} de
    config['body'] au lieu d'être ajouté en query string — indispensable pour
    les APIs SQL-over-REST où le filtre vit dans la requête elle-même.

    Cible exclusivement des lakehouses avec schémas : les données vont dans
    config['target_schema'] (requis), les tables techniques watermark et
    log_runs dans log_schema (défaut : 'flume'). Chaque ligne écrite porte
    les colonnes de traçabilité _flume_run_id et _flume_ingested_at.

    dry_run=True valide la config, appelle réellement l'API (donc vérifie
    aussi les credentials) et compte les lignes, mais n'écrit rien : ni
    données, ni watermark, ni log_runs. Les premiers enregistrements bruts
    sont renvoyés dans RunResult.sample. Sans accumulation en mémoire.

    Dans Fabric, le chemin local par défaut est automatiquement résolu vers
    l'URI ABFSS OneLake du lakehouse par défaut (le montage local ne permet
    pas le commit du transaction log delta-rs). storage_options est passé tel
    quel à delta-rs pour un stockage non-Fabric ou une auth spécifique."""
    source_name = config.get("name", "<sans_nom>") if isinstance(config, dict) else "<sans_nom>"
    run_id = str(uuid.uuid4())
    start_ts = _utc_now()
    status = "failed"
    rows_loaded = 0
    error_message = None
    sample = None

    try:
        lakehouse_tables_path = resolve_lakehouse_tables_path(lakehouse_tables_path)
    except Exception:  # noqa: BLE001
        pass

    try:
        validate_config(config)

        incremental = config.get("incremental", {})
        params = dict(config.get("params", {}))
        # Variables offertes au templating de 'body' et 'params'. Alimentées
        # par le watermark quand il doit atterrir ailleurs qu'en query string.
        variables: dict[str, str] = {}

        if incremental.get("enabled"):
            last_value = read_watermark(
                lakehouse_tables_path,
                source_name,
                schema=log_schema,
                storage_options=storage_options,
            )
            if last_value is None:
                # Premier run : sans plancher, un placeholder resterait non
                # substitué et partirait tel quel dans la requête.
                last_value = incremental.get("initial_value")
            if last_value is not None:
                if incremental.get("inject", "query_param") == "body_template":
                    placeholder = incremental.get("placeholder", "watermark")
                    variables[placeholder] = check_value(
                        last_value,
                        incremental.get("value_format", "any"),
                        label=f"incremental : watermark '{placeholder}'",
                    )
                else:
                    params[incremental["param_name"]] = last_value

        params = render(params, variables)

        pages = paginate(
            _build_fetch_page(config, variables),
            config["base_url"],
            params,
            config.get("pagination"),
        )

        if dry_run:
            sample = []
            for page in pages:
                if len(sample) < DRY_RUN_SAMPLE_SIZE:
                    sample.extend(page[: DRY_RUN_SAMPLE_SIZE - len(sample)])
                rows_loaded += len(page)
        else:
            records: list[dict] = []
            for page in pages:
                records.extend(page)
            rows_loaded = len(records)

            if records:
                _add_lineage(records, run_id)
                append_records(
                    table_uri(
                        lakehouse_tables_path,
                        config["target_schema"],
                        config["target_table"],
                    ),
                    records,
                    storage_options=storage_options,
                )

                if incremental.get("enabled"):
                    new_watermark = _max_incremental_value(
                        records, incremental["field"]
                    )
                    if new_watermark is not None:
                        write_watermark(
                            lakehouse_tables_path,
                            source_name,
                            new_watermark,
                            schema=log_schema,
                            storage_options=storage_options,
                        )

        status = "success"
    except Exception as exc:  # noqa: BLE001 — contrat : ne jamais lever
        error_message = f"{type(exc).__name__}: {exc}"

    end_ts = _utc_now()
    result = RunResult(
        source_name=source_name,
        status=status,
        rows_loaded=rows_loaded,
        error_message=error_message,
        start_ts=start_ts,
        end_ts=end_ts,
        run_id=run_id,
        sample=sample,
    )

    if dry_run:
        return result

    try:
        write_log_run(
            lakehouse_tables_path,
            run_id=run_id,
            source_name=source_name,
            start_ts=start_ts,
            end_ts=end_ts,
            status=status,
            rows_loaded=rows_loaded,
            error_message=error_message,
            schema=log_schema,
            storage_options=storage_options,
        )
    except Exception as exc:  # noqa: BLE001
        log_error = f"écriture log_runs impossible — {type(exc).__name__}: {exc}"
        result.error_message = (
            f"{error_message} | {log_error}" if error_message else log_error
        )

    return result
