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
from flume_lib.pagination import _MISSING, get_path, paginate
from flume_lib.templating import check_value, render
from flume_lib.validation import ConfigError, validate_config
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

# Erreurs applicatives : bornes de ce qui part dans log_runs. Le message vient
# de l'API et peut être volumineux (une erreur GraphQL recopie souvent la
# requête et sa position) — inutile de le stocker en entier ligne après ligne.
MAX_ERRORS_REPORTED = 3
MAX_ERROR_DETAIL_CHARS = 500

DEFAULT_ERRORS_PATH = "errors"
# Formes par défaut de la spécification GraphQL : `errors[].message` et
# `errors[].extensions`. Surchargeable pour toute autre enveloppe d'erreur.
DEFAULT_ERROR_MESSAGE_FIELD = "message"
DEFAULT_ERROR_CODE_FIELD = "extensions.code"


class RetryableError(Exception):
    """Erreur transitoire : la requête sera rejouée. `retry_after`, quand il
    est renseigné, prime sur le backoff exponentiel."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class RetryableHTTPError(RetryableError):
    def __init__(self, status_code: int, url: str, retry_after: float | None = None):
        detail = f" (Retry-After: {retry_after:g}s)" if retry_after is not None else ""
        super().__init__(f"HTTP {status_code} sur {url}{detail}", retry_after)
        self.status_code = status_code


class APIError(Exception):
    """Erreur applicative renvoyée avec un statut HTTP de succès — le cas
    normal en GraphQL, où `errors` cohabite avec un 200 et `data: null`."""


class RetryableAPIError(RetryableError):
    """Même chose, mais annoncée comme transitoire par l'API (throttling)."""


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
        if isinstance(exc, RetryableError) and exc.retry_after is not None:
            return min(exc.retry_after, cap)
        return fallback(retry_state)

    return wait


_RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    RetryableError,
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


def _check_response_errors(
    payload, errors_config: dict | None, url: str, retry_after: float | None = None
) -> None:
    """Détecte une erreur applicative transportée par une réponse au statut
    HTTP de succès. Sans ce contrôle, une erreur partielle — en GraphQL, un
    `data` exploitable accompagné d'un `errors` (champ refusé par un scope
    manquant, par exemple) — donne un run `success` amputé d'une partie des
    données, sans le moindre signal ; une erreur totale, elle, ne se manifeste
    que par un message de pagination trompeur au lieu de celui de l'API.

    Les codes listés dans `retryable_codes` déclenchent un rejeu plutôt qu'un
    échec : c'est par là qu'arrive le throttling des APIs qui l'annoncent dans
    le corps au lieu d'un 429."""
    if not errors_config:
        return
    errors = get_path(payload, errors_config.get("path", DEFAULT_ERRORS_PATH))
    if errors is _MISSING or not errors:
        return
    if not isinstance(errors, list):
        # certaines APIs ne renvoient qu'une erreur, sans l'envelopper
        errors = [errors]

    code_field = errors_config.get("code_field", DEFAULT_ERROR_CODE_FIELD)
    message_field = errors_config.get("message_field", DEFAULT_ERROR_MESSAGE_FIELD)

    def _field(error, path):
        value = get_path(error, path) if isinstance(error, dict) else _MISSING
        return None if value is _MISSING else value

    retryable_codes = errors_config.get("retryable_codes") or []
    codes = [_field(e, code_field) for e in errors]
    messages = [
        str(_field(e, message_field) or e) for e in errors[:MAX_ERRORS_REPORTED]
    ]
    detail = " | ".join(messages)[:MAX_ERROR_DETAIL_CHARS]
    summary = f"{len(errors)} erreur(s) applicative(s) sur {url} : {detail}"

    if any(code in retryable_codes for code in codes if code is not None):
        raise RetryableAPIError(summary, retry_after)
    raise APIError(summary)


def _merge_params_into_body(body: dict, params: dict, params_path: str | None) -> dict:
    """Fusionne les paramètres de pagination dans le corps de la requête. À la
    racine par défaut ; sous `params_path` quand l'API les attend imbriqués —
    GraphQL veut curseur et taille de page à l'intérieur de `variables`, pas à
    la racine aux côtés de `query`."""
    if not params_path:
        return {**body, **params}
    merged = dict(body)
    node = merged
    parts = params_path.split(".")
    for part in parts[:-1]:
        child = dict(node.get(part) or {})
        node[part] = child
        node = child
    last = parts[-1]
    node[last] = {**(node.get(last) or {}), **params}
    return merged


def _render_body(body, variables: dict, template_paths: list | None):
    """Applique le templating au corps de la requête. Sans `template_paths`,
    tout le corps est parcouru. Avec, seules les branches désignées le sont —
    nécessaire en GraphQL : une requête compacte (`{orders{edges{node{id}}}}`)
    contient des `{id}` qu'un scan global prendrait pour des placeholders et
    ferait échouer le run."""
    if not variables or not template_paths:
        return render(body, variables)
    rendered = body
    for path in template_paths:
        rendered = _render_at_path(rendered, path.split("."), variables, path)
    return rendered


def _render_at_path(container, parts: list[str], variables: dict, full_path: str):
    head, *rest = parts
    if not isinstance(container, dict) or head not in container:
        raise ConfigError(
            f"template_paths : chemin '{full_path}' introuvable dans 'body'"
        )
    updated = dict(container)
    updated[head] = (
        render(container[head], variables)
        if not rest
        else _render_at_path(container[head], rest, variables, full_path)
    )
    return updated


def _build_fetch_page(config: dict, variables: dict | None = None):
    auth_headers, signer = build_auth(config.get("auth"))
    retry_config = config.get("retry", {})
    timeout = config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    method = str(config.get("method", "GET")).upper()
    base_body = _render_body(
        config.get("body", {}), variables or {}, config.get("template_paths")
    )
    body_format = config.get("body_format", "json")
    pagination_config = config.get("pagination") or {}
    params_in = pagination_config.get("params_in", "query")
    params_path = pagination_config.get("params_path")
    errors_config = config.get("errors")
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
            kwargs[body_key] = _merge_params_into_body(base_body, params, params_path)
        else:
            kwargs["params"] = params
            kwargs[body_key] = dict(base_body)
        response = session.request(method, url, **kwargs)
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHTTPError(response.status_code, _safe_url(url), retry_after)
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {response.status_code} sur {_safe_url(url)}",
                response=response,
            )
        payload = response.json()
        _check_response_errors(payload, errors_config, _safe_url(url), retry_after)
        # headers requis par certaines stratégies (ex. total de pages)
        return payload, response.headers

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
    config['template_paths'] restreint ce templating à certaines branches du
    corps, indispensable quand celui-ci contient déjà des accolades (GraphQL).

    config['errors'] déclare l'enveloppe d'erreur applicative des APIs qui
    répondent 200 avec l'erreur dans le corps — sans elle, un tel run finirait
    'success' avec 0 ligne et aucun message.

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
