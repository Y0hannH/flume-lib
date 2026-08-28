"""Point d'entrée unique : run_source(config) -> RunResult. Ne lève jamais
d'exception vers l'appelant."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from flume_lib._delta import resolve_lakehouse_tables_path, table_uri, write_records
from flume_lib.auth import AuthProvider
from flume_lib.logging_ import write_log_run
from flume_lib.pagination import _MISSING, get_path, paginate
from flume_lib.templating import (
    check_value,
    normalize_value,
    render,
    templated_placeholders,
)
from flume_lib.urls_ import safe_url, sanitized_request_error
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

# Modes d'écriture passés à delta-rs. Les trois modes de config (append,
# overwrite, replace_where) se ramènent à ces deux-là : un remplacement de
# fenêtre est un overwrite porteur d'un prédicat. "append" reste le défaut,
# c'est le seul qui ne détruit rien.
WRITE_APPEND = "append"
WRITE_OVERWRITE = "overwrite"

# Nombre de lignes tamponnées avant écriture. Borne la mémoire d'un run : elle
# ne dépend plus du volume de la source. Une source qui tient sous ce seuil
# produit un unique commit Delta, comme avant l'introduction des lots.
DEFAULT_BATCH_SIZE = 50_000

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
        super().__init__(f"HTTP {status_code} on {url}{detail}", retry_after)
        self.status_code = status_code


class ExpiredTokenError(RetryableError):
    """401 sur une auth à token renouvelable. Le token vient d'être renouvelé
    et la requête est rejouée sans attendre : `retry_after=0` court-circuite le
    backoff, qui n'aurait ici aucun sens — rien n'est saturé, le credential
    était simplement périmé."""

    def __init__(self, url: str):
        super().__init__(
            f"HTTP 401 on {url} — token refreshed, request replayed",
            retry_after=0,
        )


class IncrementalError(Exception):
    """Le watermark ne peut pas être calculé ou avancé sans risque."""


class APIError(Exception):
    """Erreur applicative renvoyée avec un statut HTTP de succès — le cas
    normal en GraphQL, où `errors` cohabite avec un 200 et `data: null`."""


class RetryableAPIError(RetryableError):
    """Même chose, mais annoncée comme transitoire par l'API (throttling)."""


def _response_detail(response) -> str:
    """Corps d'une réponse d'erreur, tronqué, pour le message d'exception.

    Un 4xx est définitif : il n'est pas rejoué, et le seul endroit où l'API
    dit *pourquoi* elle refuse est son corps. Sans lui, `error_message` se
    réduit à un code et une URL, et le diagnostic impose de rejouer la
    requête à la main.

    Même borne que les erreurs applicatives de `_check_response_errors` : le
    message part dans log_runs, et une réponse d'erreur recopie volontiers la
    requête entière. Les blancs sont écrasés pour qu'un corps indenté ne
    dépense pas sa troncature en retours à la ligne.
    """
    try:
        body = response.text
    except Exception:  # noqa: BLE001 — un corps illisible ne masque pas le statut
        return ""
    body = " ".join((body or "").split())
    return f": {body[:MAX_ERROR_DETAIL_CHARS]}" if body else ""


def _safe_url(url: str) -> str:
    """URL sans query string, pour les messages d'erreur. `error_message` est
    persisté dans la table Delta log_runs, lisible par tout le lakehouse : une
    URL complète y recopierait les query params, et donc un secret qu'une
    config mal écrite y aurait placé. La position du run reste lisible via
    RunResult.rows_loaded.

    Ne couvre que les messages rédigés par la lib. Ceux que requests forme
    lui-même sont traités par `sanitized_request_error` — voir urls_.py.
    """
    return safe_url(url)


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
    # Dégradations subies sans faire échouer le run — typiquement une colonne
    # dont les valeurs ne rentrent pas dans le type déduit et qui finit en
    # texte. Un run `success` peut en porter : c'est tout l'intérêt.
    warnings: list = field(default_factory=list)


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
    summary = f"{len(errors)} application error(s) on {url}: {detail}"

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
            f"template_paths: path '{full_path}' not found in 'body'"
        )
    updated = dict(container)
    updated[head] = (
        render(container[head], variables)
        if not rest
        else _render_at_path(container[head], rest, variables, full_path)
    )
    return updated


def _scope_auth_headers(session, header_names: set) -> None:
    """Fait retirer par la session ses headers d'authentification lorsqu'une
    redirection l'emmène vers un autre hôte.

    `requests.Session.rebuild_auth` ne supprime que le header littéral
    `Authorization`. Un `api_key_header` (défaut `X-API-Key`), ou un
    `bearer_token` porté par un `header_name` custom, repartait donc tel quel
    vers l'hôte de destination d'un 302 — y compris avec les stratégies de
    pagination qui n'appellent pourtant que `base_url`, puisque c'est le
    serveur, pas la config, qui décide de rediriger.

    `header_names` est partagé par référence : un renouvellement de token qui
    change de header le fait suivre.

    Posé sur l'instance plutôt que par sous-classe : la classe serait figée à
    l'import, avant que les tests puissent substituer leur propre session.
    """
    inherited = getattr(session, "rebuild_auth", None)
    should_strip = getattr(session, "should_strip_auth", None)
    if inherited is None or should_strip is None:
        return  # session factice : elle ne suit aucune redirection

    def rebuild_auth(prepared_request, response):
        inherited(prepared_request, response)
        if should_strip(response.request.url, prepared_request.url):
            for name in header_names:
                prepared_request.headers.pop(name, None)

    session.rebuild_auth = rebuild_auth


def _build_fetch_page(config: dict, variables: dict | None = None):
    auth = AuthProvider(config.get("auth"))
    retry_config = config.get("retry", {})
    timeout = config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    method = str(config.get("method", "GET")).upper()
    body_format = config.get("body_format", "json")
    pagination_config = config.get("pagination") or {}
    params_in = pagination_config.get("params_in", "query")
    template_paths = config.get("template_paths")
    templated_names: set[str] = set()
    if params_in == "body_template":
        # Le corps change à chaque page : le rendre ici échouerait sur les
        # placeholders que seule la pagination sait remplir.
        base_body = config.get("body", {})
        templated_names = templated_placeholders(base_body, template_paths)
    else:
        base_body = _render_body(config.get("body", {}), variables or {}, template_paths)
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
    auth_headers = auth.headers()
    session.headers.update(auth_headers)
    # Les headers de `config['headers']` sont des littéraux non sensibles
    # (la validation le garantit) : seuls ceux de l'auth sont suivis.
    auth_header_names = set(auth_headers)
    _scope_auth_headers(session, auth_header_names)
    if auth.signer is not None:
        session.auth = auth.signer
    body_key = "json" if body_format == "json" else "data"
    # Un GET porteur d'un corps n'a pas de semantique definie par HTTP, et la
    # plupart des APIs l'ignorent : le refuser est le bon defaut. Certaines
    # l'exigent pourtant — leur filtre ne vit que la. Une fois l'exception
    # declaree, un GET suit exactement les memes branches qu'un POST : c'est
    # `params_in` qui decide ou vont les parametres, pas la methode.
    allow_body_on_get = bool(config.get("allow_body_on_get"))

    def _request(url: str, params: dict, state: dict):
        if auth.refreshable:
            # renouvellement anticipé quand le endpoint annonce une expiration
            renewed = auth.headers()
            session.headers.update(renewed)
            auth_header_names.update(renewed)
        kwargs = {"timeout": timeout}
        if method == "GET" and not allow_body_on_get:
            kwargs["params"] = params
        elif params_in == "body":
            kwargs[body_key] = _merge_params_into_body(base_body, params, params_path)
        elif params_in == "body_template":
            # Un paramètre dont le placeholder figure dans le corps y est
            # substitué ; les autres — `limit` d'un endpoint SQL-over-REST,
            # les filtres fixes de `params` — partent en query string comme
            # avec toute autre stratégie. Sans cette répartition, ils étaient
            # simplement perdus : l'API servait sa page par défaut pendant
            # que la lib croyait dicter la sienne, et un backfill s'arrêtait
            # sur une « page partielle » après quelques centaines de lignes,
            # run marqué success.
            body_params = {k: v for k, v in params.items() if k in templated_names}
            query_params = {
                k: v for k, v in params.items() if k not in templated_names
            }
            if query_params:
                kwargs["params"] = query_params
            kwargs[body_key] = _render_body(
                base_body, {**(variables or {}), **body_params}, template_paths
            )
        else:
            kwargs["params"] = params
            kwargs[body_key] = dict(base_body)
        try:
            response = session.request(method, url, **kwargs)
        except requests.RequestException as exc:
            # urllib3 recopie l'URL demandée dans le message d'une
            # ConnectionError, query comprise. Le type est conservé : c'est
            # lui qui décide du rejeu.
            raise sanitized_request_error(exc, url, kwargs.get("params")) from None
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHTTPError(response.status_code, _safe_url(url), retry_after)
        if (
            response.status_code == 401
            and auth.refreshable
            and not state["refreshed"]
        ):
            # Une seule tentative de renouvellement par page : si le 401
            # persiste avec un token neuf, ce n'est pas une expiration et
            # rejouer ne ferait que retarder le diagnostic.
            state["refreshed"] = True
            renewed = auth.refresh()
            session.headers.update(renewed)
            auth_header_names.update(renewed)
            raise ExpiredTokenError(_safe_url(url))
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {response.status_code} on {_safe_url(url)}"
                f"{_response_detail(response)}",
                response=response,
            )
        payload = response.json()
        _check_response_errors(payload, errors_config, _safe_url(url), retry_after)
        # headers requis par certaines stratégies (ex. total de pages)
        return payload, response.headers

    def fetch_page(url: str, params: dict):
        return retryer(_request, url, params, {"refreshed": False})

    # La session porte le pool de connexions : `run_source` la ferme en
    # sortant, plutôt que d'attendre le ramasse-miettes. Un notebook qui
    # enchaîne les sources en laissait un par run.
    fetch_page.session = session
    return fetch_page


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
    try:
        return candidate < reference
    except TypeError:
        return False


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
        uri: str,
        run_id: str,
        source_name: str,
        batch_size: int,
        incremental: dict,
        lakehouse_tables_path: str,
        log_schema: str,
        storage_options: dict | None,
        write: dict | None = None,
    ):
        self._uri = uri
        self._run_id = run_id
        self._source_name = source_name
        self._batch_size = batch_size
        self._lakehouse_tables_path = lakehouse_tables_path
        self._log_schema = log_schema
        self._storage_options = storage_options

        write = write or {}
        self._write_mode = write.get("mode", WRITE_APPEND)
        self._replace_where = write.get("replace_where")
        self._partition_by = write.get("partition_by")
        # Consommé par le premier lot : voir _delta_write_args.
        self._replacement_pending = self._write_mode != WRITE_APPEND

        self._enabled = bool(incremental.get("enabled"))
        self._field = incremental.get("field")
        self._checkpoint = bool(incremental.get("checkpoint"))

        self._buffer: list[dict] = []
        self.rows_written = 0
        # Types Arrow retenus par le premier lot : les lots suivants s'y
        # conforment, sans quoi deux lots d'un même run pourraient typer
        # différemment la même colonne et casser le commit Delta.
        self._types: dict = {}
        self.warnings: list[str] = []
        # plus grande valeur vue jusqu'ici / dernière effectivement commitée
        self._pending = None
        self._written = None

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
        # Hors mode checkpoint le watermark n'est commité qu'ici, une fois le
        # run complet : la reprise n'est pas offerte, mais un run interrompu
        # ne laisse jamais un watermark avancé au-delà d'une fenêtre partielle.
        if self._enabled and not self._checkpoint:
            self._commit_watermark()

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

        _add_lineage(batch, self._run_id)
        types, fallbacks = write_records(
            self._uri,
            batch,
            **self._delta_write_args(),
            storage_options=self._storage_options,
            known_types=self._types,
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

    def _commit_watermark(self) -> None:
        if self._pending is None or self._pending == self._written:
            return
        write_watermark(
            self._lakehouse_tables_path,
            self._source_name,
            self._pending,
            schema=self._log_schema,
            storage_options=self._storage_options,
        )
        self._written = self._pending


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
    dit.

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
    warnings: list[str] = []
    fetch_page = None

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

        params = render(params, variables)

        # `warnings` est alimentée par la pagination, puis complétée par le
        # writer : les deux signalent des dégradations qui ne font pas
        # échouer le run.
        fetch_page = _build_fetch_page(config, variables)
        pages = paginate(
            fetch_page,
            config["base_url"],
            params,
            config.get("pagination"),
            warnings,
        )

        if dry_run:
            sample = []
            for page in pages:
                if len(sample) < DRY_RUN_SAMPLE_SIZE:
                    sample.extend(page[: DRY_RUN_SAMPLE_SIZE - len(sample)])
                rows_loaded += len(page)
        else:
            writer = _BatchWriter(
                uri=table_uri(
                    lakehouse_tables_path,
                    config["target_schema"],
                    config["target_table"],
                ),
                run_id=run_id,
                source_name=source_name,
                batch_size=config.get("batch_size", DEFAULT_BATCH_SIZE),
                incremental=incremental,
                lakehouse_tables_path=lakehouse_tables_path,
                log_schema=log_schema,
                storage_options=storage_options,
                write=config.get("write"),
            )
            try:
                for page in pages:
                    writer.add(page)
                writer.close()
            finally:
                # Ce que l'appelant voit doit être ce qui est réellement dans
                # la table, y compris quand le run casse en cours de route.
                rows_loaded = writer.rows_written
                # extend, pas une affectation : celles de la pagination sont
                # déjà dans la liste
                warnings.extend(writer.warnings)

        status = "success"
    except Exception as exc:  # noqa: BLE001 — contrat : ne jamais lever
        error_message = f"{type(exc).__name__}: {exc}"
    finally:
        session = getattr(fetch_page, "session", None)
        if session is not None:
            session.close()

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
        warnings=warnings,
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
