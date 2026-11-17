"""Le client HTTP d'un run : une page demandée, une réponse lue sous bornes.

`_build_fetch_page` assemble ce que la pagination appelle page après page :
auth (renouvelée), cadence (`max_requests_per_second`), rejeu (429, 5xx,
erreurs réseau, 401 sur token périmé), lecture en flux sous plafond de taille
et d'échéance, erreurs applicatives d'une réponse 200. Les headers d'auth et
les cookies ne suivent pas une redirection vers un autre hôte.

Aucune écriture ici : `writer.py` écrit, `source.py` orchestre."""

import http.cookiejar
import json
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from flume_lib._paths import MISSING, get_path
from flume_lib.auth import AuthProvider
from flume_lib.network import resolve_network
from flume_lib.secrets_ import SENSITIVE_NAME_RE, register_secret
from flume_lib.templating import (
    render,
    templated_placeholders,
)
from flume_lib.urls_ import safe_url, sanitized_request_error
from flume_lib.validation import (
    ConfigError,
)

DEFAULT_TIMEOUT_SECONDS = 60


# Plafond de l'attente dictée par un header Retry-After. Au-delà, l'attente est
# tronquée : la tentative suivante échouera probablement, et c'est le
# comportement voulu — un run marqué `failed` dans log_runs est un signal plus
# exploitable qu'un notebook bloqué une heure.
DEFAULT_MAX_RETRY_AFTER_SECONDS = 300


# Plafond du corps d'une réponse de données. Une page légitime pèse quelques
# mégaoctets ; au-delà, c'est une API qui ne pagine pas, ou un serveur qui
# déverse un flux : `response.json()` chargeait tout en mémoire, sans borne.
DEFAULT_MAX_RESPONSE_BYTES = 100 * 1024 * 1024


DEFAULT_MAX_RESPONSE_BYTES = 100 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024


_READ_CHUNK_BYTES = 64 * 1024
# Ce qu'on lit d'une réponse d'erreur pour en tirer `MAX_ERROR_DETAIL_CHARS`
# caractères : de quoi couvrir de l'UTF-8 à quatre octets par caractère, et les
# blancs que la mise en forme écrase.
_ERROR_BODY_READ_BYTES = 16 * 1024


# Erreurs applicatives : bornes de ce qui part dans log_runs. Le message vient
# de l'API et peut être volumineux (une erreur GraphQL recopie souvent la
# requête et sa position) — inutile de le stocker en entier ligne après ligne.
MAX_ERRORS_REPORTED = 3


MAX_ERRORS_REPORTED = 3
MAX_ERROR_DETAIL_CHARS = 500


DEFAULT_ERRORS_PATH = "errors"


DEFAULT_ERRORS_PATH = "errors"
# Formes par défaut de la spécification GraphQL : `errors[].message` et
# `errors[].extensions`. Surchargeable pour toute autre enveloppe d'erreur.
DEFAULT_ERROR_MESSAGE_FIELD = "message"


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


class ResponseTooLargeError(Exception):
    """Le corps d'une réponse dépasse `max_response_bytes`."""


class RunTimeoutError(Exception):
    """Le run a dépassé `max_run_seconds`."""


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
        body = _error_body_text(response)
    except Exception:  # noqa: BLE001 — un corps illisible ne masque pas le statut
        return ""
    body = " ".join((body or "").split())
    return f": {body[:MAX_ERROR_DETAIL_CHARS]}" if body else ""


def _error_body_text(response) -> str:
    """Début du corps d'une réponse d'erreur. Une vraie réponse est lue en flux
    et abandonnée après quelques kilooctets : `response.text` aurait chargé tout
    le corps, et un serveur hostile choisit sa taille. Toute autre réponse (les
    doublures des tests) garde `.text`."""
    if not isinstance(response, requests.Response):
        return response.text
    data = b""
    for chunk in response.iter_content(chunk_size=_READ_CHUNK_BYTES):
        data += chunk
        if len(data) >= _ERROR_BODY_READ_BYTES:
            break
    response.close()
    return data[:_ERROR_BODY_READ_BYTES].decode(response.encoding or "utf-8", errors="replace")


def _check_deadline(deadline: float | None, limit: float | None) -> None:
    """Lève `RunTimeoutError` si l'échéance du run est passée. `deadline` est un
    instant de l'horloge monotone ; `limit` la durée configurée, pour le message."""
    if deadline is not None and time.monotonic() > deadline:
        raise RunTimeoutError(
            f"the run exceeded max_run_seconds ({limit:g} s) — stopped before "
            "reaching the end of the source. Rows already written stay in the table."
        )


def _discard(response) -> None:
    """Libère la connexion d'une réponse dont on ne lira pas le corps."""
    close = getattr(response, "close", None)
    if close is not None:
        close()


def _read_json(response, url: str, max_bytes: int, deadline, limit):
    """Corps JSON d'une réponse, lu en flux sous deux bornes : sa taille
    (`max_bytes`) et l'échéance du run.

    `requests` ne borne la durée que **entre deux octets** (`timeout`) : un
    serveur qui en envoie un toutes les 50 secondes garde la requête ouverte
    indéfiniment. L'échéance est donc revérifiée à chaque bloc lu. Toute
    réponse qui n'est pas une `requests.Response` (les doublures des tests) garde
    `.json()`."""
    if not isinstance(response, requests.Response):
        return response.json()
    declared = response.headers.get("Content-Length", "")
    if declared.isdigit() and int(declared) > max_bytes:
        response.close()
        raise ResponseTooLargeError(
            f"response from {safe_url(url)} announces {int(declared)} bytes, above "
            f"max_response_bytes ({max_bytes})"
        )
    chunks, size = [], 0
    for chunk in response.iter_content(chunk_size=_READ_CHUNK_BYTES):
        size += len(chunk)
        if size > max_bytes:
            response.close()
            raise ResponseTooLargeError(
                f"response from {safe_url(url)} exceeds max_response_bytes "
                f"({max_bytes}) — the API is not paginating, or the page size is too large"
            )
        _check_deadline(deadline, limit)
        chunks.append(chunk)
    response.close()
    try:
        return json.loads(b"".join(chunks))
    except ValueError as exc:
        # même type que `response.json()` : c'est lui que `error_message` recopie
        raise requests.exceptions.JSONDecodeError(str(exc), "", 0) from None


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
    serveur évite le bannissement sur les APIs à gouvernance stricte.

    Le backoff est dispersé (`retry.jitter`, vrai par défaut) : entre la moitié
    du délai et le délai plein. Des sources lancées ensemble — `run_sources` —
    qui reçoivent un 429 au même instant se représenteraient sinon toutes au
    même instant, et retomberaient ensemble sur le même 429. La moitié fixe
    garde un délai réel : un tirage dans [0, délai] pouvait rejouer aussitôt."""
    exponential = wait_exponential(multiplier=retry_config.get("backoff_multiplier", 1))
    jitter = retry_config.get("jitter", True)
    cap = retry_config.get(
        "max_retry_after_seconds", DEFAULT_MAX_RETRY_AFTER_SECONDS
    )

    def fallback(retry_state):
        delay = exponential(retry_state)
        if jitter:
            # dispersion des clients, pas de la cryptographie
            return delay / 2 + random.uniform(0, delay / 2)  # nosec B311
        return delay

    def wait(retry_state):
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None else None
        if isinstance(exc, RetryableError) and exc.retry_after is not None:
            return min(exc.retry_after, cap)
        return fallback(retry_state)

    return wait


class _Throttle:
    """Espace les requêtes d'un run : au plus `per_second` départs par seconde.

    Le 429 réactif arrive après le dépassement, et sur une API à gouvernance
    stricte c'est le dépassement qui est sanctionné. Chaque *tentative* passe
    par ici, rejeux compris. Une requête dont le départ tomberait au-delà de
    l'échéance du run échoue tout de suite, sans dormir jusque-là pour rien.
    Un compteur par run — des sources parallèles ont chacune le leur : un quota
    partagé entre elles se règle avec `max_workers`."""

    def __init__(self, per_second: float | None):
        # None : pas de limite, `wait` ne fait rien
        self._interval = 1.0 / per_second if per_second else None
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self, deadline: float | None = None, limit: float | None = None) -> None:
        if self._interval is None:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self._interval
        if deadline is not None and start >= deadline:
            raise RunTimeoutError(
                f"max_requests_per_second would delay the next request past "
                f"max_run_seconds ({limit:g} s) — stopped before reaching the "
                "end of the source. Rows already written stay in the table."
            )
        if start > now:
            time.sleep(start - now)


_RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    RetryableError,
)


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
    if errors is MISSING or not errors:
        return
    if not isinstance(errors, list):
        # certaines APIs ne renvoient qu'une erreur, sans l'envelopper
        errors = [errors]

    code_field = errors_config.get("code_field", DEFAULT_ERROR_CODE_FIELD)
    message_field = errors_config.get("message_field", DEFAULT_ERROR_MESSAGE_FIELD)

    def _field(error, path):
        value = get_path(error, path) if isinstance(error, dict) else MISSING
        return None if value is MISSING else value

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


class _NoCookies(http.cookiejar.DefaultCookiePolicy):
    """Politique d'un jar qui ne retient rien et ne renvoie rien."""

    def set_ok(self, cookie, request) -> bool:
        return False

    def return_ok(self, cookie, request) -> bool:
        return False


def _refuse_cookies(session) -> None:
    """Fait ignorer par la session tout cookie que l'API lui pose
    (`"persist_cookies": false`).

    Une `requests.Session` se comporte comme un navigateur : le `Set-Cookie`
    de la page 1 repart avec la page 2. Certaines APIs donnent alors au cookie
    de session le pas sur le header d'authentification, et répondent 401 à une
    requête dont le token est pourtant valide — dès la deuxième page.

    Deux verrous, parce que la politique du jar ne suffit pas. Elle empêche
    la session de retenir un cookie et d'en renvoyer un, mais `requests` suit
    une redirection avec un jar propre à la requête, qui ne l'hérite pas : le
    cookie posé par un 302 arrivait quand même à sa destination (mesuré sur
    requests 2.34). Le header `Cookie` est donc aussi retiré de chaque requête
    de redirection, par le même crochet que `_scope_auth_headers` — appelé
    après que `requests` l'a recalculé.

    Les appels de token (`auth.py`) n'utilisent pas de session : ils n'ont
    jamais porté de cookie, et rien ne change pour eux.
    """
    jar = getattr(session, "cookies", None)
    inherited = getattr(session, "rebuild_auth", None)
    if jar is None or inherited is None:
        return  # session factice : elle ne tient aucun cookie
    jar.set_policy(_NoCookies())

    def rebuild_auth(prepared_request, response):
        inherited(prepared_request, response)
        prepared_request.headers.pop("Cookie", None)

    session.rebuild_auth = rebuild_auth


def _set_cookie_persistence(session, persist: bool) -> None:
    """Le défaut de `requests` — garder et renvoyer les cookies — ou rien."""
    if not persist:
        _refuse_cookies(session)


def _register_sensitive_headers(headers: dict) -> None:
    """Un header fixe au nom sensible n'est littéral qu'avec
    `allow_literal_secrets` : sa valeur est un secret, masqué comme tel dans
    les messages."""
    for name, value in headers.items():
        if SENSITIVE_NAME_RE.search(str(name)):
            register_secret(value)


@dataclass
class PageFetcher:
    """Ce que la pagination appelle : `fetcher(url, params)` rend `(JSON,
    headers)`. Porte aussi ce que `run_source` lit en sortant.

    `session` porte le pool de connexions : `run_source` la ferme en sortant,
    plutôt que d'attendre le ramasse-miettes — un notebook qui enchaîne les
    sources en laissait un par run. `stats` compte les pages reçues et les
    requêtes rejouées ; `auth` les renouvellements de token."""

    fetch: Callable[[str, dict], tuple]
    session: requests.Session
    stats: dict
    auth: AuthProvider

    def __call__(self, url: str, params: dict) -> tuple:
        return self.fetch(url, params)


def _build_fetch_page(
    config: dict, variables: dict | None = None, deadline: float | None = None
) -> PageFetcher:
    network = resolve_network(config)
    auth = AuthProvider(config.get("auth"), network)
    max_bytes = config.get("max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES)
    run_limit = config.get("max_run_seconds")
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
    # ce que le run a coûté en appels : pages reçues, requêtes rejouées
    stats = {"pages": 0, "retries": 0}
    throttle = _Throttle(config.get("max_requests_per_second"))

    def _count_retry(retry_state) -> None:
        stats["retries"] += 1

    retryer = Retrying(
        stop=stop_after_attempt(retry_config.get("max_attempts", 3)),
        wait=_build_wait(retry_config),
        retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
        before_sleep=_count_retry,
        reraise=True,
    )
    session = requests.Session()
    # headers de la config d'abord : l'auth ne doit jamais être écrasée par eux
    fixed_headers = config.get("headers", {})
    _register_sensitive_headers(fixed_headers)
    session.headers.update(fixed_headers)
    auth_headers = auth.headers()
    session.headers.update(auth_headers)
    # Les headers de `config['headers']` sont des littéraux non sensibles
    # (la validation le garantit) : seuls ceux de l'auth sont suivis.
    auth_header_names = set(auth_headers)
    _scope_auth_headers(session, auth_header_names)
    _set_cookie_persistence(session, config.get("persist_cookies", True))
    if auth.signer is not None:
        session.auth = auth.signer
    body_key = "json" if body_format == "json" else "data"
    # Un GET porteur d'un corps n'a pas de sémantique définie par HTTP, et la
    # plupart des APIs l'ignorent : le refuser est le bon défaut. Certaines
    # l'exigent pourtant — leur filtre ne vit que là. Une fois l'exception
    # déclarée, un GET suit exactement les mêmes branches qu'un POST : c'est
    # `params_in` qui décide où vont les paramètres, pas la méthode.
    allow_body_on_get = bool(config.get("allow_body_on_get"))

    def _request(url: str, params: dict, state: dict):
        if auth.refreshable:
            # renouvellement anticipé quand le endpoint annonce une expiration
            renewed = auth.headers()
            session.headers.update(renewed)
            auth_header_names.update(renewed)
        # la cadence d'abord : l'attente qu'elle impose compte dans l'échéance
        throttle.wait(deadline, run_limit)
        # avant tout appel, retries compris : l'échéance du run n'est pas
        # rejouable. `stream` : le corps est lu par morceaux, sous plafond.
        _check_deadline(deadline, run_limit)
        # `proxies` et `verify` par requête : ceux de la session cèdent devant
        # HTTPS_PROXY / REQUESTS_CA_BUNDLE, ceux d'une requête non
        kwargs = {"timeout": timeout, "stream": True, **network}
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
            _discard(response)
            raise RetryableHTTPError(response.status_code, safe_url(url), retry_after)
        if (
            response.status_code == 401
            and auth.refreshable
            and not state["refreshed"]
        ):
            # Une seule tentative de renouvellement par page : si le 401
            # persiste avec un token neuf, ce n'est pas une expiration et
            # rejouer ne ferait que retarder le diagnostic.
            state["refreshed"] = True
            _discard(response)
            renewed = auth.refresh()
            session.headers.update(renewed)
            auth_header_names.update(renewed)
            raise ExpiredTokenError(safe_url(url))
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {response.status_code} on {safe_url(url)}"
                f"{_response_detail(response)}",
                response=response,
            )
        payload = _read_json(response, url, max_bytes, deadline, run_limit)
        _check_response_errors(payload, errors_config, safe_url(url), retry_after)
        # headers requis par certaines stratégies (ex. total de pages)
        return payload, response.headers

    def fetch_page(url: str, params: dict):
        page = retryer(_request, url, params, {"refreshed": False})
        stats["pages"] += 1
        return page

    return PageFetcher(fetch_page, session, stats, auth)

