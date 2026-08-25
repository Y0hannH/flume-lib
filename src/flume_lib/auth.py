"""Stratégies d'authentification. Les credentials sont résolus au runtime
depuis des variables d'environnement ou Azure Key Vault (voir secrets_.py),
jamais lus depuis la config en clair."""

import base64
import time

import requests

from flume_lib.oauth1 import OAuth1Signer
from flume_lib.secrets_ import SecretResolutionError, resolve_secret

DEFAULT_TOKEN_TIMEOUT_SECONDS = 30
ENTRA_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Types d'auth dont le credential est un token obtenu par appel réseau, donc
# périssable et renouvelable. Les autres portent un credential statique : un
# 401 y est une erreur de configuration, que rejouer ne corrigerait pas.
REFRESHABLE_TYPES = ("oauth2_client_credentials", "token_endpoint")

# Marge avant l'expiration annoncée. Un token renouvelé à la seconde près
# arriverait expiré sur une API dont l'horloge avance de quelques secondes.
TOKEN_EXPIRY_MARGIN_SECONDS = 60


class AuthError(Exception):
    pass


def _resolve(auth_config: dict, field: str) -> str:
    """Résout un credential : nouvelle forme (référence sous la clé `field`)
    ou forme historique `{field}_env_var`."""
    legacy_key = f"{field}_env_var"
    if legacy_key in auth_config:
        ref = {"env_var": auth_config[legacy_key]}
    elif field in auth_config:
        ref = auth_config[field]
    else:
        raise AuthError(f"Clé '{field}' (ou '{legacy_key}') manquante dans la config auth")
    try:
        return resolve_secret(ref, field)
    except SecretResolutionError as exc:
        raise AuthError(str(exc)) from exc


def _extract_json_path(payload: dict, path: str):
    """Extrait une valeur d'un dict par chemin pointé, ex. 'data.token'."""
    value = payload
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise AuthError(f"Chemin '{path}' introuvable dans la réponse du token endpoint")
        value = value[part]
    return value


def _expires_in(payload: dict, path: str | None) -> float | None:
    """Durée de vie annoncée par le token endpoint, en secondes. Absente ou
    illisible, elle vaut None : le token n'est alors renouvelé qu'en réaction
    à un 401, jamais par anticipation."""
    if not path:
        return None
    value = payload
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_oauth2_client_credentials(auth_config: dict) -> tuple[str, float | None]:
    """Flux OAuth2 client_credentials — couvre les service principals Entra ID
    (APIs Microsoft : Graph, Fabric, Azure Management…) et tout IdP standard."""
    token_url = auth_config.get("token_url")
    if not token_url:
        tenant_id = auth_config.get("tenant_id")
        if not tenant_id:
            raise AuthError(
                "oauth2_client_credentials : 'token_url' ou 'tenant_id' requis"
            )
        token_url = ENTRA_TOKEN_URL.format(tenant_id=tenant_id)

    data = {
        "grant_type": "client_credentials",
        "client_id": _resolve(auth_config, "client_id"),
        "client_secret": _resolve(auth_config, "client_secret"),
    }
    if auth_config.get("scope"):
        data["scope"] = auth_config["scope"]

    timeout = auth_config.get("timeout_seconds", DEFAULT_TOKEN_TIMEOUT_SECONDS)
    response = requests.post(token_url, data=data, timeout=timeout)
    if response.status_code != 200:
        raise AuthError(
            f"oauth2_client_credentials : HTTP {response.status_code} sur {token_url}"
        )
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise AuthError("oauth2_client_credentials : 'access_token' absent de la réponse")
    return token, _expires_in(payload, "expires_in")


def _fetch_token_endpoint(auth_config: dict) -> tuple[str, float | None]:
    """Obtient un token via un appel API de login arbitraire. Les valeurs de
    'body' et 'headers' peuvent être des littéraux (valeurs non sensibles) ou
    des références de secret (env_var / keyvault_url)."""
    token_url = auth_config.get("token_url")
    if not token_url:
        raise AuthError("token_endpoint : 'token_url' requis")

    method = auth_config.get("method", "POST").upper()
    if method == "GET" and any(
        isinstance(ref, dict) for ref in auth_config.get("body", {}).values()
    ):
        raise AuthError(
            "token_endpoint : référence de secret interdite dans 'body' en GET — "
            "les paramètres partent dans l'URL (logs serveurs, proxies) ; utiliser POST"
        )

    body = {
        key: resolve_secret(ref, f"body.{key}")
        for key, ref in auth_config.get("body", {}).items()
    }
    headers = {
        key: resolve_secret(ref, f"headers.{key}")
        for key, ref in auth_config.get("headers", {}).items()
    }

    body_format = auth_config.get("body_format", "json")
    timeout = auth_config.get("timeout_seconds", DEFAULT_TOKEN_TIMEOUT_SECONDS)

    kwargs = {"headers": headers, "timeout": timeout}
    if method != "GET":
        kwargs["json" if body_format == "json" else "data"] = body
    elif body:
        kwargs["params"] = body

    response = requests.request(method, token_url, **kwargs)
    if response.status_code != 200:
        raise AuthError(f"token_endpoint : HTTP {response.status_code} sur {token_url}")

    payload = response.json()
    token = _extract_json_path(payload, auth_config.get("token_json_path", "access_token"))
    if not token:
        raise AuthError("token_endpoint : token vide dans la réponse")
    return token, _expires_in(payload, auth_config.get("expires_in_json_path"))


def _build_oauth1(auth_config: dict) -> OAuth1Signer:
    """Construit le signataire OAuth 1.0a. Les quatre credentials (consumer +
    token) sont des références de secret comme partout ailleurs."""
    kwargs = {
        "consumer_key": _resolve(auth_config, "consumer_key"),
        "consumer_secret": _resolve(auth_config, "consumer_secret"),
        "realm": auth_config.get("realm"),
        "signature_method": auth_config.get("signature_method", "HMAC-SHA256"),
    }
    # Le token est optionnel : OAuth 1.0a « two-legged » n'en a pas. Les APIs
    # à jetons applicatifs en fournissent toujours un.
    if "token" in auth_config or "token_env_var" in auth_config:
        kwargs["token"] = _resolve(auth_config, "token")
        kwargs["token_secret"] = _resolve(auth_config, "token_secret")
    try:
        return OAuth1Signer(**kwargs)
    except ValueError as exc:
        raise AuthError(str(exc)) from exc


class AuthProvider:
    """Porte les headers d'authentification d'un run et sait les régénérer.

    Le token d'un `oauth2_client_credentials` ou d'un `token_endpoint` expire
    — 60 minutes est courant. Un run qui dépasse cette durée voyait ses
    dernières pages répondre 401, un statut non rejouable : le run échouait
    en entier. Le provider renouvelle le token de deux façons :

    - par anticipation, quand le endpoint a annoncé une durée de vie
      (`expires_in`) et qu'elle est sur le point d'être atteinte ;
    - en réaction à un 401, pour les endpoints qui n'annoncent rien.

    Les auth à credential statique (bearer, api_key, basic, oauth1) ne sont
    pas renouvelables : leur 401 est une erreur de configuration, et le
    rejouer ne ferait que retarder le diagnostic.
    """

    def __init__(self, auth_config: dict | None):
        self._config = auth_config or {}
        self._type = self._config.get("type") or "none"
        self.refreshable = self._type in REFRESHABLE_TYPES
        self.signer = _build_oauth1(self._config) if self._type == "oauth1" else None
        self._headers: dict[str, str] | None = None
        self._expires_at: float | None = None

    def headers(self) -> dict[str, str]:
        """Headers courants. Les régénère si le token est expiré ou sur le
        point de l'être."""
        if self._headers is None or self._is_expiring():
            self._fetch()
        return self._headers

    def refresh(self) -> dict[str, str]:
        """Force la régénération, quelle que soit l'expiration annoncée."""
        self._fetch()
        return self._headers

    def _is_expiring(self) -> bool:
        if self._expires_at is None:
            return False
        return time.monotonic() >= self._expires_at - TOKEN_EXPIRY_MARGIN_SECONDS

    def _fetch(self) -> None:
        if self.signer is not None:
            self._headers, self._expires_at = {}, None
            return
        headers, expires_in = _build_headers(self._config)
        self._headers = headers
        self._expires_at = (
            time.monotonic() + expires_in if expires_in is not None else None
        )


def build_auth(auth_config: dict | None) -> tuple[dict[str, str], object | None]:
    """Point d'entrée historique : retourne (headers statiques, signataire par
    requête). Ne renouvelle rien — pour un run susceptible de dépasser
    l'expiration du token, utiliser AuthProvider.

    Le signataire — un `requests.auth.AuthBase` — vaut None pour toutes les
    stratégies dont l'auth tient dans un header fixe ; il n'est renseigné que
    pour oauth1, dont la signature dépend de l'URL et des paramètres de chaque
    requête et doit donc être recalculée page après page.
    """
    provider = AuthProvider(auth_config)
    return provider.headers(), provider.signer


def build_auth_headers(auth_config: dict | None) -> dict[str, str]:
    """Construit les headers HTTP d'authentification à partir de la config.

    Types supportés : bearer_token, api_key_header, basic,
    oauth2_client_credentials (service principal Entra ID ou IdP standard),
    token_endpoint (login API arbitraire). oauth1 n'entre pas dans ce cadre :
    passer par build_auth().
    """
    return _build_headers(auth_config)[0]


def _build_headers(auth_config: dict | None) -> tuple[dict[str, str], float | None]:
    """Headers d'auth et durée de vie du token quand l'endpoint l'annonce."""
    if not auth_config or auth_config.get("type") in (None, "none"):
        return {}, None

    auth_type = auth_config["type"]

    if auth_type == "bearer_token":
        token = _resolve(auth_config, "token")
        header_name = auth_config.get("header_name", "Authorization")
        value_prefix = auth_config.get("value_prefix", "Bearer ")
        return {header_name: f"{value_prefix}{token}"}, None

    if auth_type == "api_key_header":
        key = _resolve(auth_config, "key")
        header_name = auth_config.get("header_name", "X-API-Key")
        return {header_name: key}, None

    if auth_type == "basic":
        username = _resolve(auth_config, "username")
        password = _resolve(auth_config, "password")
        encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}, None

    if auth_type == "oauth2_client_credentials":
        token, expires_in = _fetch_oauth2_client_credentials(auth_config)
        return {"Authorization": f"Bearer {token}"}, expires_in

    if auth_type == "token_endpoint":
        token, expires_in = _fetch_token_endpoint(auth_config)
        header_name = auth_config.get("header_name", "Authorization")
        value_prefix = auth_config.get("value_prefix", "Bearer ")
        return {header_name: f"{value_prefix}{token}"}, expires_in

    if auth_type == "oauth1":
        raise AuthError(
            "oauth1 signe chaque requête et ne tient pas dans un header fixe — "
            "utiliser build_auth()"
        )

    raise AuthError(f"Type d'auth inconnu : '{auth_type}'")
