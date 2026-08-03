"""Stratégies d'authentification. Les credentials sont résolus au runtime
depuis des variables d'environnement ou Azure Key Vault (voir secrets_.py),
jamais lus depuis la config en clair."""

import base64

import requests

from flume_lib.secrets_ import SecretResolutionError, resolve_secret

DEFAULT_TOKEN_TIMEOUT_SECONDS = 30
ENTRA_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


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


def _fetch_oauth2_client_credentials(auth_config: dict) -> str:
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
    token = response.json().get("access_token")
    if not token:
        raise AuthError("oauth2_client_credentials : 'access_token' absent de la réponse")
    return token


def _fetch_token_endpoint(auth_config: dict) -> str:
    """Obtient un token via un appel API de login arbitraire. Les valeurs de
    'body' et 'headers' peuvent être des littéraux (valeurs non sensibles) ou
    des références de secret (env_var / keyvault_url)."""
    token_url = auth_config.get("token_url")
    if not token_url:
        raise AuthError("token_endpoint : 'token_url' requis")

    body = {
        key: resolve_secret(ref, f"body.{key}")
        for key, ref in auth_config.get("body", {}).items()
    }
    headers = {
        key: resolve_secret(ref, f"headers.{key}")
        for key, ref in auth_config.get("headers", {}).items()
    }

    method = auth_config.get("method", "POST").upper()
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

    token = _extract_json_path(response.json(), auth_config.get("token_json_path", "access_token"))
    if not token:
        raise AuthError("token_endpoint : token vide dans la réponse")
    return token


def build_auth_headers(auth_config: dict | None) -> dict[str, str]:
    """Construit les headers HTTP d'authentification à partir de la config.

    Types supportés : bearer_token, api_key_header, basic,
    oauth2_client_credentials (service principal Entra ID ou IdP standard),
    token_endpoint (login API arbitraire).

    Le token est obtenu une fois par run — pour les runs très longs dépassant
    l'expiration du token, prévoir un découpage des sources.
    """
    if not auth_config or auth_config.get("type") in (None, "none"):
        return {}

    auth_type = auth_config["type"]

    if auth_type == "bearer_token":
        token = _resolve(auth_config, "token")
        return {"Authorization": f"Bearer {token}"}

    if auth_type == "api_key_header":
        key = _resolve(auth_config, "key")
        header_name = auth_config.get("header_name", "X-API-Key")
        return {header_name: key}

    if auth_type == "basic":
        username = _resolve(auth_config, "username")
        password = _resolve(auth_config, "password")
        encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    if auth_type == "oauth2_client_credentials":
        token = _fetch_oauth2_client_credentials(auth_config)
        return {"Authorization": f"Bearer {token}"}

    if auth_type == "token_endpoint":
        token = _fetch_token_endpoint(auth_config)
        header_name = auth_config.get("header_name", "Authorization")
        value_prefix = auth_config.get("value_prefix", "Bearer ")
        return {header_name: f"{value_prefix}{token}"}

    raise AuthError(f"Type d'auth inconnu : '{auth_type}'")
