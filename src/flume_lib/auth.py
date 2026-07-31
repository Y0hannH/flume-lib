"""Stratégies d'authentification. Les credentials sont résolus depuis des
variables d'environnement au runtime, jamais lus depuis la config en clair."""

import base64
import os


class AuthError(Exception):
    pass


def _resolve_env(auth_config: dict, key: str) -> str:
    env_var = auth_config.get(key)
    if not env_var:
        raise AuthError(f"Clé '{key}' manquante dans la config auth")
    value = os.environ.get(env_var)
    if value is None:
        raise AuthError(f"Variable d'environnement '{env_var}' non définie")
    return value


def build_auth_headers(auth_config: dict | None) -> dict[str, str]:
    """Construit les headers HTTP d'authentification à partir de la config.

    Types supportés : bearer_token, api_key_header, basic.
    oauth2_client_credentials : stub, non implémenté.
    """
    if not auth_config or auth_config.get("type") in (None, "none"):
        return {}

    auth_type = auth_config["type"]

    if auth_type == "bearer_token":
        token = _resolve_env(auth_config, "token_env_var")
        return {"Authorization": f"Bearer {token}"}

    if auth_type == "api_key_header":
        key = _resolve_env(auth_config, "key_env_var")
        header_name = auth_config.get("header_name", "X-API-Key")
        return {header_name: key}

    if auth_type == "basic":
        username = _resolve_env(auth_config, "username_env_var")
        password = _resolve_env(auth_config, "password_env_var")
        encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    if auth_type == "oauth2_client_credentials":
        raise NotImplementedError(
            "oauth2_client_credentials n'est pas encore implémenté"
        )

    raise AuthError(f"Type d'auth inconnu : '{auth_type}'")
