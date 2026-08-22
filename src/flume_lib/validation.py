"""Validation stricte de la configuration d'une source.

Toute clé inconnue est une erreur : une faute de frappe sur une clé optionnelle
(`pagintaion` au lieu de `pagination`) passerait sinon en silence — la lib
ferait un appel unique, le run serait marqué `success` et la majorité des
données seraient perdues sans aucun signal."""

import difflib

from flume_lib.oauth1 import SIGNATURE_METHODS
from flume_lib.templating import VALUE_FORMATS

# Clés top-level
_REQUIRED = ("base_url", "target_schema", "target_table")
_OPTIONAL = (
    "name", "params", "auth", "pagination", "incremental", "retry",
    "timeout_seconds", "method", "body", "body_format", "headers",
)

# Clés autorisées par type d'auth. Les formes historiques *_env_var sont
# acceptées au même titre que les références de secret.
_AUTH_KEYS = {
    "none": (),
    "bearer_token": ("token", "token_env_var", "header_name", "value_prefix"),
    "api_key_header": ("key", "key_env_var", "header_name"),
    "basic": (
        "username", "password", "username_env_var", "password_env_var",
    ),
    "oauth2_client_credentials": (
        "token_url", "tenant_id", "client_id", "client_secret",
        "client_id_env_var", "client_secret_env_var", "scope", "timeout_seconds",
    ),
    "token_endpoint": (
        "token_url", "method", "body", "body_format", "headers",
        "token_json_path", "header_name", "value_prefix", "timeout_seconds",
    ),
    "oauth1": (
        "consumer_key", "consumer_secret", "token", "token_secret",
        "consumer_key_env_var", "consumer_secret_env_var",
        "token_env_var", "token_secret_env_var",
        "realm", "signature_method",
    ),
}

# Groupes de clés dont au moins une doit être présente
_AUTH_REQUIRED = {
    "bearer_token": (("token", "token_env_var"),),
    "api_key_header": (("key", "key_env_var"),),
    "basic": (
        ("username", "username_env_var"),
        ("password", "password_env_var"),
    ),
    "oauth2_client_credentials": (
        ("token_url", "tenant_id"),
        ("client_id", "client_id_env_var"),
        ("client_secret", "client_secret_env_var"),
    ),
    "token_endpoint": (("token_url",),),
    "oauth1": (
        ("consumer_key", "consumer_key_env_var"),
        ("consumer_secret", "consumer_secret_env_var"),
    ),
}

# Clés acceptées par toutes les stratégies de pagination
_PAGINATION_COMMON = ("items_field", "params_in")
_PAGINATION_KEYS = {
    "none": (),
    "offset": ("limit", "limit_param", "offset_param"),
    "page": (
        "page_param", "start_page", "size_param", "page_size",
        "total_pages_header",
    ),
    "next_link": ("next_field",),
    "cursor": ("cursor_param", "cursor_field"),
}

_INCREMENTAL_KEYS = (
    "enabled", "field", "param_name", "inject", "placeholder",
    "initial_value", "value_format",
)
_INCREMENTAL_INJECTS = ("query_param", "body_template")
_RETRY_KEYS = ("max_attempts", "backoff_multiplier", "max_retry_after_seconds")


class ConfigError(Exception):
    pass


def _check_unknown(section: str, config: dict, allowed) -> None:
    for key in config:
        if key in allowed:
            continue
        suggestion = difflib.get_close_matches(key, sorted(allowed), n=1)
        hint = f" — vouliez-vous dire '{suggestion[0]}' ?" if suggestion else ""
        raise ConfigError(f"{section} : clé inconnue '{key}'{hint}")


def _check_required_groups(section: str, config: dict, groups) -> None:
    for group in groups:
        if not any(key in config for key in group):
            names = " ou ".join(f"'{k}'" for k in group)
            raise ConfigError(f"{section} : {names} requis")


def _check_type(section: str, config: dict, allowed_types: dict, default: str):
    if not isinstance(config, dict):
        raise ConfigError(f"{section} : doit être un objet")
    type_name = config.get("type", default)
    if type_name not in allowed_types:
        known = ", ".join(sorted(allowed_types))
        raise ConfigError(
            f"{section} : type inconnu '{type_name}' — attendu l'un de : {known}"
        )
    return type_name


def validate_config(config: dict) -> None:
    """Valide la configuration d'une source. Lève ConfigError au premier
    problème rencontré, avec un message actionnable."""
    if not isinstance(config, dict):
        raise ConfigError("La configuration d'une source doit être un objet")

    _check_unknown("config", config, _REQUIRED + _OPTIONAL)
    for key in _REQUIRED:
        if not config.get(key):
            raise ConfigError(f"config : '{key}' requis")

    method = str(config.get("method", "GET")).upper()
    if method not in ("GET", "POST", "PUT", "PATCH"):
        raise ConfigError(f"config : méthode HTTP non supportée '{method}'")
    if "body" in config and method == "GET":
        raise ConfigError(
            "config : 'body' est ignoré en GET — préciser \"method\": \"POST\""
        )

    headers = config.get("headers")
    if headers is not None:
        if not isinstance(headers, dict):
            raise ConfigError("headers : doit être un objet")
        for key, value in headers.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ConfigError(
                    f"headers : '{key}' doit être une chaîne littérale — pour un "
                    "credential, utiliser 'auth' (api_key_header, bearer_token…)"
                )

    auth = config.get("auth")
    if auth is not None:
        auth_type = _check_type("auth", auth, _AUTH_KEYS, "none")
        _check_unknown("auth", auth, ("type",) + _AUTH_KEYS[auth_type])
        _check_required_groups("auth", auth, _AUTH_REQUIRED.get(auth_type, ()))
        if auth_type == "oauth1":
            signature_method = auth.get("signature_method", "HMAC-SHA256")
            if signature_method not in SIGNATURE_METHODS:
                known = ", ".join(sorted(SIGNATURE_METHODS))
                raise ConfigError(
                    f"auth : 'signature_method' inconnue '{signature_method}' — "
                    f"attendu l'une de : {known}"
                )
            has_token = "token" in auth or "token_env_var" in auth
            has_secret = "token_secret" in auth or "token_secret_env_var" in auth
            if has_token != has_secret:
                raise ConfigError(
                    "auth : 'token' et 'token_secret' vont par paire — "
                    "les omettre tous les deux donne un OAuth 1.0a two-legged"
                )
        if auth_type == "token_endpoint":
            token_method = str(auth.get("method", "POST")).upper()
            if token_method == "GET" and any(
                isinstance(v, dict) for v in auth.get("body", {}).values()
            ):
                raise ConfigError(
                    "auth : référence de secret interdite dans 'body' en GET — "
                    "les paramètres partent dans l'URL (logs serveurs, proxies)"
                )

    pagination = config.get("pagination")
    if pagination is not None:
        pagination_type = _check_type(
            "pagination", pagination, _PAGINATION_KEYS, "none"
        )
        _check_unknown(
            "pagination",
            pagination,
            ("type",) + _PAGINATION_COMMON + _PAGINATION_KEYS[pagination_type],
        )
        params_in = pagination.get("params_in", "query")
        if params_in not in ("query", "body"):
            raise ConfigError(
                f"pagination : 'params_in' doit valoir 'query' ou 'body', pas '{params_in}'"
            )
        if params_in == "body" and method == "GET":
            raise ConfigError(
                "pagination : \"params_in\": \"body\" nécessite une méthode POST"
            )

    incremental = config.get("incremental")
    if incremental is not None:
        if not isinstance(incremental, dict):
            raise ConfigError("incremental : doit être un objet")
        _check_unknown("incremental", incremental, _INCREMENTAL_KEYS)
        inject = incremental.get("inject", "query_param")
        if inject not in _INCREMENTAL_INJECTS:
            known = ", ".join(_INCREMENTAL_INJECTS)
            raise ConfigError(
                f"incremental : 'inject' inconnu '{inject}' — attendu l'un de : {known}"
            )
        value_format = incremental.get("value_format", "any")
        if value_format not in VALUE_FORMATS:
            known = ", ".join(VALUE_FORMATS)
            raise ConfigError(
                f"incremental : 'value_format' inconnu '{value_format}' — "
                f"attendu l'un de : {known}"
            )
        if incremental.get("enabled"):
            if not incremental.get("field"):
                raise ConfigError(
                    "incremental : 'field' requis quand 'enabled' est vrai"
                )
            if inject == "body_template":
                if not config.get("body"):
                    raise ConfigError(
                        "incremental : \"inject\": \"body_template\" nécessite un "
                        "'body' contenant le placeholder à substituer"
                    )
                if value_format == "any":
                    known = ", ".join(f for f in VALUE_FORMATS if f != "any")
                    raise ConfigError(
                        "incremental : 'value_format' explicite requis avec "
                        "\"inject\": \"body_template\" — attendu l'un de : "
                        f"{known}. Le filtrage des caractères ne protège qu'un "
                        "placeholder entre quotes ; un placeholder nu "
                        "(WHERE id > {last_id}) accepterait '0 OR 1=1'"
                    )
            elif not incremental.get("param_name"):
                raise ConfigError(
                    "incremental : 'param_name' requis quand 'enabled' est vrai "
                    "(ou \"inject\": \"body_template\" pour injecter dans le corps)"
                )

    retry = config.get("retry")
    if retry is not None:
        if not isinstance(retry, dict):
            raise ConfigError("retry : doit être un objet")
        _check_unknown("retry", retry, _RETRY_KEYS)
