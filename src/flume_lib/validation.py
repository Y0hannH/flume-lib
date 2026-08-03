"""Validation stricte de la configuration d'une source.

Toute clé inconnue est une erreur : une faute de frappe sur une clé optionnelle
(`pagintaion` au lieu de `pagination`) passerait sinon en silence — la lib
ferait un appel unique, le run serait marqué `success` et la majorité des
données seraient perdues sans aucun signal."""

import difflib

# Clés top-level
_REQUIRED = ("base_url", "target_schema", "target_table")
_OPTIONAL = (
    "name", "params", "auth", "pagination", "incremental", "retry",
    "timeout_seconds", "method", "body", "body_format",
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

_INCREMENTAL_KEYS = ("enabled", "field", "param_name")
_RETRY_KEYS = ("max_attempts", "backoff_multiplier")


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

    auth = config.get("auth")
    if auth is not None:
        auth_type = _check_type("auth", auth, _AUTH_KEYS, "none")
        _check_unknown("auth", auth, ("type",) + _AUTH_KEYS[auth_type])
        _check_required_groups("auth", auth, _AUTH_REQUIRED.get(auth_type, ()))
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
        if incremental.get("enabled"):
            for key in ("field", "param_name"):
                if not incremental.get(key):
                    raise ConfigError(
                        f"incremental : '{key}' requis quand 'enabled' est vrai"
                    )

    retry = config.get("retry")
    if retry is not None:
        if not isinstance(retry, dict):
            raise ConfigError("retry : doit être un objet")
        _check_unknown("retry", retry, _RETRY_KEYS)
