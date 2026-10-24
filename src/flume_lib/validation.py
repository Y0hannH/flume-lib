"""Validation stricte de la configuration d'une source.

Toute clé inconnue est une erreur : une faute de frappe sur une clé optionnelle
(`pagintaion` au lieu de `pagination`) passerait sinon en silence — la lib
ferait un appel unique, le run serait marqué `success` et la majorité des
données seraient perdues sans aucun signal."""

import difflib
import re
from typing import NamedTuple
from urllib.parse import urlsplit

from flume_lib._delta import parse_column_type
from flume_lib.auth import CLIENT_AUTH_MODES
from flume_lib.oauth1 import SIGNATURE_METHODS
from flume_lib.templating import NORMALIZERS, VALUE_FORMATS, templated_placeholders

# Clés top-level
_REQUIRED = ("base_url", "target_schema", "target_table")
_OPTIONAL = (
    "name", "params", "auth", "pagination", "incremental", "retry",
    "timeout_seconds", "method", "body", "body_format", "headers",
    "errors", "template_paths", "batch_size", "write", "allow_body_on_get",
    "fail_on_empty", "allow_insecure_http", "allow_literal_secrets",
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
        "client_id_env_var", "client_secret_env_var", "scope", "client_auth",
        "urlencode_credentials", "timeout_seconds",
    ),
    "token_endpoint": (
        "token_url", "method", "body", "body_format", "headers",
        "token_json_path", "expires_in_json_path", "header_name",
        "value_prefix", "timeout_seconds",
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
_PAGINATION_COMMON = (
    "items_field", "record_field", "params_in", "params_path",
    "max_pages", "max_rows",
)
_PAGINATION_KEYS = {
    "none": (),
    "offset": ("limit", "limit_param", "offset_param"),
    "page": (
        "page_param", "start_page", "size_param", "page_size",
        "total_pages_header", "total_pages_field",
    ),
    "next_link": ("next_field", "allowed_hosts"),
    "cursor": (
        "cursor_param", "cursor_field", "has_more_field", "limit", "limit_param",
    ),
    "keyset": (
        "key_field", "key_param", "initial_value", "value_format",
        "limit", "limit_param",
    ),
}

_PARAMS_IN = ("query", "body", "body_template")

_ERRORS_KEYS = ("path", "code_field", "message_field", "retryable_codes")

_INCREMENTAL_KEYS = (
    "enabled", "field", "param_name", "inject", "placeholder",
    "initial_value", "value_format", "normalize", "checkpoint",
    "allow_regression", "window", "advance", "lookback",
)
_INCREMENTAL_INJECTS = ("query_param", "body_template")
_INCREMENTAL_WINDOW_KEYS = ("upper_param", "upper_value")
_INCREMENTAL_ADVANCES = ("field_max", "field_max_capped", "upper_bound")
_RETRY_KEYS = ("max_attempts", "backoff_multiplier", "max_retry_after_seconds")

# Champs d'auth dont la valeur est un secret : une chaîne littérale y est
# refusée. Les identifiants publics (client_id, username, consumer_key) n'y
# figurent pas. Les formes historiques `*_env_var` sont des références, pas des
# littéraux, et ne passent jamais par là.
_AUTH_SECRET_FIELDS = {
    "bearer_token": ("token",),
    "api_key_header": ("key",),
    "basic": ("password",),
    "oauth2_client_credentials": ("client_secret",),
    "oauth1": ("consumer_secret", "token", "token_secret"),
}

# Nom d'une clé de header ou de corps qui porte un secret. Les valeurs de
# `token_endpoint.body` / `.headers` et de `headers` peuvent être littérales —
# un grant_type, un nom d'utilisateur — mais pas celles-ci.
_SENSITIVE_NAME_RE = re.compile(
    r"secret|passw|token|api[-_]?key|credential|authorization|signature",
    re.IGNORECASE,
)

_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")

_WRITE_KEYS = (
    "mode", "replace_where", "partition_by", "merge_keys", "when_matched",
    "column_types",
)
_WRITE_MODES = ("append", "overwrite", "replace_where", "merge")
_WRITE_WHEN_MATCHED = ("update",)


class ConfigError(Exception):
    pass


def _check_unknown(section: str, config: dict, allowed) -> None:
    for key in config:
        if key in allowed:
            continue
        suggestion = difflib.get_close_matches(key, sorted(allowed), n=1)
        hint = f" — did you mean '{suggestion[0]}'?" if suggestion else ""
        raise ConfigError(f"{section}: unknown key '{key}'{hint}")


def _check_required_groups(section: str, config: dict, groups) -> None:
    for group in groups:
        if not any(key in config for key in group):
            names = " or ".join(f"'{k}'" for k in group)
            raise ConfigError(f"{section}: {names} required")


def _resolve_body_path(section: str, body, path: str, required: bool):
    """Suit un chemin pointé dans `body` et retourne la valeur atteinte, ou
    None si le chemin s'interrompt et que ce n'est pas une erreur. Traverser
    autre chose qu'un objet est toujours une erreur : le chemin ne décrit pas
    la config qu'on lui donne."""
    parts = path.split(".")
    node = body
    for depth, part in enumerate(parts):
        if not isinstance(node, dict):
            prefix = ".".join(parts[:depth]) or "body"
            raise ConfigError(
                f"{section}: '{prefix}' is not an object inside 'body'"
            )
        if part not in node:
            if required:
                raise ConfigError(
                    f"{section}: path '{path}' not found in 'body'"
                )
            return None
        node = node[part]
    return node


def _is_local_host(host: str) -> bool:
    return host in _LOCAL_HOSTS or host.endswith(".localhost")


def _check_url(label: str, url, allow_insecure_http: bool) -> None:
    """Contrôle d'une URL de configuration qui recevra des credentials.

    Pas d'identifiants dans l'URL : ils finiraient dans les messages d'erreur
    et les logs de proxy, et `auth` existe pour cela. Pas de `http://` : le
    token et les données voyageraient en clair — sauf vers la machine locale,
    ou avec `allow_insecure_http` pour un réseau de confiance (une recette
    interne, un bac à sable)."""
    if not isinstance(url, str) or not url.strip():
        raise ConfigError(f"{label}: must be a URL string")
    try:
        parts = urlsplit(url.strip())
        host = (parts.hostname or "").lower()
        _ = parts.port  # lève ValueError sur un port illisible
    except ValueError as exc:
        raise ConfigError(f"{label}: not a valid URL ({exc})") from None
    if parts.scheme.lower() not in ("http", "https") or not host:
        raise ConfigError(
            f"{label}: must be an absolute http(s) URL, e.g. https://api.example.com/v1"
        )
    if "@" in parts.netloc:
        raise ConfigError(
            f"{label}: must not embed credentials (user:password@host) — they "
            "would be copied into logs and error messages; declare them under "
            "'auth' as secret references"
        )
    if (
        parts.scheme.lower() == "http"
        and not allow_insecure_http
        and not _is_local_host(host)
    ):
        raise ConfigError(
            f"{label}: plain http:// sends credentials and data in clear text — "
            'use https://, or set "allow_insecure_http": true if the network is '
            "trusted (localhost is always accepted)"
        )


def _check_literal_secret(section: str, key: str, value, allow: bool) -> None:
    """Refuse une chaîne littérale là où un secret est attendu. Le message ne
    recopie jamais la valeur."""
    if allow or not isinstance(value, str):
        return
    raise ConfigError(
        f"{section}: '{key}' is a literal string — a secret must not sit in the "
        'configuration. Reference it: {"env_var": "NAME"} or '
        '{"keyvault_url": "...", "secret_name": "..."}. For a throwaway local '
        'test only, set "allow_literal_secrets": true.'
    )


def _check_number(
    section: str, config: dict, key: str, *, integer: bool = True,
    allow_zero: bool = False,
) -> None:
    """Contrôle la valeur d'une clé numérique optionnelle.

    Jusqu'ici la validation ne regardait que les *noms* des clés de `retry` et
    de `timeout_seconds`. Un `"timeout_seconds": null` dans le JSON passait,
    `requests` recevait `timeout=None`, et le notebook attendait
    indéfiniment — sans timeout, donc sans le retry qui aurait dû suivre.
    """
    if key not in config:
        return
    value = config[key]
    # bool est un int en Python : le laisser passer donnerait 1 ou 0
    expected = int if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, expected):
        kind = "an integer" if integer else "a number"
        raise ConfigError(f"{section}: '{key}' must be {kind}")
    if allow_zero:
        if value < 0:
            raise ConfigError(f"{section}: '{key}' must not be negative")
    elif value <= 0:
        raise ConfigError(f"{section}: '{key}' must be greater than 0")


def _check_type(section: str, config: dict, allowed_types: dict, default: str):
    if not isinstance(config, dict):
        raise ConfigError(f"{section}: must be an object")
    type_name = config.get("type", default)
    if not isinstance(type_name, str) or type_name not in allowed_types:
        known = ", ".join(sorted(allowed_types))
        raise ConfigError(
            f"{section}: unknown type '{type_name}' — expected one of: {known}"
        )
    return type_name


def _validate_incremental_options(config: dict, incremental: dict) -> None:
    """Contrôles de `allow_regression`, `window`, `advance` et `lookback`.

    Statiques : tout ce qui se décide sans connaître le watermark. La borne
    haute contre le point de départ est vérifiée à l'exécution, quand ce
    dernier est relu."""
    if "allow_regression" in incremental and not isinstance(
        incremental["allow_regression"], bool
    ):
        raise ConfigError("incremental: 'allow_regression' must be a boolean")

    advance = incremental.get("advance", "field_max")
    if advance not in _INCREMENTAL_ADVANCES:
        known = ", ".join(f"'{a}'" for a in _INCREMENTAL_ADVANCES)
        raise ConfigError(
            f"incremental: unknown 'advance' '{advance}' — expected one of: {known}"
        )
    _check_number("incremental", incremental, "lookback", allow_zero=True)

    window = incremental.get("window")
    if advance != "field_max" and window is None:
        raise ConfigError(
            f"incremental: \"advance\": \"{advance}\" needs a 'window' — "
            "it is the window's upper bound that the watermark is capped to, "
            "or moved to"
        )

    if not incremental.get("enabled"):
        for key in ("window", "advance", "lookback", "allow_regression"):
            if key in incremental:
                raise ConfigError(
                    f"incremental: '{key}' only makes sense with \"enabled\": true"
                )
        return

    if window is None:
        return
    if not isinstance(window, dict):
        raise ConfigError("incremental: 'window' must be an object")
    _check_unknown("incremental.window", window, _INCREMENTAL_WINDOW_KEYS)
    for key in _INCREMENTAL_WINDOW_KEYS:
        if not isinstance(window.get(key), str) or not window[key]:
            raise ConfigError(
                f"incremental.window: '{key}' required, as a non-empty string"
            )
    upper_param, upper_value = window["upper_param"], window["upper_value"]
    params = config.get("params") or {}
    if upper_param not in params:
        known = ", ".join(sorted(params)) or "none"
        raise ConfigError(
            f"incremental.window: 'upper_param' '{upper_param}' is not a key of "
            f"'params' (found: {known}) — the window's upper bound must be the "
            "value the request actually carries"
        )
    if str(params[upper_param]) != upper_value:
        raise ConfigError(
            f"incremental.window: params['{upper_param}'] is "
            f"{str(params[upper_param])!r} but 'upper_value' is {upper_value!r} — "
            "the bound sent and the bound the watermark is capped to must be "
            "the same value"
        )
    if upper_param == incremental.get("param_name"):
        raise ConfigError(
            "incremental.window: 'upper_param' is also the incremental "
            "'param_name' — the lower and upper bounds need distinct params"
        )


class _Flags(NamedTuple):
    """Ce que le premier niveau apprend à la config, et dont les sections
    suivantes ont besoin."""

    method: str
    allow_body_on_get: bool
    allow_http: bool
    allow_literals: bool


def _validate_top_level(config: dict) -> _Flags:
    """Clés racine, méthode HTTP, drapeaux booléens et `base_url`."""
    if not isinstance(config, dict):
        raise ConfigError("A source configuration must be an object")

    _check_unknown("config", config, _REQUIRED + _OPTIONAL)
    for key in _REQUIRED:
        if not config.get(key):
            raise ConfigError(f"config: '{key}' required")

    method = str(config.get("method", "GET")).upper()
    if method not in ("GET", "POST", "PUT", "PATCH"):
        raise ConfigError(f"config: unsupported HTTP method '{method}'")
    allow_body_on_get = config.get("allow_body_on_get", False)
    if not isinstance(allow_body_on_get, bool):
        raise ConfigError("config: 'allow_body_on_get' must be a boolean")
    if "body" in config and method == "GET" and not allow_body_on_get:
        raise ConfigError(
            "config: 'body' is ignored on GET — set \"method\": \"POST\""
        )

    fail_on_empty = config.get("fail_on_empty", False)
    if not isinstance(fail_on_empty, bool):
        raise ConfigError("config: 'fail_on_empty' must be a boolean")
    for flag in ("allow_insecure_http", "allow_literal_secrets"):
        if not isinstance(config.get(flag, False), bool):
            raise ConfigError(f"config: '{flag}' must be a boolean")
    allow_http = config.get("allow_insecure_http", False)
    allow_literals = config.get("allow_literal_secrets", False)

    _check_url("config: 'base_url'", config["base_url"], allow_http)

    _check_number("config", config, "batch_size")
    _check_number("config", config, "timeout_seconds", integer=False)
    if "params" in config and not isinstance(config["params"], dict):
        raise ConfigError("params: must be an object")
    return _Flags(method, allow_body_on_get, allow_http, allow_literals)


def _validate_headers(config: dict, allow_literals: bool) -> None:
    """En-têtes fixes : littéraux, et sans secret."""
    headers = config.get("headers")
    if headers is None:
        return
    if not isinstance(headers, dict):
        raise ConfigError("headers: must be an object")
    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConfigError(
                f"headers: '{key}' must be a literal string — for a "
                "credential, use 'auth' (api_key_header, bearer_token…)"
            )
        if _SENSITIVE_NAME_RE.search(key):
            _check_literal_secret("headers", key, value, allow_literals)


def _validate_errors(config: dict) -> None:
    """Enveloppe d'erreur applicative d'une réponse 200."""
    errors_config = config.get("errors")
    if errors_config is None:
        return
    if not isinstance(errors_config, dict):
        raise ConfigError("errors: must be an object")
    _check_unknown("errors", errors_config, _ERRORS_KEYS)
    for key in ("path", "code_field", "message_field"):
        if key in errors_config and not (
            isinstance(errors_config[key], str) and errors_config[key]
        ):
            raise ConfigError(f"errors: '{key}' must be a non-empty string")
    codes = errors_config.get("retryable_codes")
    if codes is not None and not isinstance(codes, (list, tuple)):
        raise ConfigError("errors: 'retryable_codes' must be a list")


def _validate_template_paths(config: dict) -> None:
    """Branches du corps où le templating s'applique."""
    template_paths = config.get("template_paths")
    if template_paths is None:
        return
    if not isinstance(template_paths, (list, tuple)) or not all(
        isinstance(path, str) and path for path in template_paths
    ):
        raise ConfigError(
            "template_paths: must be a list of non-empty paths"
        )
    if not isinstance(config.get("body"), dict):
        raise ConfigError(
            "template_paths: only restricts templating of 'body', which is "
            "absent from the config"
        )
    for path in template_paths:
        _resolve_body_path("template_paths", config["body"], path, required=True)


def _validate_token_endpoint_shape(auth: dict) -> None:
    """`body` et `headers` sont des objets ; les chemins JSON, des chaînes. Sans
    ce contrôle, un `body` en liste ou un `token_json_path` numérique passait la
    validation et plantait plus loin, hors de `ConfigError`."""
    for section in ("body", "headers"):
        if section in auth and not isinstance(auth[section], dict):
            raise ConfigError(f"auth: '{section}' must be an object")
    for key in ("token_json_path", "expires_in_json_path", "token_url", "header_name",
                "value_prefix"):
        if key in auth and not (isinstance(auth[key], str) and auth[key]):
            raise ConfigError(f"auth: '{key}' must be a non-empty string")


def _validate_auth_secrets(
    auth: dict, auth_type: str, allow_http: bool, allow_literals: bool
) -> None:
    """Pas de secret littéral, et une URL de token acceptable."""
    for secret_field in _AUTH_SECRET_FIELDS.get(auth_type, ()):
        if secret_field in auth:
            _check_literal_secret(
                "auth", secret_field, auth[secret_field], allow_literals
            )
    if "token_url" in auth:
        _check_url("auth: 'token_url'", auth["token_url"], allow_http)
    if auth_type == "token_endpoint":
        for section in ("body", "headers"):
            for key, ref in (auth.get(section) or {}).items():
                if _SENSITIVE_NAME_RE.search(str(key)):
                    _check_literal_secret(
                        f"auth.{section}", key, ref, allow_literals
                    )


def _validate_oauth1(auth: dict) -> None:
    signature_method = auth.get("signature_method", "HMAC-SHA256")
    if signature_method not in SIGNATURE_METHODS:
        known = ", ".join(sorted(SIGNATURE_METHODS))
        raise ConfigError(
            f"auth: unknown 'signature_method' '{signature_method}' — "
            f"expected one of: {known}"
        )
    has_token = "token" in auth or "token_env_var" in auth
    has_secret = "token_secret" in auth or "token_secret_env_var" in auth
    if has_token != has_secret:
        raise ConfigError(
            "auth: 'token' and 'token_secret' go together — "
            "omitting both yields two-legged OAuth 1.0a"
        )


def _validate_oauth2(auth: dict) -> None:
    client_auth = auth.get("client_auth", "body")
    if client_auth not in CLIENT_AUTH_MODES:
        known = ", ".join(CLIENT_AUTH_MODES)
        raise ConfigError(
            f"auth: unknown 'client_auth' '{client_auth}' — "
            f"expected one of: {known}"
        )
    if "urlencode_credentials" in auth:
        if not isinstance(auth["urlencode_credentials"], bool):
            raise ConfigError("auth: 'urlencode_credentials' must be a boolean")
        if client_auth != "basic":
            raise ConfigError(
                "auth: 'urlencode_credentials' encodes the Basic header and "
                'only makes sense with "client_auth": "basic"'
            )


def _validate_token_endpoint_method(auth: dict) -> None:
    token_method = str(auth.get("method", "POST")).upper()
    if token_method == "GET" and any(
        isinstance(v, dict) for v in (auth.get("body") or {}).values()
    ):
        raise ConfigError(
            "auth: secret references are not allowed in 'body' on GET — "
            "the parameters travel in the URL (server logs, proxies)"
        )


def _validate_auth(config: dict, allow_http: bool, allow_literals: bool) -> None:
    """Bloc `auth` : type, clés, secrets, URL du token."""
    auth = config.get("auth")
    if auth is None:
        return
    auth_type = _check_type("auth", auth, _AUTH_KEYS, "none")
    _check_unknown("auth", auth, ("type",) + _AUTH_KEYS[auth_type])
    _check_required_groups("auth", auth, _AUTH_REQUIRED.get(auth_type, ()))
    _check_number("auth", auth, "timeout_seconds", integer=False)
    if auth_type == "token_endpoint":
        _validate_token_endpoint_shape(auth)
    _validate_auth_secrets(auth, auth_type, allow_http, allow_literals)
    if auth_type == "oauth1":
        _validate_oauth1(auth)
    if auth_type == "oauth2_client_credentials":
        _validate_oauth2(auth)
    if auth_type == "token_endpoint":
        _validate_token_endpoint_method(auth)


# Clés de pagination qui nomment un champ de la réponse ou un paramètre de la
# requête : des chaînes. Une liste ou un nombre n'était refusé nulle part, et
# plantait à la première page — ou dans la validation elle-même.
_PAGINATION_NAME_KEYS = (
    "items_field", "record_field", "limit_param", "offset_param",
    "page_param", "size_param", "cursor_param", "cursor_field", "has_more_field",
    "key_field", "key_param", "next_field", "total_pages_header", "total_pages_field",
)


def _validate_pagination_names(pagination: dict) -> None:
    for key in _PAGINATION_NAME_KEYS:
        value = pagination.get(key)
        if value is not None and not (isinstance(value, str) and value):
            raise ConfigError(f"pagination: '{key}' must be a non-empty string")


def _validate_params_in(
    config: dict, pagination: dict, method: str, allow_body_on_get: bool
) -> str:
    """Où vont les paramètres de pagination ; rend `params_in`."""
    params_in = pagination.get("params_in", "query")
    if params_in not in _PARAMS_IN:
        known = ", ".join(f"'{v}'" for v in _PARAMS_IN)
        raise ConfigError(
            f"pagination: 'params_in' must be one of {known}, "
            f"not '{params_in}'"
        )
    if (
        params_in in ("body", "body_template")
        and method == "GET"
        and not allow_body_on_get
    ):
        raise ConfigError(
            f'pagination: "params_in": "{params_in}" requires a POST method'
        )
    if params_in == "body_template":
        if not isinstance(config.get("body"), dict) or not config["body"]:
            raise ConfigError(
                'pagination: "params_in": "body_template" substitutes the '
                "parameters into 'body', which is absent from the config"
            )
    return params_in


def _validate_params_path(config: dict, pagination: dict, params_in: str) -> None:
    params_path = pagination.get("params_path")
    if params_path is None:
        return
    if not isinstance(params_path, str) or not params_path:
        raise ConfigError(
            "pagination: 'params_path' must be a non-empty path"
        )
    if params_in != "body":
        raise ConfigError(
            "pagination: 'params_path' nests the parameters inside the "
            "body and therefore requires \"params_in\": \"body\""
        )
    target = _resolve_body_path(
        "pagination", config.get("body") or {}, params_path, required=False
    )
    if target is not None and not isinstance(target, dict):
        raise ConfigError(
            f"pagination: 'params_path' points at '{params_path}', which "
            "is not an object inside 'body'"
        )


def _validate_page_totals(pagination: dict) -> None:
    if pagination.get("total_pages_header") and pagination.get(
        "total_pages_field"
    ):
        raise ConfigError(
            "pagination: 'total_pages_header' and 'total_pages_field' "
            "are mutually exclusive — the total page count is read "
            "either from the headers or from the body, and declaring "
            "both leaves it unsaid which one decides"
        )
    for key in ("total_pages_header", "total_pages_field"):
        value = pagination.get(key)
        if value is not None and (not isinstance(value, str) or not value):
            raise ConfigError(
                f"pagination: '{key}' must be a non-empty string"
            )


def _validate_allowed_hosts(hosts) -> None:
    if (
        not isinstance(hosts, (list, tuple))
        or not hosts
        or not all(
            isinstance(h, str) and h and not re.search(r"[/@:\s]", h)
            for h in hosts
        )
    ):
        raise ConfigError(
            "pagination: 'allowed_hosts' must be a non-empty list of "
            "bare host names (no scheme, port or path), e.g. "
            '["cdn.example.com"]'
        )


def _validate_keyset(config: dict, pagination: dict, params_in: str) -> None:
    for key in ("key_field", "key_param"):
        if not pagination.get(key):
            raise ConfigError(
                f"pagination: '{key}' required with \"type\": \"keyset\""
            )
    value_format = pagination.get("value_format", "any")
    if value_format not in VALUE_FORMATS:
        known = ", ".join(VALUE_FORMATS)
        raise ConfigError(
            f"pagination: unknown 'value_format' '{value_format}' — "
            f"expected one of: {known}"
        )
    if params_in != "body_template":
        return
    key_param = pagination["key_param"]
    available = templated_placeholders(
        config.get("body") or {}, config.get("template_paths")
    )
    if key_param not in available:
        known = ", ".join(sorted(available)) or "none"
        raise ConfigError(
            f"pagination: placeholder '{{{key_param}}}' is missing "
            "from 'body' — the pagination key would have nowhere to "
            f"be substituted (placeholders found: {known})"
        )
    if value_format == "any":
        known = ", ".join(f for f in VALUE_FORMATS if f != "any")
        raise ConfigError(
            "pagination: an explicit 'value_format' is required when the "
            "key is interpolated into the body — expected one of: "
            f"{known}. The key comes from the API response; character "
            "filtering only protects a quoted placeholder, a bare "
            "placeholder (WHERE id > {last_key}) would accept "
            "'0 OR 1=1'"
        )


def _validate_pagination(config: dict, method: str, allow_body_on_get: bool) -> None:
    """Bloc `pagination` : stratégie, bornes, clés propres à chaque type."""
    pagination = config.get("pagination")
    if pagination is None:
        return
    pagination_type = _check_type(
        "pagination", pagination, _PAGINATION_KEYS, "none"
    )
    _check_unknown(
        "pagination",
        pagination,
        ("type",) + _PAGINATION_COMMON + _PAGINATION_KEYS[pagination_type],
    )
    _validate_pagination_names(pagination)
    params_in = _validate_params_in(config, pagination, method, allow_body_on_get)

    for key in ("max_pages", "max_rows", "limit", "page_size"):
        _check_number("pagination", pagination, key)
    # une API indexée à partir de 0 est légitime
    _check_number("pagination", pagination, "start_page", allow_zero=True)

    _validate_params_path(config, pagination, params_in)

    if pagination_type == "page":
        _validate_page_totals(pagination)
    if pagination_type == "next_link" and "allowed_hosts" in pagination:
        _validate_allowed_hosts(pagination["allowed_hosts"])
    if pagination_type == "cursor":
        for key in ("cursor_param", "cursor_field"):
            if not pagination.get(key):
                raise ConfigError(
                    f"pagination: '{key}' required with \"type\": \"cursor\""
                )
    if pagination_type == "keyset":
        _validate_keyset(config, pagination, params_in)


def _validate_incremental(config: dict) -> None:
    """Bloc `incremental` : watermark, injection, fenêtre."""
    incremental = config.get("incremental")
    if incremental is None:
        return
    if not isinstance(incremental, dict):
        raise ConfigError("incremental: must be an object")
    _check_unknown("incremental", incremental, _INCREMENTAL_KEYS)
    inject = incremental.get("inject", "query_param")
    if inject not in _INCREMENTAL_INJECTS:
        known = ", ".join(_INCREMENTAL_INJECTS)
        raise ConfigError(
            f"incremental: unknown 'inject' '{inject}' — expected one of: {known}"
        )
    value_format = incremental.get("value_format", "any")
    if value_format not in VALUE_FORMATS:
        known = ", ".join(VALUE_FORMATS)
        raise ConfigError(
            f"incremental: unknown 'value_format' '{value_format}' — "
            f"expected one of: {known}"
        )
    normalize = incremental.get("normalize", "none")
    if normalize not in NORMALIZERS:
        known = ", ".join(NORMALIZERS)
        raise ConfigError(
            f"incremental: unknown 'normalize' '{normalize}' — "
            f"expected one of: {known}"
        )
    if incremental.get("checkpoint") and not incremental.get("enabled"):
        raise ConfigError(
            "incremental: 'checkpoint' commits the watermark batch by batch "
            "and only makes sense with \"enabled\": true"
        )
    for key in ("field", "param_name", "placeholder"):
        value = incremental.get(key)
        if value is not None and not (isinstance(value, str) and value):
            raise ConfigError(f"incremental: '{key}' must be a non-empty string")
    _validate_incremental_options(config, incremental)
    if incremental.get("enabled"):
        if not incremental.get("field"):
            raise ConfigError(
                "incremental: 'field' required when 'enabled' is true"
            )
        if inject == "body_template":
            if not config.get("body"):
                raise ConfigError(
                    "incremental: \"inject\": \"body_template\" requires a "
                    "'body' containing the placeholder to substitute"
                )
            if value_format == "any":
                known = ", ".join(f for f in VALUE_FORMATS if f != "any")
                raise ConfigError(
                    "incremental: an explicit 'value_format' is required "
                    "with \"inject\": \"body_template\" — expected one of: "
                    f"{known}. Character filtering only protects a quoted "
                    "placeholder; a bare placeholder "
                    "(WHERE id > {last_id}) would accept '0 OR 1=1'"
                )
        elif not incremental.get("param_name"):
            raise ConfigError(
                "incremental: 'param_name' required when 'enabled' is true "
                "(or \"inject\": \"body_template\" to inject into the body)"
            )


def _validate_replace_where(write: dict, mode: str) -> None:
    predicate = write.get("replace_where")
    if mode == "replace_where" and not predicate:
        raise ConfigError(
            "write: 'replace_where' required with \"mode\": \"replace_where\" — "
            "without a predicate the replacement would cover the whole table, "
            'which is requested explicitly with "mode": "overwrite"'
        )
    if predicate is not None:
        if not isinstance(predicate, str) or not predicate.strip():
            raise ConfigError(
                "write: 'replace_where' must be a non-empty SQL predicate"
            )
        if mode != "replace_where":
            raise ConfigError(
                f"write: 'replace_where' is ignored with \"mode\": \"{mode}\" — "
                "set \"mode\": \"replace_where\" for it to be applied"
            )
        if "{" in predicate or "}" in predicate:
            raise ConfigError(
                "write: 'replace_where' is not templated — placeholders "
                "would stay literal and the predicate would match no row. "
                "Build the string on the caller side, one window per run"
            )


def _validate_merge(write: dict, mode: str) -> None:
    merge_keys = write.get("merge_keys")
    if mode == "merge":
        if (
            not isinstance(merge_keys, (list, tuple))
            or not merge_keys
            or not all(isinstance(key, str) and key for key in merge_keys)
        ):
            raise ConfigError(
                "write: 'merge_keys' required with \"mode\": \"merge\" — a "
                "non-empty list of column names identifying a row. Without "
                "them nothing says which target row an incoming one replaces"
            )
        if any("`" in key for key in merge_keys):
            raise ConfigError(
                "write: 'merge_keys' must not contain a backtick — column "
                "names are quoted with it in the merge predicate"
            )
        if len(set(merge_keys)) != len(merge_keys):
            raise ConfigError("write: 'merge_keys' lists a column twice")
        when_matched = write.get("when_matched", "update")
        if when_matched not in _WRITE_WHEN_MATCHED:
            known = ", ".join(f"'{m}'" for m in _WRITE_WHEN_MATCHED)
            raise ConfigError(
                f"write: unknown 'when_matched' '{when_matched}' — "
                f"expected one of: {known}"
            )
    else:
        for key in ("merge_keys", "when_matched"):
            if key in write:
                raise ConfigError(
                    f"write: '{key}' is ignored with \"mode\": \"{mode}\" — "
                    "set \"mode\": \"merge\" for it to be applied"
                )


def _validate_column_types(write: dict) -> None:
    column_types = write.get("column_types")
    if column_types is not None:
        if not isinstance(column_types, dict) or not column_types:
            raise ConfigError(
                "write: 'column_types' must be a non-empty object mapping a "
                'column name to a type, e.g. {"amount": "decimal(18,4)"}'
            )
        for column, spec in column_types.items():
            if not isinstance(column, str) or not column:
                raise ConfigError("write: 'column_types' keys must be column names")
            try:
                parse_column_type(spec)
            except ValueError as exc:
                raise ConfigError(f"write: column_types['{column}']: {exc}") from None


def _validate_partition_by(write: dict) -> None:
    partition_by = write.get("partition_by")
    if partition_by is not None:
        if not isinstance(partition_by, (list, tuple)) or not partition_by:
            raise ConfigError(
                "write: 'partition_by' must be a non-empty list of columns"
            )
        if not all(isinstance(col, str) and col for col in partition_by):
            raise ConfigError(
                "write: 'partition_by' must contain only non-empty column "
                "names"
            )


def _validate_write(config: dict) -> None:
    """Bloc `write` : mode d'écriture, clés de fusion, partition, types."""
    write = config.get("write")
    if write is None:
        return
    if not isinstance(write, dict):
        raise ConfigError("write: must be an object")
    _check_unknown("write", write, _WRITE_KEYS)
    mode = write.get("mode", "append")
    if mode not in _WRITE_MODES:
        known = ", ".join(f"'{m}'" for m in _WRITE_MODES)
        raise ConfigError(
            f"write: unknown 'mode' '{mode}' — expected one of: {known}"
        )
    _validate_replace_where(write, mode)
    _validate_merge(write, mode)
    _validate_column_types(write)
    _validate_partition_by(write)


def _validate_checkpoint_mode(config: dict) -> None:
    """`checkpoint` et un mode qui remplace ne vont pas ensemble."""
    # `merge` est idempotent — rejouer un lot met à jour les mêmes lignes — donc
    # la reprise de `checkpoint` ne l'abîme pas, contrairement à un
    # remplacement qui effacerait ce que le run interrompu avait déjà écrit.
    if (config.get("incremental") or {}).get("checkpoint") and (
        (config.get("write") or {}).get("mode", "append") not in ("append", "merge")
    ):
        raise ConfigError(
            "write: \"mode\": \"{mode}\" is incompatible with "
            '"incremental.checkpoint". The replacement happens on the first '
            "batch; a restart after an interruption would resume from the "
            "watermark and replace the window again, wiping out what the "
            "interrupted run had already written there. Replaying a backfill "
            "starts from the beginning of its window, not from its middle.".format(
                mode=config["write"]["mode"]
            )
        )


def _validate_retry(config: dict) -> None:
    """Bloc `retry` : valeurs numériques."""
    retry = config.get("retry")
    if retry is None:
        return
    if not isinstance(retry, dict):
        raise ConfigError("retry: must be an object")
    _check_unknown("retry", retry, _RETRY_KEYS)
    _check_number("retry", retry, "max_attempts")
    _check_number("retry", retry, "backoff_multiplier", integer=False)
    # 0 est valide : il désactive l'attente dictée par le serveur
    _check_number(
        "retry", retry, "max_retry_after_seconds",
        integer=False, allow_zero=True,
    )


def validate_config(config: dict) -> None:
    """Valide la configuration d'une source. Lève ConfigError au premier
    problème rencontré, avec un message actionnable."""
    flags = _validate_top_level(config)
    _validate_headers(config, flags.allow_literals)
    _validate_errors(config)
    _validate_template_paths(config)
    _validate_auth(config, flags.allow_http, flags.allow_literals)
    _validate_pagination(config, flags.method, flags.allow_body_on_get)
    _validate_incremental(config)
    _validate_write(config)
    _validate_checkpoint_mode(config)
    _validate_retry(config)
