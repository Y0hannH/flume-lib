"""Validation stricte de la configuration d'une source.

Toute clé inconnue est une erreur : une faute de frappe sur une clé optionnelle
(`pagintaion` au lieu de `pagination`) passerait sinon en silence — la lib
ferait un appel unique, le run serait marqué `success` et la majorité des
données seraient perdues sans aucun signal."""

import difflib

from flume_lib.auth import CLIENT_AUTH_MODES
from flume_lib.oauth1 import SIGNATURE_METHODS
from flume_lib.templating import NORMALIZERS, VALUE_FORMATS, templated_placeholders

# Clés top-level
_REQUIRED = ("base_url", "target_schema", "target_table")
_OPTIONAL = (
    "name", "params", "auth", "pagination", "incremental", "retry",
    "timeout_seconds", "method", "body", "body_format", "headers",
    "errors", "template_paths", "batch_size", "write", "allow_body_on_get",
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
        "timeout_seconds",
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
    "next_link": ("next_field",),
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
)
_INCREMENTAL_INJECTS = ("query_param", "body_template")
_RETRY_KEYS = ("max_attempts", "backoff_multiplier", "max_retry_after_seconds")

_WRITE_KEYS = ("mode", "replace_where", "partition_by")
_WRITE_MODES = ("append", "overwrite", "replace_where")


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


def _check_type(section: str, config: dict, allowed_types: dict, default: str):
    if not isinstance(config, dict):
        raise ConfigError(f"{section}: must be an object")
    type_name = config.get("type", default)
    if type_name not in allowed_types:
        known = ", ".join(sorted(allowed_types))
        raise ConfigError(
            f"{section}: unknown type '{type_name}' — expected one of: {known}"
        )
    return type_name


def validate_config(config: dict) -> None:
    """Valide la configuration d'une source. Lève ConfigError au premier
    problème rencontré, avec un message actionnable."""
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

    if "batch_size" in config:
        batch_size = config["batch_size"]
        # bool est un int en Python : le laisser passer donnerait batch_size=1
        if not isinstance(batch_size, int) or isinstance(batch_size, bool):
            raise ConfigError("config: 'batch_size' must be an integer")
        if batch_size < 1:
            raise ConfigError("config: 'batch_size' must be greater than 0")

    headers = config.get("headers")
    if headers is not None:
        if not isinstance(headers, dict):
            raise ConfigError("headers: must be an object")
        for key, value in headers.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ConfigError(
                    f"headers: '{key}' must be a literal string — for a "
                    "credential, use 'auth' (api_key_header, bearer_token…)"
                )

    errors_config = config.get("errors")
    if errors_config is not None:
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

    template_paths = config.get("template_paths")
    if template_paths is not None:
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
        if auth_type == "oauth2_client_credentials":
            client_auth = auth.get("client_auth", "body")
            if client_auth not in CLIENT_AUTH_MODES:
                known = ", ".join(CLIENT_AUTH_MODES)
                raise ConfigError(
                    f"auth: unknown 'client_auth' '{client_auth}' — "
                    f"expected one of: {known}"
                )
        if auth_type == "token_endpoint":
            token_method = str(auth.get("method", "POST")).upper()
            if token_method == "GET" and any(
                isinstance(v, dict) for v in auth.get("body", {}).values()
            ):
                raise ConfigError(
                    "auth: secret references are not allowed in 'body' on GET — "
                    "the parameters travel in the URL (server logs, proxies)"
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

        for key in ("max_pages", "max_rows"):
            if key not in pagination:
                continue
            value = pagination[key]
            if not isinstance(value, int) or isinstance(value, bool):
                raise ConfigError(f"pagination: '{key}' must be an integer")
            if value < 1:
                raise ConfigError(f"pagination: '{key}' must be greater than 0")

        params_path = pagination.get("params_path")
        if params_path is not None:
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

        if pagination_type == "page":
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

        if pagination_type == "cursor":
            for key in ("cursor_param", "cursor_field"):
                if not pagination.get(key):
                    raise ConfigError(
                        f"pagination: '{key}' required with \"type\": \"cursor\""
                    )

        if pagination_type == "keyset":
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
            if params_in == "body_template":
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
            if params_in == "body_template" and value_format == "any":
                known = ", ".join(f for f in VALUE_FORMATS if f != "any")
                raise ConfigError(
                    "pagination: an explicit 'value_format' is required when the "
                    "key is interpolated into the body — expected one of: "
                    f"{known}. The key comes from the API response; character "
                    "filtering only protects a quoted placeholder, a bare "
                    "placeholder (WHERE id > {last_key}) would accept "
                    "'0 OR 1=1'"
                )

    incremental = config.get("incremental")
    if incremental is not None:
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

    write = config.get("write")
    if write is not None:
        if not isinstance(write, dict):
            raise ConfigError("write: must be an object")
        _check_unknown("write", write, _WRITE_KEYS)
        mode = write.get("mode", "append")
        if mode not in _WRITE_MODES:
            known = ", ".join(f"'{m}'" for m in _WRITE_MODES)
            raise ConfigError(
                f"write: unknown 'mode' '{mode}' — expected one of: {known}"
            )
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

    if config.get("incremental", {}).get("checkpoint") and (
        config.get("write", {}).get("mode", "append") != "append"
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

    retry = config.get("retry")
    if retry is not None:
        if not isinstance(retry, dict):
            raise ConfigError("retry: must be an object")
        _check_unknown("retry", retry, _RETRY_KEYS)
