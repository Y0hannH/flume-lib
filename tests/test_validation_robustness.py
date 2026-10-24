"""`validate_config` ne lève que `ConfigError`.

Un appelant qui valide en amont d'un long batch attrape `ConfigError` et rien
d'autre. Or quelques valeurs mal formées — une liste là où une chaîne est
attendue, un `null` pour un bloc — faisaient lever `TypeError` ou
`AttributeError` du fond de la validation. Trouvés en faisant muter au hasard
des dizaines de milliers de configurations ; chaque site de plantage a son test,
et un test de mutation garde le contrat pour la suite.
"""

import copy
import random

import pytest

from flume_lib.validation import ConfigError, validate_config

BASE = {
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}


def cfg(**overrides):
    return {**BASE, **overrides}


class TestEachCrashSite:
    """Les sept sites relevés par le fuzzer."""

    @pytest.mark.parametrize("bad", [["a"], {"k": "v"}, 3, True])
    def test_auth_type_must_be_a_string(self, bad):
        with pytest.raises(ConfigError, match="unknown type"):
            validate_config(cfg(auth={"type": bad}))

    @pytest.mark.parametrize("bad", [["a"], {"k": "v"}, 3, True])
    def test_pagination_type_must_be_a_string(self, bad):
        with pytest.raises(ConfigError, match="unknown type"):
            validate_config(cfg(pagination={"type": bad}))

    @pytest.mark.parametrize("bad", [3, 1.5, True, ["a"], "x", None])
    def test_params_must_be_an_object(self, bad):
        with pytest.raises(ConfigError, match="params: must be an object"):
            validate_config(cfg(params=bad))

    @pytest.mark.parametrize("section", ["body", "headers"])
    @pytest.mark.parametrize("bad", ["x", ["a"], 3, True])
    def test_token_endpoint_body_and_headers_must_be_objects(self, section, bad):
        auth = {"type": "token_endpoint", "token_url": "https://idp.test/login", section: bad}
        with pytest.raises(ConfigError, match=f"'{section}' must be an object"):
            validate_config(cfg(auth=auth))

    @pytest.mark.parametrize(
        "key", ["token_json_path", "expires_in_json_path", "header_name", "value_prefix"]
    )
    @pytest.mark.parametrize("bad", [3, ["a"], "", True])
    def test_token_endpoint_names_must_be_strings(self, key, bad):
        auth = {"type": "token_endpoint", "token_url": "https://idp.test/login", key: bad}
        with pytest.raises(ConfigError, match=f"'{key}' must be a non-empty string"):
            validate_config(cfg(auth=auth))

    def test_token_endpoint_get_with_a_null_body_is_refused(self):
        """`null` n'est pas « absent » ici : l'appel de token itère sur `body`."""
        auth = {"type": "token_endpoint", "token_url": "https://idp.test/l",
                "method": "GET", "body": None}
        with pytest.raises(ConfigError, match="'body' must be an object"):
            validate_config(cfg(auth=auth))

    @pytest.mark.parametrize("bad", [["a"], {"k": "v"}, 3, True, ""])
    @pytest.mark.parametrize("key", ["key_field", "key_param"])
    def test_keyset_names_must_be_strings(self, key, bad):
        pagination = {"type": "keyset", "key_field": "id", "key_param": "after", key: bad}
        with pytest.raises(ConfigError, match=f"'{key}' must be a non-empty string"):
            validate_config(cfg(pagination=pagination))

    @pytest.mark.parametrize("key", ["items_field", "record_field", "limit_param"])
    @pytest.mark.parametrize("bad", [3, ["a"], "", True])
    def test_other_pagination_names_must_be_strings(self, key, bad):
        with pytest.raises(ConfigError, match=f"'{key}' must be a non-empty string"):
            validate_config(cfg(pagination={"type": "offset", key: bad}))

    def test_a_null_name_is_still_accepted(self):
        """`null` veut dire « non renseigné » : le défaut s'applique."""
        validate_config(cfg(pagination={"type": "offset", "items_field": None}))

    @pytest.mark.parametrize("key", ["field", "param_name", "placeholder"])
    @pytest.mark.parametrize("bad", [3, ["a"], "", True])
    def test_incremental_names_must_be_strings(self, key, bad):
        incremental = {"enabled": True, "field": "ts", "param_name": "since", key: bad}
        with pytest.raises(ConfigError, match=f"'{key}' must be a non-empty string"):
            validate_config(cfg(incremental=incremental))

    @pytest.mark.parametrize("bad", [3, "x", ["a"]])
    def test_window_with_params_of_the_wrong_type(self, bad):
        incremental = {
            "enabled": True, "field": "ts", "param_name": "since",
            "window": {"upper_param": "until", "upper_value": "2026-02-01"},
            "advance": "upper_bound",
        }
        with pytest.raises(ConfigError, match="params: must be an object"):
            validate_config(cfg(params=bad, incremental=incremental))


class TestNullBlocksMeanAbsent:
    """`incremental: null` passait la validation puis plantait dans le contrôle
    de `checkpoint`, et à l'exécution. Un bloc nul vaut un bloc absent."""

    @pytest.mark.parametrize("block", ["incremental", "write", "auth", "pagination", "retry",
                                       "errors", "headers"])
    def test_a_null_block_is_accepted(self, block):
        validate_config(cfg(**{block: None}))

    def test_a_null_incremental_runs_like_an_absent_one(self, monkeypatch):
        from flume_lib.source import run_source

        class Session:
            def __init__(self):
                self.headers = {}

            def close(self):
                pass

            def request(self, method, url, **kwargs):
                class Response:
                    status_code = 200
                    headers = {}
                    text = "[]"

                    def json(self):
                        return [{"id": 1}]

                return Response()

        monkeypatch.setattr("flume_lib.source.requests.Session", Session)
        monkeypatch.setattr("flume_lib.source.write_records", lambda *a, **k: ({}, []))
        monkeypatch.setattr("flume_lib.source.write_log_run", lambda *a, **k: None)

        result = run_source(cfg(incremental=None), lakehouse_tables_path="/tmp/Tables")

        assert result.status == "success", result.error_message


# ---------------------------------------------------------------------------
# Le contrat, tenu par mutation aléatoire
# ---------------------------------------------------------------------------

RICH = {
    **BASE,
    "name": "s",
    "method": "POST",
    "params": {"a": "1", "until": "2026-02-01"},
    "headers": {"Accept": "application/json"},
    "body": {"query": "select * from t where id > {last_id}", "variables": {"x": 1}},
    "template_paths": ["query"],
    "timeout_seconds": 30,
    "batch_size": 1000,
    "errors": {"path": "errors", "retryable_codes": ["THROTTLED"]},
    "auth": {
        "type": "oauth2_client_credentials", "token_url": "https://idp.test/token",
        "client_id": "id", "client_secret": {"env_var": "S"}, "client_auth": "basic",
    },
    "pagination": {
        "type": "keyset", "key_field": "id", "key_param": "last_id", "limit": 100,
        "params_in": "body_template", "value_format": "numeric", "max_pages": 10,
    },
    "incremental": {
        "enabled": True, "field": "ts", "param_name": "since",
        "window": {"upper_param": "until", "upper_value": "2026-02-01"},
        "advance": "upper_bound", "lookback": 2, "value_format": "iso_date",
    },
    "write": {"mode": "merge", "merge_keys": ["id"], "column_types": {"a": "decimal(18,4)"}},
    "retry": {"max_attempts": 3, "backoff_multiplier": 1},
}

SECOND = {
    **BASE,
    "auth": {"type": "token_endpoint", "token_url": "https://idp.test/login",
             "method": "GET", "body": {"u": "x"}, "headers": {"A": "b"}},
    "pagination": {"type": "next_link", "next_field": "n", "allowed_hosts": ["cdn.test"]},
}

VALUES = [
    None, "", "x", 0, -1, 1, 1.5, True, False, [], [""], ["a"], [1], {}, {"k": "v"},
    {"k": 1}, [[]], [{}], "http://x.test", "GET", "POST", "merge", "keyset", "cursor",
    "page", "offset", "next_link", "none", "basic", "body", "oauth1", "token_endpoint",
    "{x}", "decimal(5,2)", "numeric", "iso_date", "any", "upper_bound", "body_template",
]


def _paths(node, path=()):
    yield path
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _paths(value, path + (key,))


def _mutate(base, rng):
    config = copy.deepcopy(base)
    paths = [p for p in _paths(config) if p]
    for _ in range(rng.choice([1, 1, 2, 3])):
        path = rng.choice(paths)
        try:
            node = config
            for key in path[:-1]:
                node = node[key]
            if rng.random() < 0.25:
                node.pop(path[-1], None)
            else:
                node[path[-1]] = copy.deepcopy(rng.choice(VALUES))
        except (KeyError, TypeError, AttributeError):
            pass  # le chemin a disparu sous une mutation précédente
    return config


class TestOnlyConfigErrorEscapes:
    @pytest.mark.parametrize("seed", range(4))
    def test_randomly_mutated_configs(self, seed):
        rng = random.Random(seed)
        for _ in range(1500):
            config = _mutate(rng.choice([RICH, SECOND]), rng)
            try:
                validate_config(copy.deepcopy(config))
            except ConfigError:
                continue
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{type(exc).__name__}: {exc} — for {config!r}")

    def test_the_rich_base_is_itself_valid(self):
        """Sans quoi les mutations ne testeraient que le premier refus."""
        validate_config(copy.deepcopy(RICH))
        validate_config(copy.deepcopy(SECOND))
