"""Chemins que la suite ne traversait pas : accès Key Vault, échecs réseau et
réponses d'erreur des appels de token, comparaisons de watermark entre types,
et refus de `run_source_windows`.

Un chemin d'erreur non testé est celui qui casse le jour où on en a besoin. Le
plus important ici est Key Vault : c'est par lui que passent les secrets de
production, et rien ne l'exerçait."""

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from flume_lib import auth
from flume_lib.auth import AuthError, _expires_in, _renewal_instant
from flume_lib.incremental import compare, parse_date
from flume_lib.secrets_ import (
    SecretResolutionError,
    _get_keyvault_secret,
    forget_secrets,
    resolve_secret,
)
from flume_lib.windows import _advanced, _starting_day, run_source_windows


@pytest.fixture(autouse=True)
def _clean_registry():
    forget_secrets()
    yield
    forget_secrets()


# ---------------------------------------------------------------------------
# Key Vault
# ---------------------------------------------------------------------------


def _fake_azure(monkeypatch, value="vault-secret-value"):
    """Installe azure-identity et azure-keyvault-secrets factices. Les appels
    sont enregistrés pour vérifier ce qui est demandé au coffre."""
    seen = {}

    class SecretClient:
        def __init__(self, vault_url, credential):
            seen["vault_url"], seen["credential"] = vault_url, credential

        def get_secret(self, name):
            seen["name"] = name
            return SimpleNamespace(value=value)

    identity = types.ModuleType("azure.identity")
    identity.DefaultAzureCredential = lambda: "default-credential"
    secrets = types.ModuleType("azure.keyvault.secrets")
    secrets.SecretClient = SecretClient
    for name, module in {
        "azure": types.ModuleType("azure"),
        "azure.identity": identity,
        "azure.keyvault": types.ModuleType("azure.keyvault"),
        "azure.keyvault.secrets": secrets,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return seen


class TestKeyVault:
    def test_fabric_resolves_through_notebookutils(self, monkeypatch):
        calls = []
        notebookutils = SimpleNamespace(
            credentials=SimpleNamespace(
                getSecret=lambda url, name: calls.append((url, name)) or "from-fabric"
            )
        )
        monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)

        assert _get_keyvault_secret("https://kv.test", "api-token") == "from-fabric"
        assert calls == [("https://kv.test", "api-token")]

    def test_outside_fabric_it_uses_the_azure_sdk(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "notebookutils", None)  # import impossible
        seen = _fake_azure(monkeypatch)

        assert _get_keyvault_secret("https://kv.test", "api-token") == "vault-secret-value"
        assert seen == {
            "vault_url": "https://kv.test",
            "credential": "default-credential",
            "name": "api-token",
        }

    def test_without_the_sdk_the_error_says_what_to_install(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "notebookutils", None)
        monkeypatch.setitem(sys.modules, "azure.identity", None)
        monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", None)

        with pytest.raises(SecretResolutionError, match=r"flume-lib\[azure\]"):
            _get_keyvault_secret("https://kv.test", "api-token")

    def test_a_reference_is_resolved_stripped_and_remembered(self, monkeypatch):
        from flume_lib.secrets_ import redact

        monkeypatch.setitem(sys.modules, "notebookutils", None)
        _fake_azure(monkeypatch, value="  padded-secret-value \n")

        value = resolve_secret(
            {"keyvault_url": "https://kv.test", "secret_name": "api-token"}, "token"
        )

        assert value == "padded-secret-value"
        assert redact("echo padded-secret-value") == "echo <redacted>"

    def test_a_reference_without_secret_name_is_refused(self):
        with pytest.raises(SecretResolutionError, match="secret_name"):
            resolve_secret({"keyvault_url": "https://kv.test"}, "token")

    @pytest.mark.parametrize("ref", [{"unknown": "x"}, {}, 123, None, ["env_var"]])
    def test_anything_else_is_an_invalid_reference(self, ref):
        with pytest.raises(SecretResolutionError, match="invalid reference"):
            resolve_secret(ref, "token")

    def test_a_missing_environment_variable_names_the_variable_not_a_value(self, monkeypatch):
        monkeypatch.delenv("FLUME_TEST_ABSENT", raising=False)
        with pytest.raises(SecretResolutionError, match="FLUME_TEST_ABSENT"):
            resolve_secret({"env_var": "FLUME_TEST_ABSENT"}, "token")


# ---------------------------------------------------------------------------
# Appels de token : échecs réseau et réponses d'erreur
# ---------------------------------------------------------------------------

OAUTH2 = {
    "token_url": "https://idp.test/token?tenant=acme",
    "client_id": "public-id",
    "client_secret": "client-secret-value",
}
TOKEN_ENDPOINT = {"token_url": "https://idp.test/login?tenant=acme", "body": {"user": "u"}}


def _response(status=200, payload=None, text=""):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = payload if payload is not None else {}
    response.text = text
    return response


class TestTokenCallFailures:
    def test_oauth2_network_error_keeps_its_type_and_loses_its_query(self, monkeypatch):
        def boom(*a, **k):
            raise requests.ConnectionError(
                "Max retries exceeded with url: /token?tenant=acme (Caused by ...)"
            )

        monkeypatch.setattr(auth.requests, "post", boom)
        with pytest.raises(requests.ConnectionError) as exc:
            auth._fetch_oauth2_client_credentials(OAUTH2)
        assert "tenant=acme" not in str(exc.value)

    def test_token_endpoint_network_error_keeps_its_type(self, monkeypatch):
        def boom(*a, **k):
            raise requests.Timeout("read timed out: /login?tenant=acme")

        monkeypatch.setattr(auth.requests, "request", boom)
        with pytest.raises(requests.Timeout) as exc:
            auth._fetch_token_endpoint(TOKEN_ENDPOINT)
        assert "tenant=acme" not in str(exc.value)

    def test_oauth2_refusal_shows_the_status_and_the_start_of_the_body(self, monkeypatch):
        monkeypatch.setattr(
            auth.requests, "post",
            lambda *a, **k: _response(401, text='{"error":\n "invalid_client"}'),
        )
        expected = r'HTTP 401 on https://idp.test/token.*"invalid_client"'
        with pytest.raises(AuthError, match=expected) as exc:
            auth._fetch_oauth2_client_credentials(OAUTH2)
        assert "tenant=acme" not in str(exc.value)

    def test_oauth2_response_without_access_token(self, monkeypatch):
        monkeypatch.setattr(auth.requests, "post", lambda *a, **k: _response(200, {"other": 1}))
        with pytest.raises(AuthError, match="'access_token' missing"):
            auth._fetch_oauth2_client_credentials(OAUTH2)

    def test_token_endpoint_refusal(self, monkeypatch):
        monkeypatch.setattr(
            auth.requests, "request", lambda *a, **k: _response(403, text="forbidden")
        )
        with pytest.raises(AuthError, match="token_endpoint: HTTP 403.*forbidden"):
            auth._fetch_token_endpoint(TOKEN_ENDPOINT)

    def test_token_endpoint_empty_token(self, monkeypatch):
        monkeypatch.setattr(
            auth.requests, "request", lambda *a, **k: _response(200, {"access_token": ""})
        )
        with pytest.raises(AuthError, match="empty token"):
            auth._fetch_token_endpoint(TOKEN_ENDPOINT)

    def test_token_endpoint_token_path_missing(self, monkeypatch):
        monkeypatch.setattr(
            auth.requests, "request", lambda *a, **k: _response(200, {"data": {}})
        )
        with pytest.raises(AuthError, match="not found in the token endpoint response"):
            auth._fetch_token_endpoint({**TOKEN_ENDPOINT, "token_json_path": "data.token"})

    def test_token_endpoint_get_without_body_sends_no_params(self, monkeypatch):
        captured = {}

        def request(method, url, **kwargs):
            captured.update(method=method, kwargs=kwargs)
            return _response(200, {"access_token": "tok-from-get-call"})

        monkeypatch.setattr(auth.requests, "request", request)
        token, _ = auth._fetch_token_endpoint(
            {"token_url": "https://idp.test/login", "method": "GET"}
        )
        assert token == "tok-from-get-call"
        assert captured["method"] == "GET"
        assert "params" not in captured["kwargs"]

    def test_a_malformed_oauth1_signer_is_an_auth_error(self, monkeypatch):
        monkeypatch.setenv("CK", "consumer-key-value")
        monkeypatch.setenv("CS", "consumer-secret-value")
        with pytest.raises(AuthError, match="control characters"):
            auth._build_oauth1(
                {
                    "consumer_key": {"env_var": "CK"},
                    "consumer_secret": {"env_var": "CS"},
                    "realm": "acct\nX-Injected: 1",
                }
            )


class TestExpiresIn:
    def test_a_numeric_text_is_read(self):
        assert _expires_in({"expires_in": "3600"}, "expires_in") == 3600.0

    @pytest.mark.parametrize("payload", [{"expires_in": "soon"}, {"expires_in": {"a": 1}}, {}])
    def test_an_unreadable_lifetime_is_none(self, payload):
        assert _expires_in(payload, "expires_in") is None

    def test_a_nested_path(self):
        assert _expires_in({"data": {"ttl": 120}}, "data.ttl") == 120.0

    def test_no_path_means_no_lifetime(self):
        assert _expires_in({"expires_in": 60}, None) is None

    def test_the_renewal_instant_ignores_an_unknown_lifetime(self):
        assert _renewal_instant(None) is None


# ---------------------------------------------------------------------------
# Comparaison de watermarks de types différents
# ---------------------------------------------------------------------------


class TestCompareAcrossTypes:
    @pytest.mark.parametrize(
        "left, right, expected",
        [(5, "10", -1), ("20", 5, 1), (5, "5", 0), (2.5, "2.5", 0)],
    )
    def test_a_number_and_a_numeric_text_compare_as_numbers(self, left, right, expected):
        assert compare(left, right) == expected

    @pytest.mark.parametrize("left, right", [(5, "abc"), ("abc", 5), (5, ""), (None, 5)])
    def test_a_number_and_a_non_numeric_text_do_not_compare(self, left, right):
        assert compare(left, right) is None

    @pytest.mark.parametrize("value", ["2026-13-45", "2026-02-30", "not a date", 20260102, None])
    def test_parse_date_refuses_what_is_not_a_calendar_day(self, value):
        assert parse_date(value) is None


# ---------------------------------------------------------------------------
# run_source_windows : refus
# ---------------------------------------------------------------------------


class TestWindowsGuards:
    def test_a_watermark_that_is_not_a_date_is_refused(self):
        with pytest.raises(ValueError, match="does not start with a yyyy-MM-dd date"):
            _starting_day("last-tuesday", "watermark of 's'")

    def test_a_timestamp_gives_its_day(self):
        assert _starting_day("2026-09-28T10:00:00Z", "w").isoformat() == "2026-09-28"

    @pytest.mark.parametrize(
        "before, after, expected",
        [
            ("2026-01-01", None, False),  # rien n'a été écrit
            (None, "2026-01-05", True),  # premier run qui avance
            ("2026-01-01", "2026-01-05", True),
            ("2026-01-05", "2026-01-05", False),
            ("2026-01-05", "2026-01-01", False),
        ],
    )
    def test_advanced(self, before, after, expected):
        result = SimpleNamespace(watermark_before=before, watermark_after=after)
        assert _advanced(result) is expected

    def test_the_config_must_carry_a_name(self):
        with pytest.raises(ValueError, match="needs a 'name'"):
            run_source_windows(lambda end: {"incremental": {}}, "2026-02-01", 10)

    @pytest.mark.parametrize("bad", [0, -3, True, 1.5, "7"])
    def test_max_span_days_must_be_a_positive_integer(self, bad):
        with pytest.raises(ValueError, match="max_span_days"):
            run_source_windows(lambda end: {"name": "s"}, "2026-02-01", bad)
