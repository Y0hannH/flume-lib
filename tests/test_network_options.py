"""`proxy` et `ca_bundle` : validation, application aux requêtes de données et
aux appels de token, masquage d'un proxy authentifié."""

from unittest.mock import MagicMock

import pytest
import requests

from flume_lib import ConfigError, run_source, validate_config
from flume_lib.auth import AuthProvider
from flume_lib.network import NetworkConfigError, parse_proxy_url, resolve_network

BASE = {
    "name": "s",
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
    "pagination": {"type": "none", "items_field": "items"},
}


@pytest.fixture
def ca_file(tmp_path):
    path = tmp_path / "corp-ca.pem"
    path.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    return str(path)


class TestValidation:
    @pytest.mark.parametrize("good", [
        "http://proxy.corp:3128", "https://proxy.corp", "http://10.0.0.5:8080/",
        {"env_var": "PROXY_URL"},
        {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "proxy"},
    ])
    def test_a_proxy_is_an_url_or_a_secret_reference(self, good):
        validate_config({**BASE, "proxy": good})

    @pytest.mark.parametrize("bad", [
        "proxy.corp:3128", "ftp://proxy.corp", "socks5://proxy.corp:1080", "http://", "",
        "  ", 3128, True, ["http://proxy.corp"], {"url": "http://proxy.corp"}, {},
    ])
    def test_anything_else_is_refused(self, bad):
        with pytest.raises(ConfigError, match="proxy"):
            validate_config({**BASE, "proxy": bad})

    def test_credentials_in_a_literal_proxy_url_are_refused_without_being_quoted(self):
        with pytest.raises(ConfigError) as caught:
            validate_config({**BASE, "proxy": "http://user:hunter2xx@proxy.corp:3128"})
        assert "hunter2xx" not in str(caught.value)
        assert "reference the whole URL as a secret" in str(caught.value)

    @pytest.mark.parametrize("bad", ["", "  ", 3, True, ["a.pem"], {"path": "a.pem"}])
    def test_ca_bundle_is_a_path(self, bad):
        with pytest.raises(ConfigError, match="ca_bundle"):
            validate_config({**BASE, "ca_bundle": bad})

    def test_a_ca_bundle_path_is_accepted(self):
        validate_config({**BASE, "ca_bundle": "/lakehouse/default/Files/certs/corp.pem"})

    def test_null_means_absent(self):
        validate_config({**BASE, "proxy": None, "ca_bundle": None})

    def test_the_old_configs_are_unchanged(self):
        validate_config(BASE)


class TestResolveNetwork:
    def test_nothing_configured_gives_nothing(self):
        assert resolve_network(BASE) == {}

    def test_a_literal_proxy_covers_both_schemes(self):
        settings = resolve_network({**BASE, "proxy": "http://proxy.corp:3128"})
        assert settings == {
            "proxies": {"http": "http://proxy.corp:3128", "https": "http://proxy.corp:3128"}
        }

    def test_a_referenced_proxy_may_carry_credentials(self, monkeypatch):
        monkeypatch.setenv("PROXY_URL", "http://svc:s3cretpass@proxy.corp:3128\n")
        settings = resolve_network({**BASE, "proxy": {"env_var": "PROXY_URL"}})
        assert settings["proxies"]["https"] == "http://svc:s3cretpass@proxy.corp:3128"

    def test_an_unset_variable_is_explained(self, monkeypatch):
        monkeypatch.delenv("PROXY_URL", raising=False)
        with pytest.raises(NetworkConfigError, match="PROXY_URL"):
            resolve_network({**BASE, "proxy": {"env_var": "PROXY_URL"}})

    def test_a_bad_resolved_url_is_refused_without_being_quoted(self, monkeypatch):
        monkeypatch.setenv("PROXY_URL", "svc:s3cretpass@proxy.corp")
        with pytest.raises(NetworkConfigError) as caught:
            resolve_network({**BASE, "proxy": {"env_var": "PROXY_URL"}})
        assert "s3cretpass" not in str(caught.value)

    def test_the_ca_bundle_becomes_verify(self, ca_file):
        assert resolve_network({**BASE, "ca_bundle": ca_file}) == {"verify": ca_file}

    def test_a_missing_ca_file_is_explained(self, tmp_path):
        with pytest.raises(NetworkConfigError, match="ca_bundle: no file at"):
            resolve_network({**BASE, "ca_bundle": str(tmp_path / "absent.pem")})

    def test_a_directory_is_not_a_ca_file(self, tmp_path):
        with pytest.raises(NetworkConfigError, match="no file at"):
            resolve_network({**BASE, "ca_bundle": str(tmp_path)})

    def test_parse_accepts_credentials_only_when_told_to(self):
        parse_proxy_url("http://u:p@proxy.corp:3128", allow_credentials=True)
        with pytest.raises(ValueError):
            parse_proxy_url("http://u:p@proxy.corp:3128", allow_credentials=False)


class Recording:
    kwargs: list = []
    behaviour = None

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        Recording.kwargs.append(kwargs)
        if Recording.behaviour is not None:
            Recording.behaviour()
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        response.json.return_value = {"items": [{"id": 1}]}
        return response


@pytest.fixture
def http(monkeypatch):
    Recording.kwargs, Recording.behaviour = [], None
    monkeypatch.setattr("flume_lib.source.requests.Session", Recording)


def run(config, tmp_path):
    return run_source(config, lakehouse_tables_path=str(tmp_path), dry_run=True)


@pytest.mark.usefixtures("http")
class TestInARun:
    def test_data_requests_carry_the_proxy_and_the_ca(self, tmp_path, ca_file):
        config = {**BASE, "proxy": "http://proxy.corp:3128", "ca_bundle": ca_file}
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert Recording.kwargs[0]["proxies"] == {
            "http": "http://proxy.corp:3128", "https": "http://proxy.corp:3128",
        }
        assert Recording.kwargs[0]["verify"] == ca_file

    def test_without_the_keys_requests_keep_their_defaults(self, tmp_path):
        run(BASE, tmp_path)
        assert "proxies" not in Recording.kwargs[0] and "verify" not in Recording.kwargs[0]

    def test_a_missing_ca_file_fails_the_run_before_any_request(self, tmp_path):
        result = run({**BASE, "ca_bundle": str(tmp_path / "absent.pem")}, tmp_path)
        assert result.status == "failed"
        assert result.error_message.startswith("NetworkConfigError:")
        assert Recording.kwargs == []

    def test_a_proxy_credential_is_masked_in_the_error_message(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROXY_URL", "http://svc:s3cretpass@proxy.corp:3128")

        def refuse():
            raise requests.exceptions.ProxyError(
                "Cannot connect to proxy http://svc:s3cretpass@proxy.corp:3128"
            )

        Recording.behaviour = refuse
        result = run({**BASE, "proxy": {"env_var": "PROXY_URL"},
                      "retry": {"max_attempts": 1}}, tmp_path)

        assert result.status == "failed"
        assert "s3cretpass" not in result.error_message
        assert "<redacted>" in result.error_message


class TestTheEnvironmentDoesNotOverrideTheConfig:
    def test_the_configured_proxy_beats_the_environment(self, monkeypatch):
        """`requests` fait céder le proxy de la session devant HTTPS_PROXY ;
        celui d'une requête ne cède pas. C'est pourquoi la lib passe `proxies`
        à chaque requête plutôt que de le poser sur la session."""
        monkeypatch.setenv("HTTPS_PROXY", "http://from-env:1")
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)

        settings = requests.Session().merge_environment_settings(
            "https://api.test/items", {"https": "http://configured:2"}, None, None, None
        )
        assert settings["proxies"]["https"] == "http://configured:2"


class TestTokenCalls:
    """L'IdP est derrière le même proxy que l'API."""

    NETWORK = {"proxies": {"http": "http://p:1", "https": "http://p:1"}, "verify": "/ca.pem"}

    def test_the_oauth2_call_carries_the_network_settings(self, monkeypatch):
        monkeypatch.setenv("CID", "client-id-value")
        monkeypatch.setenv("CS", "client-secret-value")
        post = MagicMock()
        post.return_value.status_code = 200
        post.return_value.json.return_value = {"access_token": "tok-123456", "expires_in": 3600}
        monkeypatch.setattr("flume_lib.auth.requests.post", post)

        provider = AuthProvider(
            {
                "type": "oauth2_client_credentials",
                "token_url": "https://idp.test/token",
                "client_id": {"env_var": "CID"},
                "client_secret": {"env_var": "CS"},
            },
            self.NETWORK,
        )
        provider.headers()

        assert post.call_args.kwargs["proxies"] == self.NETWORK["proxies"]
        assert post.call_args.kwargs["verify"] == "/ca.pem"

    def test_the_token_endpoint_call_carries_them_too(self, monkeypatch):
        monkeypatch.setenv("PW", "password-value")
        request = MagicMock()
        request.return_value.status_code = 200
        request.return_value.json.return_value = {"token": "tok-123456"}
        monkeypatch.setattr("flume_lib.auth.requests.request", request)

        provider = AuthProvider(
            {
                "type": "token_endpoint",
                "token_url": "https://login.test/auth",
                "body": {"password": {"env_var": "PW"}},
                "token_json_path": "token",
            },
            self.NETWORK,
        )
        provider.headers()

        assert request.call_args.kwargs["proxies"] == self.NETWORK["proxies"]
        assert request.call_args.kwargs["verify"] == "/ca.pem"

    def test_without_network_settings_the_calls_are_as_before(self, monkeypatch):
        monkeypatch.setenv("CID", "client-id-value")
        monkeypatch.setenv("CS", "client-secret-value")
        post = MagicMock()
        post.return_value.status_code = 200
        post.return_value.json.return_value = {"access_token": "tok-123456"}
        monkeypatch.setattr("flume_lib.auth.requests.post", post)

        AuthProvider(
            {
                "type": "oauth2_client_credentials",
                "token_url": "https://idp.test/token",
                "client_id": {"env_var": "CID"},
                "client_secret": {"env_var": "CS"},
            }
        ).headers()

        assert "proxies" not in post.call_args.kwargs and "verify" not in post.call_args.kwargs
