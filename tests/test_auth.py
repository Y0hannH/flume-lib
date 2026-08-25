import base64
from unittest.mock import MagicMock

import pytest

import flume_lib.secrets_ as secrets_
from flume_lib.auth import (
    TOKEN_EXPIRY_MARGIN_SECONDS,
    AuthError,
    AuthProvider,
    build_auth_headers,
)
from flume_lib.secrets_ import SecretResolutionError, resolve_secret


class TestBearerToken:
    def test_builds_header(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "secret123")
        headers = build_auth_headers({"type": "bearer_token", "token_env_var": "MY_TOKEN"})
        assert headers == {"Authorization": "Bearer secret123"}

    def test_new_form_env_var_ref(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "secret123")
        headers = build_auth_headers(
            {"type": "bearer_token", "token": {"env_var": "MY_TOKEN"}}
        )
        assert headers == {"Authorization": "Bearer secret123"}

    def test_keyvault_ref(self, monkeypatch):
        monkeypatch.setattr(
            secrets_, "_get_keyvault_secret", lambda url, name: f"kv:{url}:{name}"
        )
        headers = build_auth_headers(
            {
                "type": "bearer_token",
                "token": {
                    "keyvault_url": "https://kv.vault.azure.net",
                    "secret_name": "src-token",
                },
            }
        )
        assert headers == {
            "Authorization": "Bearer kv:https://kv.vault.azure.net:src-token"
        }

    def test_custom_header_name_and_prefix(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "secret123")
        headers = build_auth_headers(
            {
                "type": "bearer_token",
                "token": {"env_var": "MY_TOKEN"},
                "header_name": "X-Access-Token",
                "value_prefix": "",
            }
        )
        assert headers == {"X-Access-Token": "secret123"}

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_TOKEN", raising=False)
        with pytest.raises(AuthError, match="MISSING_TOKEN"):
            build_auth_headers({"type": "bearer_token", "token_env_var": "MISSING_TOKEN"})

    def test_missing_config_key_raises(self):
        with pytest.raises(AuthError, match="token"):
            build_auth_headers({"type": "bearer_token"})


class TestApiKeyHeader:
    def test_default_header_name(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "k1")
        headers = build_auth_headers({"type": "api_key_header", "key_env_var": "MY_KEY"})
        assert headers == {"X-API-Key": "k1"}

    def test_custom_header_name(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "k1")
        headers = build_auth_headers(
            {
                "type": "api_key_header",
                "key_env_var": "MY_KEY",
                "header_name": "Ocp-Apim-Subscription-Key",
            }
        )
        assert headers == {"Ocp-Apim-Subscription-Key": "k1"}


class TestBasic:
    def test_builds_basic_header(self, monkeypatch):
        monkeypatch.setenv("USER_VAR", "alice")
        monkeypatch.setenv("PASS_VAR", "s3cret")
        headers = build_auth_headers(
            {"type": "basic", "username_env_var": "USER_VAR", "password_env_var": "PASS_VAR"}
        )
        expected = base64.b64encode(b"alice:s3cret").decode()
        assert headers == {"Authorization": f"Basic {expected}"}


class TestResolveSecret:
    def test_literal_string(self):
        assert resolve_secret("client_credentials") == "client_credentials"

    def test_missing_secret_name_raises(self):
        with pytest.raises(SecretResolutionError, match="secret_name"):
            resolve_secret({"keyvault_url": "https://kv.vault.azure.net"})

    def test_invalid_ref_raises(self):
        with pytest.raises(SecretResolutionError, match="invalid reference"):
            resolve_secret(42)


def _mock_token_response(monkeypatch, payload, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    mock = MagicMock(return_value=response)
    monkeypatch.setattr("flume_lib.auth.requests.post", mock)
    monkeypatch.setattr("flume_lib.auth.requests.request", mock)
    return mock


class TestOauth2ClientCredentials:
    def test_entra_service_principal(self, monkeypatch):
        monkeypatch.setenv("SP_CLIENT_ID", "app-id")
        monkeypatch.setenv("SP_CLIENT_SECRET", "app-secret")
        mock = _mock_token_response(monkeypatch, {"access_token": "jwt-abc"})

        headers = build_auth_headers(
            {
                "type": "oauth2_client_credentials",
                "tenant_id": "mon-tenant",
                "client_id": {"env_var": "SP_CLIENT_ID"},
                "client_secret": {"env_var": "SP_CLIENT_SECRET"},
                "scope": "https://graph.microsoft.com/.default",
            }
        )

        assert headers == {"Authorization": "Bearer jwt-abc"}
        url = mock.call_args.args[0]
        assert url == "https://login.microsoftonline.com/mon-tenant/oauth2/v2.0/token"
        data = mock.call_args.kwargs["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "app-id"
        assert data["client_secret"] == "app-secret"
        assert data["scope"] == "https://graph.microsoft.com/.default"

    def test_client_secret_from_keyvault(self, monkeypatch):
        monkeypatch.setattr(secrets_, "_get_keyvault_secret", lambda url, name: "kv-secret")
        _mock_token_response(monkeypatch, {"access_token": "jwt-kv"})

        headers = build_auth_headers(
            {
                "type": "oauth2_client_credentials",
                "tenant_id": "t",
                "client_id": "app-id-public",
                "client_secret": {
                    "keyvault_url": "https://kv.vault.azure.net",
                    "secret_name": "sp-secret",
                },
            }
        )
        assert headers == {"Authorization": "Bearer jwt-kv"}

    def test_custom_token_url(self, monkeypatch):
        mock = _mock_token_response(monkeypatch, {"access_token": "t1"})
        monkeypatch.setenv("CS", "cs-val")
        build_auth_headers(
            {
                "type": "oauth2_client_credentials",
                "token_url": "https://idp.exemple.com/token",
                "client_id": "cid",
                "client_secret": {"env_var": "CS"},
            }
        )
        assert mock.call_args.args[0] == "https://idp.exemple.com/token"

    def test_http_error_raises(self, monkeypatch):
        monkeypatch.setenv("CS", "x")
        _mock_token_response(monkeypatch, {}, status_code=401)
        with pytest.raises(AuthError, match="401"):
            build_auth_headers(
                {
                    "type": "oauth2_client_credentials",
                    "tenant_id": "t",
                    "client_id": "cid",
                    "client_secret": {"env_var": "CS"},
                }
            )

    def test_missing_tenant_and_url_raises(self):
        with pytest.raises(AuthError, match="token_url.*tenant_id|tenant_id.*token_url"):
            build_auth_headers(
                {"type": "oauth2_client_credentials", "client_id": "c", "client_secret": "s"}
            )


class TestTokenEndpoint:
    def test_login_with_secret_refs(self, monkeypatch):
        monkeypatch.setenv("API_PASSWORD", "pwd123")
        mock = _mock_token_response(monkeypatch, {"data": {"token": "tok-xyz"}})

        headers = build_auth_headers(
            {
                "type": "token_endpoint",
                "token_url": "https://api.exemple.com/login",
                "body": {
                    "username": "svc_flume",
                    "password": {"env_var": "API_PASSWORD"},
                },
                "token_json_path": "data.token",
            }
        )

        assert headers == {"Authorization": "Bearer tok-xyz"}
        assert mock.call_args.args == ("POST", "https://api.exemple.com/login")
        assert mock.call_args.kwargs["json"] == {
            "username": "svc_flume",
            "password": "pwd123",
        }

    def test_form_body_and_custom_header(self, monkeypatch):
        _mock_token_response(monkeypatch, {"access_token": "tok-form"})
        monkeypatch.setenv("K", "kv")
        headers = build_auth_headers(
            {
                "type": "token_endpoint",
                "token_url": "https://api.exemple.com/token",
                "body_format": "form",
                "body": {"key": {"env_var": "K"}},
                "header_name": "X-Auth-Token",
                "value_prefix": "",
            }
        )
        assert headers == {"X-Auth-Token": "tok-form"}

    def test_bad_json_path_raises(self, monkeypatch):
        _mock_token_response(monkeypatch, {"unexpected": "shape"})
        with pytest.raises(AuthError, match="access_token"):
            build_auth_headers(
                {"type": "token_endpoint", "token_url": "https://api.exemple.com/login"}
            )

    def test_missing_url_raises(self):
        with pytest.raises(AuthError, match="token_url"):
            build_auth_headers({"type": "token_endpoint"})

    def test_get_with_secret_ref_in_body_raises(self, monkeypatch):
        monkeypatch.setenv("S", "x")
        with pytest.raises(AuthError, match="GET"):
            build_auth_headers(
                {
                    "type": "token_endpoint",
                    "token_url": "https://api.exemple.com/token",
                    "method": "GET",
                    "body": {"api_key": {"env_var": "S"}},
                }
            )

    def test_get_with_literals_only_is_allowed(self, monkeypatch):
        mock = _mock_token_response(monkeypatch, {"access_token": "t"})
        headers = build_auth_headers(
            {
                "type": "token_endpoint",
                "token_url": "https://api.exemple.com/token",
                "method": "GET",
                "body": {"format": "json"},
            }
        )
        assert headers == {"Authorization": "Bearer t"}
        assert mock.call_args.kwargs["params"] == {"format": "json"}


class TestEdgeCases:
    def test_no_auth_returns_empty(self):
        assert build_auth_headers(None) == {}
        assert build_auth_headers({"type": "none"}) == {}

    def test_unknown_type_raises(self):
        with pytest.raises(AuthError, match="zigzag"):
            build_auth_headers({"type": "zigzag"})


class TestAuthProviderRefresh:
    """Le token d'un run long expire : le provider le renouvelle, par
    anticipation quand l'endpoint annonce une durée de vie, à la demande sinon."""

    SP = {
        "type": "oauth2_client_credentials",
        "token_url": "https://idp.exemple.com/token",
        "client_id": "id",
        "client_secret": "sec",
    }

    def test_static_auth_is_not_refreshable(self, monkeypatch):
        monkeypatch.setenv("T", "tok")
        for config in (
            None,
            {"type": "none"},
            {"type": "bearer_token", "token": {"env_var": "T"}},
            {"type": "api_key_header", "key": {"env_var": "T"}},
        ):
            assert AuthProvider(config).refreshable is False

    def test_token_based_auth_is_refreshable(self):
        assert AuthProvider(self.SP).refreshable is True
        assert AuthProvider(
            {"type": "token_endpoint", "token_url": "https://x/login"}
        ).refreshable is True

    def test_token_is_fetched_once_and_cached(self, monkeypatch):
        mock = _mock_token_response(monkeypatch, {"access_token": "t1"})
        provider = AuthProvider(self.SP)

        assert provider.headers() == {"Authorization": "Bearer t1"}
        assert provider.headers() == {"Authorization": "Bearer t1"}
        assert mock.call_count == 1

    def test_refresh_forces_a_new_token(self, monkeypatch):
        response = MagicMock()
        response.status_code = 200
        response.json.side_effect = [
            {"access_token": "t1"}, {"access_token": "t2"},
        ]
        mock = MagicMock(return_value=response)
        monkeypatch.setattr("flume_lib.auth.requests.post", mock)

        provider = AuthProvider(self.SP)
        assert provider.headers() == {"Authorization": "Bearer t1"}
        assert provider.refresh() == {"Authorization": "Bearer t2"}
        assert provider.headers() == {"Authorization": "Bearer t2"}
        assert mock.call_count == 2

    def test_an_announced_expiry_triggers_a_proactive_refresh(self, monkeypatch):
        response = MagicMock()
        response.status_code = 200
        response.json.side_effect = [
            {"access_token": "t1", "expires_in": 3600},
            {"access_token": "t2", "expires_in": 3600},
        ]
        monkeypatch.setattr("flume_lib.auth.requests.post", MagicMock(return_value=response))

        clock = {"now": 1000.0}
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: clock["now"])

        provider = AuthProvider(self.SP)
        assert provider.headers() == {"Authorization": "Bearer t1"}

        # encore valide une heure moins la marge
        clock["now"] = 1000.0 + 3600 - TOKEN_EXPIRY_MARGIN_SECONDS - 1
        assert provider.headers() == {"Authorization": "Bearer t1"}

        # dans la marge : renouvelé avant que l'API ne réponde 401
        clock["now"] = 1000.0 + 3600 - TOKEN_EXPIRY_MARGIN_SECONDS
        assert provider.headers() == {"Authorization": "Bearer t2"}

    def test_without_expires_in_nothing_is_refreshed_on_its_own(self, monkeypatch):
        mock = _mock_token_response(monkeypatch, {"access_token": "t1"})
        clock = {"now": 0.0}
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: clock["now"])

        provider = AuthProvider(self.SP)
        provider.headers()
        clock["now"] = 10_000_000.0
        provider.headers()
        assert mock.call_count == 1

    def test_token_endpoint_expiry_is_read_where_declared(self, monkeypatch):
        _mock_token_response(
            monkeypatch, {"data": {"token": "t", "ttl": 120}}
        )
        provider = AuthProvider(
            {
                "type": "token_endpoint",
                "token_url": "https://x/login",
                "token_json_path": "data.token",
                "expires_in_json_path": "data.ttl",
            }
        )
        provider.headers()
        assert provider._expires_at is not None

    def test_oauth1_exposes_a_signer_and_no_headers(self, monkeypatch):
        monkeypatch.setenv("CK", "k")
        monkeypatch.setenv("CS", "s")
        provider = AuthProvider(
            {
                "type": "oauth1",
                "consumer_key": {"env_var": "CK"},
                "consumer_secret": {"env_var": "CS"},
            }
        )
        assert provider.headers() == {}
        assert provider.signer is not None
        assert provider.refreshable is False
