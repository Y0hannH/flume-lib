import base64

import pytest

from flume_lib.auth import AuthError, build_auth_headers


class TestBearerToken:
    def test_builds_header(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "secret123")
        headers = build_auth_headers({"type": "bearer_token", "token_env_var": "MY_TOKEN"})
        assert headers == {"Authorization": "Bearer secret123"}

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_TOKEN", raising=False)
        with pytest.raises(AuthError, match="MISSING_TOKEN"):
            build_auth_headers({"type": "bearer_token", "token_env_var": "MISSING_TOKEN"})

    def test_missing_config_key_raises(self):
        with pytest.raises(AuthError, match="token_env_var"):
            build_auth_headers({"type": "bearer_token"})


class TestApiKeyHeader:
    def test_default_header_name(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "k1")
        headers = build_auth_headers({"type": "api_key_header", "key_env_var": "MY_KEY"})
        assert headers == {"X-API-Key": "k1"}

    def test_custom_header_name(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "k1")
        headers = build_auth_headers(
            {"type": "api_key_header", "key_env_var": "MY_KEY", "header_name": "Ocp-Apim-Subscription-Key"}
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


class TestEdgeCases:
    def test_no_auth_returns_empty(self):
        assert build_auth_headers(None) == {}
        assert build_auth_headers({"type": "none"}) == {}

    def test_oauth2_is_stub(self):
        with pytest.raises(NotImplementedError):
            build_auth_headers({"type": "oauth2_client_credentials"})

    def test_unknown_type_raises(self):
        with pytest.raises(AuthError, match="zigzag"):
            build_auth_headers({"type": "zigzag"})
