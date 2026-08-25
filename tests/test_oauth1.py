"""Tests de la signature OAuth 1.0a.

Le test central rejoue le vecteur de référence publié par Twitter (le seul
jeu consommateur/token/nonce/timestamp complet et vérifiable publiquement) :
il valide d'un coup la normalisation des paramètres, la construction de la
base string et le HMAC. Les autres tests couvrent les propriétés dont dépend
une API SQL-over-REST signée : corps JSON non signé, query string signée,
realm hors signature."""

import pytest
import requests

from flume_lib.auth import AuthError, build_auth, build_auth_headers
from flume_lib.oauth1 import OAuth1Signer

# Vecteur de référence Twitter — HMAC-SHA1, corps form-urlencoded
REF = {
    "consumer_key": "xvz1evFS4wEEPTGEFPHBog",
    "consumer_secret": "kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw",
    "token": "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb",
    "token_secret": "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE",
}
REF_URL = "https://api.twitter.com/1/statuses/update.json?include_entities=true"
REF_BODY = (
    "status=Hello%20Ladies%20%2B%20Gentlemen%2C%20a%20signed%20OAuth%20request%21"
)
REF_OAUTH_PARAMS = {
    "oauth_consumer_key": REF["consumer_key"],
    "oauth_nonce": "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg",
    "oauth_signature_method": "HMAC-SHA1",
    "oauth_timestamp": "1318622958",
    "oauth_token": REF["token"],
    "oauth_version": "1.0",
}
REF_SIGNATURE = "tnnArxj06cWHq44gCs1OSKk/jLY="
FORM = "application/x-www-form-urlencoded"


class TestReferenceVector:
    def test_signature_matches_published_value(self):
        signer = OAuth1Signer(signature_method="HMAC-SHA1", **REF)
        signature = signer.sign("POST", REF_URL, REF_BODY, FORM, REF_OAUTH_PARAMS)
        assert signature == REF_SIGNATURE

    def test_base_string_matches_published_value(self):
        signer = OAuth1Signer(signature_method="HMAC-SHA1", **REF)
        base = signer.signature_base_string(
            "POST", REF_URL, REF_BODY, FORM, REF_OAUTH_PARAMS
        )
        assert base.startswith(
            "POST&https%3A%2F%2Fapi.twitter.com%2F1%2Fstatuses%2Fupdate.json&"
        )
        # les paramètres du corps form-urlencoded entrent dans la signature
        assert "status%3DHello%2520Ladies" in base


class TestSqlOverRestShape:
    """POST avec corps JSON et pagination en query string — la forme des APIs
    qui transportent une requête SQL dans le corps."""

    signer = OAuth1Signer(realm="1234567", **REF)
    url = "https://api.example.com/services/rest/query/v1/sql?limit=1000&offset=0"

    def test_json_body_is_not_signed(self):
        params = dict(REF_OAUTH_PARAMS, oauth_signature_method="HMAC-SHA256")
        base = self.signer.signature_base_string(
            "POST", self.url, b'{"q": "SELECT 1"}', "application/json", params
        )
        assert "SELECT" not in base
        assert "limit%3D1000" in base and "offset%3D0" in base

    def test_signature_changes_with_offset(self):
        params = dict(REF_OAUTH_PARAMS, oauth_signature_method="HMAC-SHA256")
        page1 = self.signer.sign("POST", self.url, None, "application/json", params)
        page2 = self.signer.sign(
            "POST", self.url.replace("offset=0", "offset=1000"), None,
            "application/json", params,
        )
        assert page1 != page2

    def test_header_carries_realm_outside_the_signature(self):
        header = self.signer.authorization_header("POST", self.url)
        assert header.startswith('OAuth realm="1234567", ')
        assert 'oauth_signature_method="HMAC-SHA256"' in header
        assert "oauth_signature=" in header
        assert "oauth_nonce=" in header

    def test_nonce_differs_between_requests(self):
        first = self.signer.authorization_header("POST", self.url)
        second = self.signer.authorization_header("POST", self.url)
        assert first != second


class TestSignerBehaviour:
    def test_applies_to_a_prepared_request(self):
        signer = OAuth1Signer(realm="ACCT", **REF)
        request = requests.Request(
            "POST", "https://api.test/sql?limit=10", json={"q": "SELECT 1"}
        ).prepare()
        signer(request)
        assert request.headers["Authorization"].startswith('OAuth realm="ACCT", ')

    def test_two_legged_without_token(self):
        signer = OAuth1Signer(
            consumer_key=REF["consumer_key"], consumer_secret=REF["consumer_secret"]
        )
        header = signer.authorization_header("GET", "https://api.test/things")
        assert "oauth_token=" not in header

    def test_default_port_is_stripped_from_base_uri(self):
        signer = OAuth1Signer(**REF)
        params = dict(REF_OAUTH_PARAMS)
        with_port = signer.signature_base_string(
            "GET", "https://api.test:443/things", None, None, params
        )
        without_port = signer.signature_base_string(
            "GET", "https://api.test/things", None, None, params
        )
        assert with_port == without_port

    def test_non_default_port_is_kept(self):
        signer = OAuth1Signer(**REF)
        base = signer.signature_base_string(
            "GET", "https://api.test:8443/things", None, None, dict(REF_OAUTH_PARAMS)
        )
        assert "%3A8443" in base

    def test_unknown_signature_method_raises(self):
        with pytest.raises(ValueError, match="signature_method"):
            OAuth1Signer(signature_method="HMAC-MD5", **REF)


class TestBuildAuth:
    def _config(self, monkeypatch):
        for name, value in (
            ("NS_CK", REF["consumer_key"]),
            ("NS_CS", REF["consumer_secret"]),
            ("NS_TK", REF["token"]),
            ("NS_TS", REF["token_secret"]),
        ):
            monkeypatch.setenv(name, value)
        return {
            "type": "oauth1",
            "realm": "1234567",
            "consumer_key": {"env_var": "NS_CK"},
            "consumer_secret": {"env_var": "NS_CS"},
            "token": {"env_var": "NS_TK"},
            "token_secret": {"env_var": "NS_TS"},
        }

    def test_returns_a_signer_and_no_static_headers(self, monkeypatch):
        headers, signer = build_auth(self._config(monkeypatch))
        assert headers == {}
        assert isinstance(signer, OAuth1Signer)
        assert signer.realm == "1234567"
        assert signer.signature_method == "HMAC-SHA256"

    def test_credentials_come_from_secret_references(self, monkeypatch):
        _, signer = build_auth(self._config(monkeypatch))
        assert signer.consumer_secret == REF["consumer_secret"]
        assert signer.token == REF["token"]

    def test_other_auth_types_return_no_signer(self, monkeypatch):
        monkeypatch.setenv("T", "abc")
        headers, signer = build_auth({"type": "bearer_token", "token": {"env_var": "T"}})
        assert headers == {"Authorization": "Bearer abc"}
        assert signer is None

    def test_no_auth_returns_nothing(self):
        assert build_auth(None) == ({}, None)

    def test_legacy_header_builder_refuses_oauth1(self, monkeypatch):
        with pytest.raises(AuthError, match="build_auth"):
            build_auth_headers(self._config(monkeypatch))

    def test_missing_token_secret_raises(self, monkeypatch):
        config = self._config(monkeypatch)
        del config["token_secret"]
        with pytest.raises(AuthError, match="token_secret"):
            build_auth(config)
