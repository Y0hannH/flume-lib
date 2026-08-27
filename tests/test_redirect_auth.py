"""Les headers d'auth ne doivent pas suivre une redirection vers un autre hôte.

`requests.Session.rebuild_auth` ne retire que le header littéral
`Authorization` : tout credential porté par un header custom repartait vers la
destination du 302.
"""

import requests

from flume_lib.source import _scope_auth_headers


def _redirect(session, from_url, to_url, headers):
    """Rejoue ce que requests fait sur une redirection : il prépare la
    nouvelle requête avec les headers de la précédente, puis appelle
    `rebuild_auth`."""
    original = requests.Request("GET", from_url).prepare()
    response = requests.Response()
    response.request = original
    prepared = requests.Request("GET", to_url).prepare()
    prepared.headers.update(headers)
    session.rebuild_auth(prepared, response)
    return prepared.headers


class TestCrossHostRedirect:
    def test_a_custom_auth_header_does_not_follow(self):
        session = requests.Session()
        _scope_auth_headers(session, {"X-API-Key"})
        headers = _redirect(
            session, "https://api.test/items", "https://evil.test/items",
            {"X-API-Key": "SUPERSECRET"},
        )
        assert "X-API-Key" not in headers

    def test_authorization_still_does_not_follow(self):
        """Le comportement natif de requests doit être préservé, pas remplacé."""
        session = requests.Session()
        _scope_auth_headers(session, {"X-API-Key"})
        headers = _redirect(
            session, "https://api.test/items", "https://evil.test/items",
            {"Authorization": "Bearer SUPERSECRET"},
        )
        assert "Authorization" not in headers

    def test_a_downgrade_to_http_strips_the_header(self):
        session = requests.Session()
        _scope_auth_headers(session, {"X-API-Key"})
        headers = _redirect(
            session, "https://api.test/items", "http://api.test/items",
            {"X-API-Key": "SUPERSECRET"},
        )
        assert "X-API-Key" not in headers

    def test_the_header_name_is_matched_case_insensitively(self):
        session = requests.Session()
        _scope_auth_headers(session, {"X-Api-Key"})
        headers = _redirect(
            session, "https://api.test/items", "https://evil.test/items",
            {"x-api-key": "SUPERSECRET"},
        )
        assert "X-API-Key" not in headers


class TestSameHostRedirect:
    def test_the_header_follows_a_redirect_on_the_same_host(self):
        """Une API qui redirige vers un de ses propres chemins reste
        utilisable : retirer l'auth ici casserait des sources légitimes."""
        session = requests.Session()
        _scope_auth_headers(session, {"X-API-Key"})
        headers = _redirect(
            session, "https://api.test/items", "https://api.test/v2/items",
            {"X-API-Key": "SUPERSECRET"},
        )
        assert headers["X-API-Key"] == "SUPERSECRET"


class TestNonSession:
    def test_a_session_without_rebuild_auth_is_left_alone(self):
        """Les tests substituent une session factice : elle ne suit aucune
        redirection, il n'y a rien à protéger."""
        fake = type("Fake", (), {"headers": {}})()
        _scope_auth_headers(fake, {"X-API-Key"})  # ne doit pas lever
        assert not hasattr(fake, "rebuild_auth")
