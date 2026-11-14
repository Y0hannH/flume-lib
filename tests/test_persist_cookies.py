"""`persist_cookies` : une session `requests` renvoie les cookies qu'on lui pose.

Une API qui répond à la page 1 avec un `Set-Cookie` de session reçoit ce
cookie avec la page 2. Certaines lui donnent alors le pas sur le header
d'authentification et répondent 401 à un token pourtant valide : la source
cassait à la deuxième page, et dans Postman aussi tant qu'on n'effaçait pas les
cookies.

Le serveur est réel, sur la boucle locale : c'est le comportement du jar de
`requests` qu'on vérifie, et une session factice n'en a pas.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from flume_lib.source import _refuse_cookies, run_source
from flume_lib.validation import ConfigError, validate_config

ITEMS = [{"id": n} for n in range(1, 6)]  # 5 lignes : pages de 2, 2 et 1


class _Api(BaseHTTPRequestHandler):
    """Pose un cookie à chaque 200 et refuse toute requête qui en porte un.

    `/items` répond directement. `/moved` redirige vers `/items` en posant un
    cookie sur le 302 : celui-là ne passe jamais par le jar de la session.
    """

    seen: list = []

    def log_message(self, *args):
        pass

    def _reply(self, status, headers=(), body=b""):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parts = urlsplit(self.path)
        cookie = self.headers.get("Cookie")
        _Api.seen.append((parts.path, cookie, self.headers.get("Authorization")))
        if parts.path == "/moved":
            self._reply(302, [
                ("Set-Cookie", "hop=1; Path=/"),
                ("Location", f"/items?{parts.query}"),
            ])
            return
        if cookie:
            self._reply(401, body=b'{"error": "session cookie wins over the bearer"}')
            return
        query = parse_qs(parts.query)
        page, size = int(query["page"][0]), int(query["per_page"][0])
        rows = ITEMS[(page - 1) * size: page * size]
        body = json.dumps(rows).encode()
        self._reply(200, [
            ("Set-Cookie", "_session=abc; Path=/; HttpOnly"),
            ("Content-Type", "application/json"),
        ], body)


@pytest.fixture
def api():
    _Api.seen = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def config(base_url, **overrides):
    return {
        "name": "bookings",
        "base_url": base_url,
        "auth": {"type": "bearer_token", "token": {"env_var": "FLUME_TEST_TOKEN"}},
        "pagination": {
            "type": "page", "page_param": "page", "size_param": "per_page", "page_size": 2,
        },
        "target_schema": "bronze",
        "target_table": "bookings",
        **overrides,
    }


def run(cfg, tmp_path):
    return run_source(cfg, lakehouse_tables_path=str(tmp_path), dry_run=True)


@pytest.fixture(autouse=True)
def token(monkeypatch):
    monkeypatch.setenv("FLUME_TEST_TOKEN", "T0KEN")


def cookies_sent(path="/items"):
    return [cookie for seen_path, cookie, _ in _Api.seen if seen_path == path]


class TestDefault:
    """Le défaut ne change pas : une API qui repose sur sa session la garde."""

    @pytest.mark.parametrize("overrides", [{}, {"persist_cookies": True}])
    def test_the_cookie_of_page_1_is_sent_back_with_page_2(self, api, tmp_path, overrides):
        result = run(config(f"{api}/items", **overrides), tmp_path)

        assert cookies_sent() == [None, "_session=abc"]
        assert result.status == "failed"
        assert "HTTP 401" in result.error_message


class TestRefused:
    def test_every_page_is_sent_without_a_cookie(self, api, tmp_path):
        result = run(config(f"{api}/items", persist_cookies=False), tmp_path)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 5
        assert cookies_sent() == [None, None, None]

    def test_the_bearer_is_still_sent(self, api, tmp_path):
        run(config(f"{api}/items", persist_cookies=False), tmp_path)

        assert {authorization for _, _, authorization in _Api.seen} == {"Bearer T0KEN"}

    def test_a_cookie_set_by_a_redirect_does_not_reach_its_target(self, api, tmp_path):
        """La politique du jar ne couvre pas ce cas : `requests` suit la
        redirection avec un jar propre à la requête."""
        result = run(config(f"{api}/moved", persist_cookies=False), tmp_path)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 5
        assert cookies_sent("/moved") == [None, None, None]
        assert cookies_sent("/items") == [None, None, None]

    def test_the_redirect_cookie_does_reach_its_target_by_default(self, api, tmp_path):
        """Témoin du test précédent : sans l'option, le cookie du 302 suit."""
        result = run(config(f"{api}/moved"), tmp_path)

        assert cookies_sent("/items") == ["hop=1"]
        assert result.status == "failed"

    def test_the_jar_stays_empty(self, api):
        session = requests.Session()
        _refuse_cookies(session)

        session.get(f"{api}/items", params={"page": 1, "per_page": 2})

        assert len(session.cookies) == 0

    def test_a_session_without_a_jar_is_left_alone(self):
        """Les tests substituent une session factice : elle ne tient aucun
        cookie, il n'y a rien à refuser."""

        class Bare:
            pass

        _refuse_cookies(Bare())


class TestValidation:
    @pytest.mark.parametrize("value", [True, False])
    def test_a_boolean_is_accepted(self, value):
        validate_config(config("https://api.test/items", persist_cookies=value))

    @pytest.mark.parametrize("value", ["false", "true", 0, 1, None])
    def test_non_boolean_raises(self, value):
        with pytest.raises(ConfigError, match="'persist_cookies' must be a boolean"):
            validate_config(config("https://api.test/items", persist_cookies=value))

    def test_it_is_refused_before_any_session_is_opened(self, monkeypatch, tmp_path):
        def no_session():
            raise AssertionError("a session was opened")

        monkeypatch.setattr("flume_lib.source.requests.Session", no_session)

        result = run(config("https://api.test/items", persist_cookies="false"), tmp_path)

        assert result.status == "failed"
        assert result.error_message.startswith("ConfigError:")
        assert "persist_cookies" in result.error_message
