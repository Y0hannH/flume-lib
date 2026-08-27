"""La query string ne doit jamais atteindre `error_message`, donc `log_runs`.

`_safe_url` couvre les messages que la lib rédige. Ces tests couvrent l'autre
moitié : ceux que requests et urllib3 forment eux-mêmes, avant que le code de
la lib reprenne la main.
"""

import pytest
import requests

from flume_lib.source import run_source
from flume_lib.urls_ import REDACTED, safe_url, sanitized_request_error, scrub_query
from tests.test_source import BASE_CONFIG, TABLES_PATH, delta, http  # noqa: F401

# Message tel qu'urllib3 le forme réellement : l'URL demandée y est recopiée
# avec sa query string.
URLLIB3_MESSAGE = (
    "HTTPSConnectionPool(host='api.test', port=443): Max retries exceeded "
    "with url: /items?api_key=SUPERSECRET&page=1 (Caused by "
    "NameResolutionError(\"Failed to resolve 'api.test'\"))"
)


class TestScrubQuery:
    def test_the_query_string_is_replaced(self):
        scrubbed = scrub_query(
            URLLIB3_MESSAGE, "https://api.test/items?api_key=SUPERSECRET&page=1"
        )
        assert "SUPERSECRET" not in scrubbed
        assert REDACTED in scrubbed
        # ce qui reste doit encore permettre de diagnostiquer
        assert "api.test" in scrubbed
        assert "/items" in scrubbed

    def test_a_url_without_query_leaves_the_message_intact(self):
        assert scrub_query("boom", "https://api.test/items") == "boom"

    def test_none_and_empty_urls_are_ignored(self):
        assert scrub_query("boom", None, "") == "boom"

    def test_safe_url_drops_query_and_fragment(self):
        assert safe_url("https://api.test/items?k=v#frag") == "https://api.test/items"


class TestSanitizedRequestError:
    def test_the_exception_type_is_preserved(self):
        """Le type décide du rejeu : une ConnectionError assainie doit rester
        une ConnectionError, sans quoi un incident réseau cesserait d'être
        rejoué."""
        original = requests.ConnectionError(URLLIB3_MESSAGE)
        sanitized = sanitized_request_error(
            original, "https://api.test/items?api_key=SUPERSECRET&page=1"
        )
        assert type(sanitized) is requests.ConnectionError
        assert "SUPERSECRET" not in str(sanitized)

    def test_the_prepared_url_is_used_when_available(self):
        """requests ajoute `params=` à l'URL au moment de préparer la requête :
        la query qui apparaît dans le message n'est pas celle qu'on lui a
        passée."""
        original = requests.ConnectionError(URLLIB3_MESSAGE)
        original.request = type("Req", (), {"url": "https://api.test/items?api_key=SUPERSECRET&page=1"})()
        sanitized = sanitized_request_error(original, "https://api.test/items")
        assert "SUPERSECRET" not in str(sanitized)

    def test_an_untouched_message_returns_the_same_exception(self):
        original = requests.ConnectionError("boom")
        assert sanitized_request_error(original, "https://api.test/items") is original


class TestRunSourceDoesNotLeakTheQueryString:
    def test_a_connection_error_reaches_log_runs_without_its_query(
        self, http, delta, monkeypatch  # noqa: F811
    ):
        def explode(self, method, url, **kwargs):
            raise requests.ConnectionError(URLLIB3_MESSAGE)

        monkeypatch.setattr(http, "request", explode)
        config = {**BASE_CONFIG, "params": {"api_key": "SUPERSECRET"},
                  "retry": {"max_attempts": 1}}

        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "SUPERSECRET" not in result.error_message
        # le type est conservé : c'est lui qui dit ce qui s'est passé
        assert result.error_message.startswith("ConnectionError:")
        # et rien de sensible ne part dans log_runs
        assert "SUPERSECRET" not in delta["log"][0]["error_message"]


class TestAuthDoesNotLeakTheTokenUrl:
    def test_a_token_url_query_is_stripped_from_the_http_error(self, monkeypatch):
        from flume_lib import auth

        class Response:
            status_code = 401
            text = "invalid_client"

        monkeypatch.setattr(auth.requests, "post", lambda *a, **k: Response())
        config = {
            "type": "oauth2_client_credentials",
            "token_url": "https://idp.test/token?client_secret=SUPERSECRET",
            "client_id": "id",
            "client_secret": "shh",
        }
        with pytest.raises(auth.AuthError) as excinfo:
            auth.build_auth_headers(config)
        assert "SUPERSECRET" not in str(excinfo.value)
        assert "idp.test/token" in str(excinfo.value)
