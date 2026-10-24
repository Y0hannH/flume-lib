"""Durcissement de la surface de sécurité : secrets littéraux, origine de
`next_link`, fuites vers `log_runs`, transport en clair.

Chaque classe prouve un comportement qui était absent : les sondes qui ont
justifié ces contrôles (un secret littéral accepté, une page suivante suivie
vers un hôte étranger, des identifiants d'URL recopiés dans un message) sont
devenues des tests.
"""

# Les fixtures `http` et `delta` sont importées de test_source, puis reçues en
# argument : pytest les résout par nom, ce que F811 prend pour une redéfinition.
# ruff: noqa: F811

import base64

import pytest

# Import à plat : voir test_error_sanitization.py
from test_source import BASE_CONFIG, TABLES_PATH, FakeResponse, delta, http  # noqa: F401

from flume_lib import auth
from flume_lib.pagination import PaginationError, check_next_url, paginate
from flume_lib.secrets_ import (
    MIN_REDACTABLE_LENGTH,
    forget_secrets,
    redact,
    register_secret,
    resolve_secret,
)
from flume_lib.source import MAX_RUN_MESSAGE_CHARS, run_source
from flume_lib.urls_ import REDACTED, safe_url
from flume_lib.validation import ConfigError, validate_config

BASE = {
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}
SECRET = "s3cr3t-value-xyz"


def cfg(**overrides):
    return {**BASE, **overrides}


@pytest.fixture(autouse=True)
def _clean_registry():
    forget_secrets()
    yield
    forget_secrets()


# ---------------------------------------------------------------------------
# Masquage des valeurs secrètes
# ---------------------------------------------------------------------------


class TestRedact:
    def test_a_registered_secret_is_masked(self):
        register_secret(SECRET)
        assert redact(f"bad key {SECRET} given") == f"bad key {REDACTED} given"

    def test_the_percent_encoded_form_is_masked_too(self):
        """Un secret renvoyé dans une URL y arrive encodé."""
        register_secret("a+b/c=secret")
        assert "a%2Bb%2Fc%3Dsecret" in "?k=a%2Bb%2Fc%3Dsecret"
        assert redact("?k=a%2Bb%2Fc%3Dsecret") == f"?k={REDACTED}"

    def test_the_longest_secret_wins_over_one_it_contains(self):
        register_secret("abcdefgh")
        register_secret("abcdefgh-and-more")
        assert redact("x abcdefgh-and-more y") == f"x {REDACTED} y"

    def test_a_short_value_is_not_registered(self):
        """Masquer « dev » détruirait les messages sans rien protéger."""
        register_secret("a" * (MIN_REDACTABLE_LENGTH - 1))
        assert redact("aaaaa developer") == "aaaaa developer"

    def test_non_strings_are_ignored(self):
        register_secret(None)
        register_secret(123456789)
        assert redact("123456789") == "123456789"

    def test_empty_text_is_returned_as_is(self):
        assert redact("") == ""

    def test_resolving_an_env_var_registers_it(self, monkeypatch):
        monkeypatch.setenv("API_TOK", f"  {SECRET}\n")
        assert resolve_secret({"env_var": "API_TOK"}) == SECRET
        assert redact(f"echo {SECRET}") == f"echo {REDACTED}"

    def test_resolving_a_key_vault_secret_registers_it(self, monkeypatch):
        monkeypatch.setattr("flume_lib.secrets_._get_keyvault_secret", lambda u, n: SECRET)
        resolve_secret({"keyvault_url": "https://kv.test", "secret_name": "n"})
        assert redact(SECRET) == REDACTED

    def test_a_literal_is_not_registered(self):
        """Un littéral est une valeur non sensible (grant_type…) : le masquer
        abîmerait les messages."""
        resolve_secret("client_credentials")
        assert redact("grant client_credentials") == "grant client_credentials"

    def test_a_basic_header_is_registered(self, monkeypatch):
        monkeypatch.setenv("U", "alice")
        monkeypatch.setenv("P", SECRET)
        headers, _ = auth._build_headers(
            {"type": "basic", "username": {"env_var": "U"}, "password": {"env_var": "P"}}
        )
        encoded = headers["Authorization"].split()[1]
        assert base64.b64decode(encoded).decode() == f"alice:{SECRET}"
        assert redact(f"echo {encoded}") == f"echo {REDACTED}"

    def test_an_obtained_token_is_registered(self, monkeypatch):
        class Response:
            status_code = 200
            text = ""

            def json(self):
                return {"access_token": "tok-from-the-idp-123"}

        monkeypatch.setattr(auth.requests, "post", lambda *a, **k: Response())
        monkeypatch.setenv("CID", "id-public-value")
        monkeypatch.setenv("CS", "client-secret-value")
        auth._build_headers(
            {
                "type": "oauth2_client_credentials",
                "token_url": "https://idp.test/token",
                "client_id": {"env_var": "CID"},
                "client_secret": {"env_var": "CS"},
            }
        )
        assert redact("Bearer tok-from-the-idp-123") == f"Bearer {REDACTED}"


class TestTokenEndpointErrorBody:
    def test_whitespace_is_collapsed(self):
        class Response:
            text = "line one\n   line two\r\n\t line three"

        assert auth._error_detail(Response()) == " — line one line two line three"


# ---------------------------------------------------------------------------
# URL sans identifiants
# ---------------------------------------------------------------------------


class TestSafeUrl:
    def test_userinfo_is_stripped(self):
        assert (
            safe_url("https://user:hunter2@api.test:8443/v1/x?k=1")
            == "https://api.test:8443/v1/x"
        )

    def test_user_without_password_is_stripped_too(self):
        assert safe_url("https://tokenvalue@api.test/x") == "https://api.test/x"

    def test_a_plain_url_is_unchanged_apart_from_the_query(self):
        assert safe_url("https://api.test/v1/x?k=1#frag") == "https://api.test/v1/x"


# ---------------------------------------------------------------------------
# run_source : ce qui sort est masqué et borné
# ---------------------------------------------------------------------------


class TestRunSourcePublishesNoSecret:
    def _failing(self, http, monkeypatch, status, text):
        def respond(self, method, url, **kwargs):
            return FakeResponse({}, status_code=status, text=text)

        monkeypatch.setattr(http, "request", respond)

    def test_a_secret_echoed_by_a_4xx_body_is_masked(self, http, delta, monkeypatch):
        monkeypatch.setenv("TOK", SECRET)
        self._failing(http, monkeypatch, 400, f'{{"error": "invalid token {SECRET}"}}')
        config = {
            **BASE_CONFIG,
            "auth": {"type": "bearer_token", "token": {"env_var": "TOK"}},
        }

        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert SECRET not in result.error_message
        assert REDACTED in result.error_message
        # ce qui est persisté est ce qui a été masqué
        assert SECRET not in delta["log"][0]["error_message"]

    def test_an_echoed_token_obtained_from_the_idp_is_masked(
        self, http, delta, monkeypatch
    ):
        class TokenResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"access_token": "tok-issued-by-the-idp"}

        monkeypatch.setattr(auth.requests, "post", lambda *a, **k: TokenResponse())
        monkeypatch.setenv("CID", "client-id-value")
        monkeypatch.setenv("CS", "client-secret-value")
        self._failing(http, monkeypatch, 403, "forbidden for tok-issued-by-the-idp")
        config = {
            **BASE_CONFIG,
            "auth": {
                "type": "oauth2_client_credentials",
                "token_url": "https://idp.test/token",
                "client_id": {"env_var": "CID"},
                "client_secret": {"env_var": "CS"},
            },
        }

        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert "tok-issued-by-the-idp" not in result.error_message
        assert "tok-issued-by-the-idp" not in delta["log"][0]["error_message"]

    def test_a_secret_in_the_url_path_is_masked(self, http, delta, monkeypatch):
        """Hors query string : `safe_url` ne peut rien, le masquage oui."""
        monkeypatch.setenv("BOT", SECRET)

        def explode(self, method, url, **kwargs):
            raise ValueError(f"cannot reach https://api.test/bot{SECRET}/items")

        monkeypatch.setattr(http, "request", explode)
        config = {
            **BASE_CONFIG,
            "auth": {"type": "api_key_header", "key": {"env_var": "BOT"}},
        }
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert SECRET not in result.error_message

    def test_the_message_is_capped(self, http, delta, monkeypatch):
        def explode(self, method, url, **kwargs):
            raise ValueError("x" * 10_000)

        monkeypatch.setattr(http, "request", explode)
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert len(result.error_message) == MAX_RUN_MESSAGE_CHARS
        assert result.error_message.endswith("…")
        assert len(delta["log"][0]["error_message"]) == MAX_RUN_MESSAGE_CHARS

    def test_a_secret_straddling_the_cap_leaves_no_fragment(
        self, http, delta, monkeypatch
    ):
        """Masquer avant de tronquer : couper d'abord laisserait un morceau de
        secret que plus aucune valeur connue ne reconnaît."""
        monkeypatch.setenv("TOK", SECRET)
        padding = "x" * (MAX_RUN_MESSAGE_CHARS - len("ValueError: ") - 5)

        def explode(self, method, url, **kwargs):
            raise ValueError(padding + SECRET)

        monkeypatch.setattr(http, "request", explode)
        config = {
            **BASE_CONFIG,
            "auth": {"type": "bearer_token", "token": {"env_var": "TOK"}},
        }
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert SECRET[:MIN_REDACTABLE_LENGTH] not in result.error_message

    def test_a_secret_in_a_warning_is_masked(self, http, delta, monkeypatch):
        monkeypatch.setenv("TOK", SECRET)
        http.next_payloads = [[{"id": 1}]]
        delta["write_result"] = ({}, [f"column 'x' degraded near {SECRET}"])
        config = {
            **BASE_CONFIG,
            "auth": {"type": "bearer_token", "token": {"env_var": "TOK"}},
        }

        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success"
        assert all(SECRET not in w for w in result.warnings)
        assert any(REDACTED in w for w in result.warnings)

    def test_a_clean_run_is_untouched(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "success"
        assert result.error_message is None


# ---------------------------------------------------------------------------
# Secrets littéraux
# ---------------------------------------------------------------------------

LITERAL_SECRET_AUTHS = [
    ("bearer_token", {"type": "bearer_token", "token": SECRET}, "token"),
    ("api_key", {"type": "api_key_header", "key": SECRET}, "key"),
    ("basic", {"type": "basic", "username": "u", "password": SECRET}, "password"),
    (
        "oauth2",
        {
            "type": "oauth2_client_credentials",
            "token_url": "https://idp.test/token",
            "client_id": "id",
            "client_secret": SECRET,
        },
        "client_secret",
    ),
    (
        "oauth1_consumer_secret",
        {
            "type": "oauth1",
            "consumer_key": "ck",
            "consumer_secret": SECRET,
        },
        "consumer_secret",
    ),
    (
        "oauth1_token_secret",
        {
            "type": "oauth1",
            "consumer_key": "ck",
            "consumer_secret": {"env_var": "CS"},
            "token": {"env_var": "T"},
            "token_secret": SECRET,
        },
        "token_secret",
    ),
    (
        "oauth1_token",
        {
            "type": "oauth1",
            "consumer_key": "ck",
            "consumer_secret": {"env_var": "CS"},
            "token": SECRET,
            "token_secret": {"env_var": "TS"},
        },
        "token",
    ),
]


class TestLiteralSecrets:
    @pytest.mark.parametrize(
        "auth_config, field",
        [(a, f) for _, a, f in LITERAL_SECRET_AUTHS],
        ids=[i for i, _, _ in LITERAL_SECRET_AUTHS],
    )
    def test_a_literal_secret_is_refused(self, auth_config, field):
        with pytest.raises(ConfigError, match=f"'{field}' is a literal string") as exc:
            validate_config(cfg(auth=auth_config))
        # le message dit où est le problème, jamais ce qu'il contient
        assert SECRET not in str(exc.value)

    @pytest.mark.parametrize(
        "auth_config",
        [a for _, a, _ in LITERAL_SECRET_AUTHS],
        ids=[i for i, _, _ in LITERAL_SECRET_AUTHS],
    )
    def test_the_opt_out_accepts_it(self, auth_config):
        validate_config(cfg(auth=auth_config, allow_literal_secrets=True))

    def test_references_are_accepted(self):
        validate_config(
            cfg(
                auth={
                    "type": "oauth2_client_credentials",
                    "token_url": "https://idp.test/token",
                    "client_id": "public-id",
                    "client_secret": {"keyvault_url": "https://kv.test", "secret_name": "s"},
                }
            )
        )

    def test_the_legacy_env_var_forms_are_accepted(self):
        validate_config(cfg(auth={"type": "bearer_token", "token_env_var": "TOK"}))
        validate_config(
            cfg(
                auth={
                    "type": "basic",
                    "username": "alice",
                    "password_env_var": "PWD",
                }
            )
        )

    def test_public_identifiers_may_be_literal(self):
        validate_config(
            cfg(
                auth={
                    "type": "oauth2_client_credentials",
                    "token_url": "https://idp.test/token",
                    "client_id": "public-id",
                    "client_secret": {"env_var": "CS"},
                }
            )
        )

    def test_the_flag_must_be_a_boolean(self):
        with pytest.raises(ConfigError, match="allow_literal_secrets"):
            validate_config(cfg(allow_literal_secrets="yes"))

    def test_token_endpoint_body_secret_is_refused(self):
        with pytest.raises(ConfigError, match="'password' is a literal string"):
            validate_config(
                cfg(
                    auth={
                        "type": "token_endpoint",
                        "token_url": "https://idp.test/login",
                        "body": {"username": "alice", "password": SECRET},
                    }
                )
            )

    def test_token_endpoint_header_secret_is_refused(self):
        with pytest.raises(ConfigError, match="X-Api-Key"):
            validate_config(
                cfg(
                    auth={
                        "type": "token_endpoint",
                        "token_url": "https://idp.test/login",
                        "headers": {"X-Api-Key": SECRET},
                    }
                )
            )

    def test_token_endpoint_non_sensitive_literals_are_fine(self):
        validate_config(
            cfg(
                auth={
                    "type": "token_endpoint",
                    "token_url": "https://idp.test/login",
                    "body": {
                        "grant_type": "password",
                        "username": "alice",
                        "password": {"env_var": "PWD"},
                    },
                    "headers": {"Accept": "application/json"},
                }
            )
        )

    @pytest.mark.parametrize("name", ["Authorization", "X-API-Key", "x-auth-token"])
    def test_a_credential_in_top_level_headers_is_refused(self, name):
        """Ces headers sont supposés non sensibles : ils ne sont pas retirés à
        une redirection hors origine. Un secret n'a rien à y faire."""
        with pytest.raises(ConfigError, match="literal string") as exc:
            validate_config(cfg(headers={name: SECRET}))
        assert SECRET not in str(exc.value)

    def test_ordinary_top_level_headers_are_fine(self):
        validate_config(cfg(headers={"Accept": "application/json", "Prefer": "transient"}))

    def test_top_level_header_credential_accepted_with_the_opt_out(self):
        validate_config(cfg(headers={"Authorization": SECRET}, allow_literal_secrets=True))


# ---------------------------------------------------------------------------
# URL de configuration : http en clair, identifiants dans l'URL
# ---------------------------------------------------------------------------


class TestConfigUrls:
    def test_plain_http_is_refused(self):
        with pytest.raises(ConfigError, match="plain http://"):
            validate_config(cfg(base_url="http://api.example.com/v1"))

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost:8080/x",
            "http://127.0.0.1/x",
            "http://[::1]:9000/x",
            "http://mock.localhost/x",
        ],
    )
    def test_local_http_is_accepted(self, url):
        validate_config(cfg(base_url=url))

    def test_the_opt_out_accepts_a_trusted_network(self):
        validate_config(cfg(base_url="http://intranet.corp/x", allow_insecure_http=True))

    def test_the_flag_must_be_a_boolean(self):
        with pytest.raises(ConfigError, match="allow_insecure_http"):
            validate_config(cfg(allow_insecure_http=1))

    def test_https_is_accepted(self):
        validate_config(cfg(base_url="https://api.example.com/v1?fixed=1"))

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://files.example.com/x",
            "file:///etc/passwd",
            "api.example.com/v1",
            "/v1/items",
            "https://",
        ],
    )
    def test_anything_but_an_absolute_http_url_is_refused(self, url):
        with pytest.raises(ConfigError, match="absolute http"):
            validate_config(cfg(base_url=url))

    def test_a_non_string_url_is_refused(self):
        with pytest.raises(ConfigError, match="URL string"):
            validate_config(cfg(base_url=123))

    def test_a_bad_port_is_refused(self):
        with pytest.raises(ConfigError, match="not a valid URL"):
            validate_config(cfg(base_url="https://api.example.com:notaport/x"))

    def test_credentials_in_the_url_are_refused(self):
        with pytest.raises(ConfigError, match="must not embed credentials") as exc:
            validate_config(cfg(base_url="https://alice:hunter2@api.example.com/v1"))
        assert "hunter2" not in str(exc.value)

    def test_credentials_in_the_url_are_refused_even_with_the_http_opt_out(self):
        with pytest.raises(ConfigError, match="must not embed credentials"):
            validate_config(
                cfg(base_url="http://a:b@intranet.corp/x", allow_insecure_http=True)
            )

    def test_plain_http_token_url_is_refused(self):
        with pytest.raises(ConfigError, match=r"auth: 'token_url'.*plain http://"):
            validate_config(
                cfg(
                    auth={
                        "type": "oauth2_client_credentials",
                        "token_url": "http://idp.example.com/token",
                        "client_id": "id",
                        "client_secret": {"env_var": "CS"},
                    }
                )
            )

    def test_credentials_in_the_token_url_are_refused(self):
        with pytest.raises(ConfigError, match=r"auth: 'token_url'.*credentials"):
            validate_config(
                cfg(
                    auth={
                        "type": "token_endpoint",
                        "token_url": "https://alice:pw@idp.example.com/login",
                    }
                )
            )

    def test_tenant_id_form_needs_no_token_url(self):
        validate_config(
            cfg(
                auth={
                    "type": "oauth2_client_credentials",
                    "tenant_id": "tenant",
                    "client_id": "id",
                    "client_secret": {"env_var": "CS"},
                }
            )
        )


# ---------------------------------------------------------------------------
# next_link : la page suivante reste chez le même hôte
# ---------------------------------------------------------------------------


def _paged(next_url, base="https://api.example.com/v1", **config):
    """Deux pages : la première désigne `next_url`. Retourne (URLs requêtées,
    pages lues)."""
    seen = []
    payloads = iter(
        [
            ({"items": [{"id": 1}], "next": next_url}, {}),
            ({"items": [{"id": 2}]}, {}),
        ]
    )

    def fetch(url, params):
        seen.append(url)
        return next(payloads)

    pages = list(
        paginate(fetch, base, {}, {"type": "next_link", "items_field": "items", **config})
    )
    return seen, pages


class TestNextLinkOrigin:
    def test_the_same_origin_is_followed(self):
        seen, pages = _paged("https://api.example.com/v1?page=2")
        assert seen == ["https://api.example.com/v1", "https://api.example.com/v1?page=2"]
        assert len(pages) == 2

    def test_the_default_port_is_the_same_origin(self):
        seen, _ = _paged("https://api.example.com:443/v1?page=2")
        assert len(seen) == 2

    def test_the_host_is_compared_case_insensitively(self):
        seen, _ = _paged("https://API.Example.COM/v1?page=2")
        assert len(seen) == 2

    def test_another_host_is_refused_before_any_request(self):
        seen = []
        payloads = iter([({"items": [{"id": 1}], "next": "https://evil.test/steal?x=1"}, {})])

        def fetch(url, params):
            seen.append(url)
            return next(payloads)

        with pytest.raises(PaginationError, match="outside the origin") as exc:
            list(
                paginate(
                    fetch,
                    "https://api.example.com/v1",
                    {},
                    {"type": "next_link", "items_field": "items"},
                )
            )
        assert seen == ["https://api.example.com/v1"]
        message = str(exc.value)
        assert "evil.test" in message
        assert "x=1" not in message  # pas de query string dans le message
        assert "allowed_hosts" in message

    @pytest.mark.parametrize(
        "url",
        [
            "https://api.example.com.evil.test/v1",  # suffixe
            "https://api.example.com@evil.test/v1",  # userinfo trompeur
            "https://api.example.com:8443/v1",  # autre port
            "http://api.example.com/v1",  # rétrogradation
            "//evil.test/v1",  # sans schéma
        ],
    )
    def test_lookalikes_are_refused(self, url):
        with pytest.raises(PaginationError):
            _paged(url)

    def test_a_listed_host_is_followed(self):
        seen, _ = _paged("https://cdn.example.net/page2", allowed_hosts=["cdn.example.net"])
        assert seen[1] == "https://cdn.example.net/page2"

    def test_allowed_hosts_are_matched_case_insensitively(self):
        seen, _ = _paged("https://CDN.example.net/page2", allowed_hosts=["Cdn.Example.Net"])
        assert len(seen) == 2

    def test_a_listed_host_never_allows_a_downgrade_to_http(self):
        with pytest.raises(PaginationError, match="downgrades https to http"):
            _paged("http://cdn.example.net/page2", allowed_hosts=["cdn.example.net"])

    def test_a_host_outside_the_list_is_still_refused(self):
        with pytest.raises(PaginationError, match="outside the origin"):
            _paged("https://evil.test/x", allowed_hosts=["cdn.example.net"])

    def test_a_relative_url_is_left_to_requests(self):
        """Pas d'hôte à contrôler ; `requests` refusera l'URL sans schéma."""
        assert check_next_url("/v1?page=2", "https://api.example.com/v1") == "/v1?page=2"

    def test_a_non_string_is_an_error_not_a_request(self):
        with pytest.raises(PaginationError, match="not a URL"):
            check_next_url({"href": "x"}, "https://api.example.com/v1")

    def test_an_unreadable_url_is_an_error(self):
        with pytest.raises(PaginationError, match="unreadable"):
            check_next_url("https://api.example.com:notaport/x", "https://api.example.com/v1")

    def test_an_empty_next_field_ends_pagination(self):
        seen, pages = _paged("")
        assert len(seen) == 1 and len(pages) == 1

    def test_a_cross_host_link_fails_the_run_with_the_reason(self, http, delta):
        http.next_payloads = [{"data": [{"id": 1}], "next": "https://evil.test/steal"}]
        config = {
            **BASE_CONFIG,
            "pagination": {"type": "next_link", "items_field": "data"},
        }

        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "outside the origin" in result.error_message
        assert len(http.instances[-1].calls) == 1  # rien n'est parti vers l'hôte étranger


class TestAllowedHostsConfig:
    NEXT_LINK = {"type": "next_link"}

    def test_a_valid_list_is_accepted(self):
        hosts = ["cdn.example.net", "eu.example.net"]
        validate_config(cfg(pagination={**self.NEXT_LINK, "allowed_hosts": hosts}))

    @pytest.mark.parametrize(
        "hosts",
        [[], "cdn.example.net", ["https://cdn.example.net"], ["cdn.example.net:8443"],
         ["cdn.example.net/x"], ["a@b"], [""], [1], ["with space"]],
    )
    def test_a_malformed_list_is_refused(self, hosts):
        with pytest.raises(ConfigError, match="allowed_hosts"):
            validate_config(cfg(pagination={**self.NEXT_LINK, "allowed_hosts": hosts}))

    def test_it_only_makes_sense_with_next_link(self):
        with pytest.raises(ConfigError, match="unknown key 'allowed_hosts'"):
            validate_config(
                cfg(pagination={"type": "offset", "allowed_hosts": ["cdn.example.net"]})
            )
