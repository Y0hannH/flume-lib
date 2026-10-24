"""Robustesse : renouvellement du token, encodage Basic, run interrompu, chemin
du lakehouse non résolu, et types imposés (`write.column_types`).

Chaque classe prouve un défaut constaté à l'audit : un token de 30 s rappelé à
chaque requête, une annulation de notebook sans trace dans `log_runs`, un
montant écrit en flottant, et un cast Arrow qui transforme en silence une
valeur illisible en nul.
"""

# Les fixtures `http` et `delta` sont importées de test_source, puis reçues en
# argument : pytest les résout par nom, ce que F811 prend pour une redéfinition.
# ruff: noqa: F811

import base64
from decimal import Decimal
from unittest.mock import MagicMock

import arro3.core as ac
import pytest

# Import à plat : voir test_error_sanitization.py
from test_source import BASE_CONFIG, FakeResponse, delta, http  # noqa: F401

from flume_lib import auth
from flume_lib._delta import (
    DeltaWriteError,
    parse_column_type,
    query_table,
    records_to_table,
)
from flume_lib.auth import (
    TOKEN_EXPIRY_MARGIN_SECONDS,
    AuthProvider,
    _renewal_instant,
)
from flume_lib.source import LOCAL_LAKEHOUSE_PREFIX, run_source
from flume_lib.validation import ConfigError, validate_config

BASE = {
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}


def cfg(**overrides):
    return {**BASE, **overrides}


# ---------------------------------------------------------------------------
# Marge d'expiration du token
# ---------------------------------------------------------------------------


class TestTokenExpiryMargin:
    SP = {
        "type": "oauth2_client_credentials",
        "token_url": "https://idp.test/token",
        "client_id": "id",
        "client_secret": "secret-value",
    }

    @staticmethod
    def _issuing(monkeypatch, expires_in):
        calls = {"n": 0}

        def post(url, **kwargs):
            calls["n"] += 1
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "access_token": f"tok-{calls['n']}",
                "expires_in": expires_in,
            }
            return response

        monkeypatch.setattr("flume_lib.auth.requests.post", post)
        return calls

    def test_a_short_lived_token_is_not_refetched_on_every_request(self, monkeypatch):
        """Une marge fixe de 60 s déclarait expiré un token de 30 s dès sa
        réception : le endpoint était rappelé à chaque page."""
        calls = self._issuing(monkeypatch, 30)
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: 1000.0)
        provider = AuthProvider(self.SP)
        for _ in range(5):
            provider.headers()
        assert calls["n"] == 1

    def test_it_is_renewed_inside_the_proportional_margin(self, monkeypatch):
        calls = self._issuing(monkeypatch, 30)
        clock = {"now": 1000.0}
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: clock["now"])
        provider = AuthProvider(self.SP)
        provider.headers()

        clock["now"] = 1000.0 + 26.9  # 30 s de vie, 3 s de marge
        provider.headers()
        assert calls["n"] == 1

        clock["now"] = 1000.0 + 27.0
        provider.headers()
        assert calls["n"] == 2

    @pytest.mark.parametrize("lifetime", [0, -5])
    def test_a_non_positive_lifetime_means_unknown(self, monkeypatch, lifetime):
        """Illisible, donc traité comme absent : jamais de rappel en boucle."""
        calls = self._issuing(monkeypatch, lifetime)
        clock = {"now": 0.0}
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: clock["now"])
        provider = AuthProvider(self.SP)
        provider.headers()
        clock["now"] = 10_000.0
        provider.headers()
        assert calls["n"] == 1

    def test_the_margin_is_capped_for_long_lived_tokens(self, monkeypatch):
        monkeypatch.setattr("flume_lib.auth.time.monotonic", lambda: 0.0)
        assert _renewal_instant(86_400) == 86_400 - TOKEN_EXPIRY_MARGIN_SECONDS

    def test_no_announced_lifetime_gives_no_instant(self):
        assert _renewal_instant(None) is None


# ---------------------------------------------------------------------------
# OAuth2 : Basic encodé selon la RFC 6749 §2.3.1 (option)
# ---------------------------------------------------------------------------


class TestUrlencodedBasic:
    CLIENT_ID = "my:client"
    CLIENT_SECRET = "p+ss%word:x y"

    def _header(self, monkeypatch, **extra):
        captured = {}

        def post(url, data=None, headers=None, timeout=None):
            captured.update(headers=headers, data=data)
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {"access_token": "tok-abcdef"}
            return response

        monkeypatch.setattr(auth.requests, "post", post)
        monkeypatch.setenv("CID", self.CLIENT_ID)
        monkeypatch.setenv("CSEC", self.CLIENT_SECRET)
        auth._fetch_oauth2_client_credentials(
            {
                "token_url": "https://idp.test/token",
                "client_auth": "basic",
                "client_id": {"env_var": "CID"},
                "client_secret": {"env_var": "CSEC"},
                **extra,
            }
        )
        token = captured["headers"]["Authorization"].split()[1]
        return base64.b64decode(token).decode()

    def test_the_default_is_unchanged(self, monkeypatch):
        assert self._header(monkeypatch) == f"{self.CLIENT_ID}:{self.CLIENT_SECRET}"

    def test_the_option_form_urlencodes_both_parts(self, monkeypatch):
        decoded = self._header(monkeypatch, urlencode_credentials=True)
        assert decoded == "my%3Aclient:p%2Bss%25word%3Ax+y"

    def test_an_encoded_header_is_masked_in_messages(self, monkeypatch):
        from flume_lib.secrets_ import forget_secrets, redact

        forget_secrets()
        try:
            self._header(monkeypatch, urlencode_credentials=True)
            encoded = base64.b64encode(b"my%3Aclient:p%2Bss%25word%3Ax+y").decode()
            assert redact(f"echo {encoded}") == "echo <redacted>"
        finally:
            forget_secrets()

    def test_the_key_is_accepted_with_basic(self):
        validate_config(
            cfg(
                auth={
                    "type": "oauth2_client_credentials",
                    "token_url": "https://idp.test/token",
                    "client_auth": "basic",
                    "urlencode_credentials": True,
                    "client_id": "id",
                    "client_secret": {"env_var": "S"},
                }
            )
        )

    def test_it_makes_no_sense_in_body_mode(self):
        with pytest.raises(ConfigError, match='"client_auth": "basic"'):
            validate_config(
                cfg(
                    auth={
                        "type": "oauth2_client_credentials",
                        "token_url": "https://idp.test/token",
                        "urlencode_credentials": True,
                        "client_id": "id",
                        "client_secret": {"env_var": "S"},
                    }
                )
            )

    def test_it_must_be_a_boolean(self):
        with pytest.raises(ConfigError, match="urlencode_credentials"):
            validate_config(
                cfg(
                    auth={
                        "type": "oauth2_client_credentials",
                        "token_url": "https://idp.test/token",
                        "client_auth": "basic",
                        "urlencode_credentials": "yes",
                        "client_id": "id",
                        "client_secret": {"env_var": "S"},
                    }
                )
            )


# ---------------------------------------------------------------------------
# Run interrompu
# ---------------------------------------------------------------------------


class TestInterruptedRun:
    CONFIG = {
        **BASE_CONFIG,
        "batch_size": 2,
        "pagination": {"type": "offset", "limit": 2},
    }

    @staticmethod
    def _interrupt_on_second_page(http, monkeypatch, exception):
        pages = [[{"id": 1}, {"id": 2}]]

        def request(self, method, url, **kwargs):
            if not pages:
                raise exception
            return FakeResponse(pages.pop(0))

        monkeypatch.setattr(http, "request", request)

    def test_a_keyboard_interrupt_is_logged_then_raised(self, http, delta, monkeypatch):
        self._interrupt_on_second_page(http, monkeypatch, KeyboardInterrupt())

        with pytest.raises(KeyboardInterrupt):
            run_source(self.CONFIG, lakehouse_tables_path="/tmp/Tables")

        assert len(delta["log"]) == 1
        row = delta["log"][0]
        assert row["status"] == "failed"
        assert row["error_message"].startswith("KeyboardInterrupt: run interrupted")
        # ce que le run a réellement écrit avant d'être annulé
        assert row["rows_loaded"] == 2
        assert len(delta["writes"]) == 1

    def test_a_system_exit_is_logged_then_raised(self, http, delta, monkeypatch):
        self._interrupt_on_second_page(http, monkeypatch, SystemExit(1))
        with pytest.raises(SystemExit):
            run_source(self.CONFIG, lakehouse_tables_path="/tmp/Tables")
        assert delta["log"][0]["error_message"].startswith("SystemExit:")

    def test_the_session_is_closed_on_interruption(self, http, delta, monkeypatch):
        self._interrupt_on_second_page(http, monkeypatch, KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            run_source(self.CONFIG, lakehouse_tables_path="/tmp/Tables")
        assert http.instances[-1].closed is True

    def test_a_dry_run_raises_without_logging(self, http, delta, monkeypatch):
        self._interrupt_on_second_page(http, monkeypatch, KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            run_source(self.CONFIG, lakehouse_tables_path="/tmp/Tables", dry_run=True)
        assert delta["log"] == []

    def test_an_ordinary_error_still_never_raises(self, http, delta, monkeypatch):
        self._interrupt_on_second_page(http, monkeypatch, ValueError("boom"))
        result = run_source(self.CONFIG, lakehouse_tables_path="/tmp/Tables")
        assert result.status == "failed"
        assert "boom" in result.error_message


# ---------------------------------------------------------------------------
# Chemin du lakehouse non résolu
# ---------------------------------------------------------------------------


class TestUnresolvedLakehousePath:
    def test_the_local_mount_path_is_reported(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=LOCAL_LAKEHOUSE_PREFIX + "Tables")

        assert result.status == "success"
        message = next(w for w in result.warnings if "was not resolved" in w)
        assert "os error 1" in message
        assert "lakehouse_tables_path" in message

    def test_an_explicit_path_is_not_reported(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path="abfss://ws@host/lh/Tables")
        assert not any("was not resolved" in w for w in result.warnings)

    def test_a_dry_run_writes_nothing_so_it_says_nothing(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(
            BASE_CONFIG, lakehouse_tables_path=LOCAL_LAKEHOUSE_PREFIX + "Tables", dry_run=True
        )
        assert not any("was not resolved" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Types imposés
# ---------------------------------------------------------------------------


def table_values(records, column_types, known_types=None):
    table, fallbacks = records_to_table(records, known_types, column_types)
    types = {f.name: f.type for f in table.schema}
    return types, {name: table.column(name).to_pylist() for name in types}, fallbacks


class TestParseColumnType:
    @pytest.mark.parametrize(
        "spec, expected",
        [
            ("decimal(18,4)", ("decimal", 18, 4)),
            ("DECIMAL( 18 , 4 )", ("decimal", 18, 4)),
            ("decimal(38,0)", ("decimal", 38, 0)),
            ("date", ("date", None, None)),
            (" String ", ("string", None, None)),
        ],
    )
    def test_valid(self, spec, expected):
        assert parse_column_type(spec) == expected

    @pytest.mark.parametrize(
        "spec",
        [
            "decimal", "decimal(39,2)", "decimal(0,0)", "decimal(4,5)",
            "timestamp", "int", "", 12, None,
        ],
    )
    def test_invalid(self, spec):
        with pytest.raises(ValueError, match="unknown column type"):
            parse_column_type(spec)


class TestDeclaredDecimal:
    def test_floats_ints_and_texts_become_exact_decimals(self):
        types, values, fallbacks = table_values(
            [{"a": 0.1}, {"a": 12}, {"a": "3.1415"}, {"a": None}], {"a": "decimal(18,4)"}
        )
        assert types["a"] == ac.DataType.decimal128(18, 4)
        assert values["a"] == [Decimal("0.1000"), Decimal("12.0000"), Decimal("3.1415"), None]
        assert fallbacks == []

    def test_a_float_is_read_by_its_shortest_form_not_its_binary_expansion(self):
        """0.1 vaut 0.1000000000000000055… en binaire : lu par sa forme la plus
        courte, il tient dans dix décimales."""
        _, values, _ = table_values([{"a": 0.1}], {"a": "decimal(18,10)"})
        assert values["a"] == [Decimal("0.1000000000")]

    def test_float_noise_beyond_the_scale_is_refused_not_rounded(self):
        # 0.1 + 0.2 vaut 0.30000000000000004 : dix-sept décimales de bruit
        with pytest.raises(DeltaWriteError, match="more than 10 decimals"):
            table_values([{"a": 0.1 + 0.2}], {"a": "decimal(18,10)"})

    def test_scientific_notation_is_accepted_when_it_fits(self):
        _, values, _ = table_values([{"a": "1.5e3"}, {"a": 2e0}], {"a": "decimal(18,2)"})
        assert values["a"] == [Decimal("1500.00"), Decimal("2.00")]

    def test_negative_and_zero(self):
        _, values, _ = table_values([{"a": -12.5}, {"a": 0}, {"a": "0.00"}], {"a": "decimal(10,2)"})
        assert values["a"] == [Decimal("-12.50"), Decimal("0.00"), Decimal("0.00")]

    @pytest.mark.parametrize(
        "value, reason",
        [
            ("1.23456", "more than 4 decimals"),
            (1e-07, "more than 4 decimals"),
            ("12345678901234567890", "integer digits"),
            ("abc", "not a decimal number"),
            ("NaN", "finite"),
            ("Infinity", "finite"),
            ("", "not a decimal number"),
            (True, "not a scalar"),
            ({"x": 1}, "not a scalar"),
            ([1], "not a scalar"),
        ],
    )
    def test_a_value_that_does_not_fit_fails_loudly(self, value, reason):
        """Le cast d'Arrow en fait un nul sans rien dire : c'était une perte
        silencieuse de montant."""
        with pytest.raises(DeltaWriteError, match=reason) as exc:
            table_values([{"a": 1}, {"a": value}], {"a": "decimal(18,4)"})
        message = str(exc.value)
        assert "column 'a'" in message
        assert "record 2 of the batch" in message
        # jamais la valeur : le message part dans log_runs
        if isinstance(value, str) and value:
            assert value not in message

    def test_a_declared_type_wins_over_a_known_type(self):
        types, values, _ = table_values(
            [{"a": "1.5"}], {"a": "decimal(10,2)"}, known_types={"a": ac.DataType.float64()}
        )
        assert types["a"] == ac.DataType.decimal128(10, 2)
        assert values["a"] == [Decimal("1.50")]

    def test_other_columns_are_inferred_as_before(self):
        types, _, _ = table_values([{"a": 1.5, "n": 2, "s": "x"}], {"a": "decimal(10,2)"})
        assert types["n"] == ac.DataType.int64()
        assert types["s"] == ac.DataType.string()

    def test_a_declared_column_absent_from_the_records_is_ignored(self):
        types, _, _ = table_values([{"n": 1}], {"ghost": "decimal(10,2)"})
        assert "ghost" not in types


class TestDeclaredDateAndString:
    def test_dates_and_iso_instants(self):
        import datetime

        _, values, _ = table_values(
            [
                {"d": "2026-01-02"},
                {"d": "2026-01-03T04:05:06Z"},
                {"d": "2026-01-04 10:00:00"},
                {"d": None},
            ],
            {"d": "date"},
        )
        assert values["d"] == [
            datetime.date(2026, 1, 2),
            datetime.date(2026, 1, 3),
            datetime.date(2026, 1, 4),
            None,
        ]

    @pytest.mark.parametrize(
        "value", ["2026-13-40", "02/01/2026", "yesterday", 20260102, "2026-02-30"]
    )
    def test_an_invalid_date_fails_loudly(self, value):
        with pytest.raises(DeltaWriteError, match="yyyy-MM-dd"):
            table_values([{"d": value}], {"d": "date"})

    def test_string_forces_text_whatever_the_json_type(self):
        types, values, _ = table_values(
            [{"id": 12345}, {"id": 6.5}, {"id": {"a": 1}}, {"id": None}], {"id": "string"}
        )
        assert types["id"] == ac.DataType.string()
        assert values["id"] == ["12345", "6.5", '{"a": 1}', None]


class TestColumnTypesConfig:
    def write(self, **write):
        return cfg(write=write)

    def test_a_valid_block_is_accepted(self):
        validate_config(
            self.write(column_types={"amount": "decimal(18,4)", "day": "date", "id": "string"})
        )

    @pytest.mark.parametrize("value", [[], {}, "amount", ["a"]])
    def test_it_must_be_a_non_empty_object(self, value):
        with pytest.raises(ConfigError, match="non-empty object"):
            validate_config(self.write(column_types=value))

    def test_an_unknown_type_names_the_column(self):
        with pytest.raises(ConfigError, match=r"column_types\['amount'\].*unknown column type"):
            validate_config(self.write(column_types={"amount": "money"}))

    def test_it_works_with_every_write_mode(self):
        validate_config(
            self.write(mode="merge", merge_keys=["id"], column_types={"amount": "decimal(18,4)"})
        )
        validate_config(self.write(mode="overwrite", column_types={"amount": "decimal(18,4)"}))


# ---------------------------------------------------------------------------
# De bout en bout sur une vraie table Delta
# ---------------------------------------------------------------------------


class _Session:
    payload: list = []

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        return FakeResponse({"items": _Session.payload})


@pytest.fixture
def real_http(monkeypatch):
    monkeypatch.setattr("flume_lib.source.requests.Session", _Session)
    return _Session


def real_config(**write):
    return {
        "name": "typed",
        "base_url": "https://api.test/items",
        "target_schema": "bronze",
        "target_table": "invoices",
        "pagination": {"type": "none", "items_field": "items"},
        "write": write,
    }


class TestColumnTypesOnDelta:
    def test_amounts_are_stored_as_exact_decimals(self, real_http, tmp_path):
        real_http.payload = [
            {"id": 1, "amount": 0.1, "day": "2026-01-02T10:00:00Z"},
            {"id": 2, "amount": "1234567.8901", "day": "2026-02-03"},
        ]
        config = real_config(
            column_types={"amount": "decimal(18,4)", "day": "date", "id": "string"}
        )

        result = run_source(config, lakehouse_tables_path=str(tmp_path))

        assert result.status == "success", result.error_message
        rows = query_table(
            f"{tmp_path}/bronze/invoices", "select * from t order by id", alias="t"
        )
        assert [r["amount"] for r in rows] == [Decimal("0.1000"), Decimal("1234567.8901")]
        assert [str(r["day"]) for r in rows] == ["2026-01-02", "2026-02-03"]
        assert [r["id"] for r in rows] == ["1", "2"]

    def test_a_value_that_does_not_fit_fails_the_run_and_writes_nothing(
        self, real_http, tmp_path
    ):
        real_http.payload = [{"id": 1, "amount": 1.5}, {"id": 2, "amount": "1.23456"}]
        config = real_config(column_types={"amount": "decimal(18,4)"})

        result = run_source(config, lakehouse_tables_path=str(tmp_path))

        assert result.status == "failed"
        assert "column 'amount'" in result.error_message
        assert "1.23456" not in result.error_message
        assert query_table(f"{tmp_path}/bronze/invoices", "select * from t", alias="t") == []

    def test_merge_keeps_the_declared_type_across_runs(self, real_http, tmp_path):
        config = real_config(
            mode="merge", merge_keys=["id"], column_types={"amount": "decimal(18,4)"}
        )
        real_http.payload = [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}]
        assert run_source(config, lakehouse_tables_path=str(tmp_path)).status == "success"

        real_http.payload = [{"id": 2, "amount": "25.5"}, {"id": 3, "amount": 30}]
        second = run_source(config, lakehouse_tables_path=str(tmp_path))

        assert second.status == "success", second.error_message
        assert (second.rows_inserted, second.rows_updated) == (1, 1)
        rows = query_table(
            f"{tmp_path}/bronze/invoices", "select id, amount from t order by id", alias="t"
        )
        assert [r["amount"] for r in rows] == [
            Decimal("10.0000"),
            Decimal("25.5000"),
            Decimal("30.0000"),
        ]
