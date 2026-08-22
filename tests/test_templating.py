"""Tests du templating de config : substitution, détection des placeholders
orphelins et contrôle des valeurs interpolées."""

import pytest

from flume_lib.templating import TemplateError, check_value, render


class TestRender:
    def test_substitutes_in_a_string(self):
        assert render("id > {last}", {"last": 42}) == "id > 42"

    def test_substitutes_in_nested_structures(self):
        value = {"q": "WHERE d >= '{wm}'", "opts": [{"since": "{wm}"}]}
        assert render(value, {"wm": "2026-01-01"}) == {
            "q": "WHERE d >= '2026-01-01'",
            "opts": [{"since": "2026-01-01"}],
        }

    def test_repeated_placeholder(self):
        assert render("{a} et {a}", {"a": "x"}) == "x et x"

    def test_leaves_non_strings_alone(self):
        assert render({"n": 3, "flag": True, "none": None}, {"a": 1}) == {
            "n": 3,
            "flag": True,
            "none": None,
        }

    def test_without_variables_nothing_is_touched(self):
        # une config sans templating ne doit pas se mettre à échouer sur des
        # accolades qui appartiennent à la requête
        payload = {"filter": "{unbalanced} {x}"}
        assert render(payload, {}) == payload

    def test_unknown_placeholder_raises(self):
        with pytest.raises(TemplateError, match="watermak"):
            render("d >= '{watermak}'", {"watermark": "2026-01-01"})

    def test_error_lists_available_variables(self):
        with pytest.raises(TemplateError, match="watermark"):
            render("{typo}", {"watermark": "2026-01-01"})

    def test_value_containing_braces_is_not_rescanned(self):
        assert render("{a}", {"a": "{b}"}) == "{b}"


class TestCheckValue:
    @pytest.mark.parametrize(
        "value", ["2026-01-01'; DROP TABLE t --", 'a" or 1=1', "x; y", "a--b", "a\nb"]
    )
    def test_rejects_structure_breaking_values(self, value):
        with pytest.raises(TemplateError, match="interdit"):
            check_value(value)

    def test_accepts_a_plain_timestamp(self):
        assert check_value("2026-08-22 10:30:00") == "2026-08-22 10:30:00"

    def test_coerces_to_text(self):
        assert check_value(42) == "42"

    def test_numeric_format(self):
        assert check_value(1234, "numeric") == "1234"
        with pytest.raises(TemplateError, match="numeric"):
            check_value("abc", "numeric")

    def test_iso_datetime_format(self):
        assert check_value("2026-08-22T10:30:00Z", "iso_datetime")
        assert check_value("2026-08-22 10:30:00", "iso_datetime")
        with pytest.raises(TemplateError, match="iso_datetime"):
            check_value("22/08/2026", "iso_datetime")

    def test_iso_date_format(self):
        assert check_value("2026-08-22", "iso_date")
        with pytest.raises(TemplateError, match="iso_date"):
            check_value("2026-08-22 10:00:00", "iso_date")

    def test_unknown_format_raises(self):
        with pytest.raises(TemplateError, match="value_format"):
            check_value("x", "epoch_millis")

    def test_label_appears_in_the_message(self):
        with pytest.raises(TemplateError, match="mon_champ"):
            check_value("a;b", label="mon_champ")
