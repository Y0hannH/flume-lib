"""Tests du templating de config : substitution, détection des placeholders
orphelins et contrôle des valeurs interpolées."""

import pytest

from flume_lib.templating import (
    TemplateError,
    check_value,
    normalize_value,
    placeholders,
    render,
    templated_placeholders,
)


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
        with pytest.raises(TemplateError, match="forbidden character"):
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


class TestPlaceholders:
    def test_finds_names_in_a_string(self):
        assert placeholders("id > {last_id} and ts > {watermark}") == {
            "last_id", "watermark",
        }

    def test_walks_dicts_and_lists(self):
        assert placeholders({"q": "{a}", "v": {"b": ["{c}", 1]}}) == {"a", "c"}

    def test_empty_containers_and_scalars(self):
        for value in ({}, [], None, 42, True):
            assert placeholders(value) == set()

    def test_a_brace_that_is_not_a_placeholder_is_ignored(self):
        # une requête GraphQL compacte n'est pas un gisement de placeholders
        assert placeholders("{orders{edges{node{id}}}}") == {"id"}


class TestTemplatedPlaceholders:
    BODY = {"q": "id > {key}", "variables": {"after": "{cursor}"}}

    def test_without_template_paths_the_whole_value_is_scanned(self):
        assert templated_placeholders(self.BODY) == {"key", "cursor"}

    def test_template_paths_restrict_the_scan(self):
        assert templated_placeholders(self.BODY, ["variables"]) == {"cursor"}
        assert templated_placeholders(self.BODY, ["q"]) == {"key"}

    def test_several_paths_are_unioned(self):
        assert templated_placeholders(self.BODY, ["q", "variables"]) == {
            "key", "cursor",
        }

    def test_an_absent_path_contributes_nothing(self):
        assert templated_placeholders(self.BODY, ["nope"]) == set()


class TestNormalizeValue:
    """`normalize` reforme la valeur du watermark avant qu'elle reparte vers
    l'API. Le cas d'usage : une API qui date ses enregistrements dans un
    fuseau local mais ne filtre qu'en UTC."""

    def test_none_leaves_the_value_untouched(self):
        # defaut : comportement historique, y compris sur un watermark non
        # textuel qui ne doit pas se retrouver stringifie
        assert normalize_value(42) == 42
        assert normalize_value("2026-08-25T14:57:44.000+02:00") == (
            "2026-08-25T14:57:44.000+02:00"
        )

    def test_offset_is_converted_to_utc(self):
        assert normalize_value("2026-08-25T14:57:44.000+02:00", "utc_iso") == (
            "2026-08-25T12:57:44.000Z"
        )

    def test_negative_offset(self):
        assert normalize_value("2026-08-25T09:57:44.000-05:00", "utc_iso") == (
            "2026-08-25T14:57:44.000Z"
        )

    def test_offset_without_colon(self):
        assert normalize_value("2026-08-25T14:57:44.000+0200", "utc_iso") == (
            "2026-08-25T12:57:44.000Z"
        )

    def test_z_suffix_is_idempotent(self):
        assert normalize_value("2026-08-25T12:57:44.000Z", "utc_iso") == (
            "2026-08-25T12:57:44.000Z"
        )

    def test_naive_value_is_read_as_utc(self):
        # rattacher la valeur au fuseau de la machine ferait dependre la borne
        # de l'endroit ou tourne le notebook
        assert normalize_value("2026-08-25T12:57:44", "utc_iso") == (
            "2026-08-25T12:57:44.000Z"
        )

    def test_space_separator_and_microseconds(self):
        assert normalize_value("2026-08-25 12:57:44.123456", "utc_iso") == (
            "2026-08-25T12:57:44.123Z"
        )

    def test_date_only_is_refused(self):
        # une date nue n'a pas d'instant : la convertir supposerait une heure
        with pytest.raises(TemplateError, match="ISO 8601"):
            normalize_value("2026-08-25", "utc_iso")

    def test_unparsable_value_is_refused(self):
        with pytest.raises(TemplateError, match="ISO 8601"):
            normalize_value("25/08/2026", "utc_iso")

    def test_unknown_normalizer(self):
        with pytest.raises(TemplateError, match="unknown 'normalize'"):
            normalize_value("2026-08-25T12:57:44Z", "utc")

    def test_dst_change_is_absorbed(self):
        # deux instants du 25 octobre 2026 de part et d'autre du changement
        # d'heure : ramenes a l'UTC, ils s'ordonnent correctement, ce que
        # leurs formes locales ne permettaient pas
        before = normalize_value("2026-10-25T02:30:00.000+02:00", "utc_iso")
        after = normalize_value("2026-10-25T02:10:00.000+01:00", "utc_iso")
        assert before == "2026-10-25T00:30:00.000Z"
        assert after == "2026-10-25T01:10:00.000Z"
        assert after > before

