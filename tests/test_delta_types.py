"""Tests de la conversion JSON -> Arrow. C'est le seul endroit où la lib
transforme réellement des données : une inférence de type ratée y corrompt
silencieusement une colonne entière."""

import json

import arro3.core as ac

from flume_lib._delta import records_to_table


def typed(records, known_types=None):
    """Retourne (types par colonne, valeurs par colonne, dégradations)."""
    table, fallbacks = records_to_table(records, known_types)
    types = {f.name: f.type for f in table.schema}
    values = {
        name: table.column(name).to_pylist() for name in types
    }
    return types, values, fallbacks


class TestScalarInference:
    def test_integers(self):
        types, values, fallbacks = typed([{"n": 1}, {"n": 2}])
        assert types["n"] == ac.DataType.int64()
        assert values["n"] == [1, 2]
        assert fallbacks == []

    def test_floats(self):
        types, values, _ = typed([{"n": 1.5}, {"n": 2.5}])
        assert types["n"] == ac.DataType.float64()
        assert values["n"] == [1.5, 2.5]

    def test_strings(self):
        types, values, _ = typed([{"s": "a"}, {"s": "b"}])
        assert types["s"] == ac.DataType.string()
        assert values["s"] == ["a", "b"]

    def test_booleans(self):
        types, values, _ = typed([{"b": True}, {"b": False}])
        assert types["b"] == ac.DataType.bool()
        assert values["b"] == [True, False]

    def test_nulls_do_not_drive_the_type(self):
        types, values, _ = typed([{"n": None}, {"n": 3}, {"n": None}])
        assert types["n"] == ac.DataType.int64()
        assert values["n"] == [None, 3, None]

    def test_an_all_null_column_is_text(self):
        types, _, _ = typed([{"n": None}, {"n": None}])
        assert types["n"] == ac.DataType.string()


class TestMixedNumerics:
    """Le cas qui corrompait silencieusement les colonnes de montants : la
    première valeur non nulle décidait du type, et un flottant rencontré
    ensuite faisait basculer toute la colonne en texte."""

    def test_an_integer_then_a_float_gives_a_float_column(self):
        types, values, fallbacks = typed([{"amount": 10}, {"amount": 10.5}])
        assert types["amount"] == ac.DataType.float64()
        assert values["amount"] == [10.0, 10.5]
        assert fallbacks == []

    def test_a_float_then_an_integer_gives_a_float_column(self):
        types, values, _ = typed([{"amount": 10.5}, {"amount": 10}])
        assert types["amount"] == ac.DataType.float64()
        assert values["amount"] == [10.5, 10.0]

    def test_the_late_float_is_not_lost(self):
        """Une seule valeur décimale à la centième ligne suffisait à
        détruire le typage des 99 précédentes."""
        records = [{"amount": i} for i in range(99)] + [{"amount": 0.5}]
        types, values, _ = typed(records)
        assert types["amount"] == ac.DataType.float64()
        assert values["amount"][-1] == 0.5


class TestDegradations:
    def test_a_number_mixed_with_text_becomes_text(self):
        types, values, _ = typed([{"id": 1}, {"id": "A-2"}])
        assert types["id"] == ac.DataType.string()
        assert values["id"] == ["1", "A-2"]

    def test_a_boolean_mixed_with_a_number_becomes_text(self):
        types, values, _ = typed([{"flag": True}, {"flag": 1}])
        assert types["flag"] == ac.DataType.string()
        assert values["flag"] == ["True", "1"]

    def test_an_out_of_range_integer_is_reported_not_swallowed(self):
        types, values, fallbacks = typed([{"n": 1}, {"n": 2**70}])
        assert types["n"] == ac.DataType.string()
        assert values["n"] == ["1", str(2**70)]
        # l'ancien repli était muet : la colonne devenait du texte sans trace
        assert len(fallbacks) == 1
        assert "'n'" in fallbacks[0]


class TestNestedStructures:
    def test_an_object_is_serialized_to_json(self):
        types, values, _ = typed([{"customer": {"id": 7, "name": "Ada"}}])
        assert types["customer"] == ac.DataType.string()
        assert json.loads(values["customer"][0]) == {"id": 7, "name": "Ada"}

    def test_a_list_is_serialized_to_json(self):
        _, values, _ = typed([{"tags": ["a", "b"]}])
        assert json.loads(values["tags"][0]) == ["a", "b"]

    def test_accents_are_not_escaped(self):
        _, values, _ = typed([{"o": {"ville": "Nîmes"}}])
        assert "Nîmes" in values["o"][0]

    def test_an_object_mixed_with_a_scalar_stays_readable(self):
        _, values, _ = typed([{"v": {"a": 1}}, {"v": "plain"}])
        assert values["v"] == ['{"a": 1}', "plain"]


class TestColumnSet:
    def test_columns_are_the_union_of_every_record(self):
        types, values, _ = typed([{"a": 1}, {"b": 2}])
        assert set(types) == {"a", "b"}
        assert values["a"] == [1, None]
        assert values["b"] == [None, 2]

    def test_first_seen_order_is_preserved(self):
        types, _, _ = typed([{"z": 1, "a": 2}, {"m": 3}])
        assert list(types) == ["z", "a", "m"]

    def test_no_record_gives_an_empty_table(self):
        table, fallbacks = records_to_table([])
        assert table.num_rows == 0
        assert fallbacks == []


class TestKnownTypes:
    """Depuis l'écriture par lots, deux lots d'un même run pourraient typer
    différemment la même colonne — le commit Delta le refuserait."""

    def test_the_previous_batch_type_wins_over_inference(self):
        # ce lot seul serait int64 ; le précédent l'a écrit en float64
        types, values, fallbacks = typed(
            [{"amount": 1}, {"amount": 2}],
            known_types={"amount": ac.DataType.float64()},
        )
        assert types["amount"] == ac.DataType.float64()
        assert values["amount"] == [1.0, 2.0]
        assert fallbacks == []

    def test_a_new_column_is_still_inferred(self):
        types, _, _ = typed(
            [{"amount": 1, "label": "x"}],
            known_types={"amount": ac.DataType.float64()},
        )
        assert types["label"] == ac.DataType.string()

    def test_an_incompatible_batch_is_reported(self):
        types, values, fallbacks = typed(
            [{"id": "A-1"}], known_types={"id": ac.DataType.int64()}
        )
        assert types["id"] == ac.DataType.string()
        assert values["id"] == ["A-1"]
        assert len(fallbacks) == 1
        assert "'id'" in fallbacks[0]


class TestAnAllNullFirstBatchIsNoLongerSilent:
    """Une colonne sans aucune valeur non nulle au premier lot est typee texte
    faute de mieux. Les lots suivants s'y conforment sans broncher :
    `_normalize` convertit, la construction reussit, et personne n'apprenait
    que la colonne de montants etait devenue du texte."""

    def test_the_degradation_is_reported(self):
        first, _, _ = typed([{"amount": None}, {"amount": None}])
        types, values, fallbacks = typed(
            [{"amount": 10.5}, {"amount": 20}], first
        )
        assert values["amount"] == ["10.5", "20"]
        assert len(fallbacks) == 1
        assert "'amount'" in fallbacks[0]
        assert "Float64" in fallbacks[0]

    def test_the_type_name_is_readable(self):
        """`str()` d'un DataType arro3 rend `arro3.core.DataType<Float64>`
        suivi d'un retour a la ligne : illisible dans log_runs."""
        first, _, _ = typed([{"n": None}])
        _, _, fallbacks = typed([{"n": 1}], first)
        assert "arro3" not in fallbacks[0]
        assert "\n" not in fallbacks[0]

    def test_a_batch_still_all_null_says_nothing(self):
        first, _, _ = typed([{"n": None}])
        _, _, fallbacks = typed([{"n": None}], first)
        assert fallbacks == []

    def test_a_genuinely_textual_column_says_nothing(self):
        """Une colonne texte alimentee par du texte n'est pas une
        degradation."""
        first, _, _ = typed([{"label": "a"}])
        _, _, fallbacks = typed([{"label": "b"}], first)
        assert fallbacks == []

    def test_an_int_to_float_widening_is_not_reported_as_text(self):
        """Le type retenu n'est pas du texte : rien a signaler ici."""
        first, _, _ = typed([{"amount": 1}, {"amount": 2.5}])
        _, values, fallbacks = typed([{"amount": 3}], first)
        assert values["amount"] == [3.0]
        assert fallbacks == []
