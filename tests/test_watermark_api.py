"""Tests des fonctions publiques du watermark : lecture, écriture, remise à zéro,
chemin par défaut. Vraies tables Delta, dans un répertoire temporaire."""

import pytest

from flume_lib import read_watermark, reset_watermark, run_source, write_watermark
from flume_lib._delta import query_table

TABLES = "/tmp/Tables"


class TestPublicWatermarkApi:
    def test_read_write_and_reset_round_trip(self, tmp_path):
        tables = str(tmp_path)
        assert read_watermark(tables, "a", schema="flume") is None

        write_watermark(tables, "a", "2026-01-01", schema="flume")
        write_watermark(tables, "a", "2026-02-01", schema="flume")
        write_watermark(tables, "b", "99", schema="flume")
        assert read_watermark(tables, "a", schema="flume") == "2026-02-01"

        assert reset_watermark("a", lakehouse_tables_path=tables) == 2
        assert read_watermark(tables, "a", schema="flume") is None
        # les autres sources ne sont pas touchées
        assert read_watermark(tables, "b", schema="flume") == "99"

    def test_reset_on_a_missing_table_is_a_noop(self, tmp_path):
        assert reset_watermark("a", lakehouse_tables_path=str(tmp_path)) == 0

    def test_reset_honours_the_schema(self, tmp_path):
        write_watermark(str(tmp_path), "a", "1", schema="ops")
        assert reset_watermark("a", schema="flume", lakehouse_tables_path=str(tmp_path)) == 0
        assert reset_watermark("a", schema="ops", lakehouse_tables_path=str(tmp_path)) == 1

    def test_a_quote_in_the_source_name_cannot_widen_the_delete(self, tmp_path):
        tables = str(tmp_path)
        write_watermark(tables, "a", "1", schema="flume")
        reset_watermark("x' OR '1'='1", lakehouse_tables_path=tables)

        assert read_watermark(tables, "a", schema="flume") == "1"

    def test_reset_requires_a_source_name(self):
        with pytest.raises(ValueError, match="source_name"):
            reset_watermark("")

    def test_read_requires_a_source_name(self):
        with pytest.raises(TypeError, match="source_name"):
            read_watermark(TABLES)

    def test_write_requires_a_value(self):
        with pytest.raises(TypeError, match="last_value"):
            write_watermark(TABLES, "a")

    def test_the_default_path_is_resolved_like_run_source(self, monkeypatch):
        seen = []
        monkeypatch.setattr(
            "flume_lib.watermark.resolve_lakehouse_tables_path",
            lambda path: seen.append(path) or "/resolved",
        )
        monkeypatch.setattr(
            "flume_lib.watermark.query_table",
            lambda uri, *a, **k: seen.append(uri) or [],
        )
        read_watermark(source_name="a")

        assert seen == ["/lakehouse/default/Tables", "/resolved/flume/watermark"]

    def test_run_source_and_the_public_functions_see_the_same_table(self, tmp_path, monkeypatch):
        class Session:
            def __init__(self):
                self.headers = {}

            def close(self):
                pass

            def request(self, method, url, **kwargs):
                class Response:
                    status_code = 200
                    headers = {}

                    def json(self):
                        return [{"id": 1, "ts": "2026-09-10"}]

                return Response()

        monkeypatch.setattr("flume_lib.source.requests.Session", Session)
        config = {
            "name": "wd_C1",
            "base_url": "https://api.test/items",
            "target_schema": "bronze",
            "target_table": "items",
            "incremental": {"enabled": True, "field": "ts", "param_name": "since"},
        }
        run_source(config, lakehouse_tables_path=str(tmp_path))

        rows = query_table(f"{tmp_path}/flume/watermark", "select * from t", alias="t")
        assert [r["last_value"] for r in rows] == ["2026-09-10"]
        assert read_watermark(str(tmp_path), "wd_C1", schema="flume") == "2026-09-10"
