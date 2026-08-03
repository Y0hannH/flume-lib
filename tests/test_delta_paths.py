"""Tests de la résolution du chemin lakehouse vers OneLake (ABFSS) et des
storage options Fabric, avec un faux notebookutils injecté dans sys.modules."""

import sys
import types

import pytest

from flume_lib._delta import (
    resolve_lakehouse_tables_path,
    storage_options_for,
)


@pytest.fixture
def fake_notebookutils(monkeypatch):
    module = types.ModuleType("notebookutils")
    module.runtime = types.SimpleNamespace(
        context={
            "currentWorkspaceId": "ws-123",
            "defaultLakehouseId": "lh-456",
        }
    )
    module.credentials = types.SimpleNamespace(
        getToken=lambda audience: f"tok-{audience}",
        getSecret=lambda url, name: "kv-secret",
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    return module


class TestResolveLakehouseTablesPath:
    def test_resolves_default_path_in_fabric(self, fake_notebookutils):
        assert resolve_lakehouse_tables_path("/lakehouse/default/Tables") == (
            "abfss://ws-123@onelake.dfs.fabric.microsoft.com/lh-456/Tables"
        )

    def test_keeps_abfss_path_unchanged(self, fake_notebookutils):
        uri = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables"
        assert resolve_lakehouse_tables_path(uri) == uri

    def test_keeps_custom_local_path_unchanged(self, fake_notebookutils):
        assert resolve_lakehouse_tables_path("/tmp/tables") == "/tmp/tables"

    def test_outside_fabric_keeps_path(self, monkeypatch):
        monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
        assert (
            resolve_lakehouse_tables_path("/lakehouse/default/Tables")
            == "/lakehouse/default/Tables"
        )

    def test_missing_context_keys_keeps_path(self, fake_notebookutils):
        fake_notebookutils.runtime.context = {}
        assert (
            resolve_lakehouse_tables_path("/lakehouse/default/Tables")
            == "/lakehouse/default/Tables"
        )


class TestStorageOptionsFor:
    def test_explicit_options_win(self, fake_notebookutils):
        options = {"bearer_token": "explicite"}
        assert storage_options_for("abfss://x@y/z", options) == options

    def test_abfss_in_fabric_gets_bearer_token(self, fake_notebookutils):
        options = storage_options_for("abfss://x@y/z", None)
        assert options == {"bearer_token": "tok-storage", "use_fabric_endpoint": "true"}

    def test_local_path_gets_no_options(self, fake_notebookutils):
        assert storage_options_for("/tmp/tables/t", None) is None

    def test_abfss_outside_fabric_gets_none(self, monkeypatch):
        monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
        assert storage_options_for("abfss://x@y/z", None) is None
