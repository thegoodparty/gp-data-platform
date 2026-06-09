"""snapshot loaders read committed files, honoring env path overrides."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema import snapshot

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_load_prod_dump_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "dump.sql"
    p.write_text('CREATE TABLE public."VoterTX" ("LALVOTERID" text);', encoding="utf-8")
    monkeypatch.setenv("LOADER_PROD_DUMP_PATH", str(p))
    assert "VoterTX" in snapshot.load_prod_dump(_CFG, "20260609")


def test_load_databricks_columns_list_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "cols.json"
    p.write_text(json.dumps([{"name": "Voters_Age", "type": "string"}]), encoding="utf-8")
    monkeypatch.setenv("LOADER_DATABRICKS_COLUMNS_PATH", str(p))
    cols = snapshot.load_databricks_columns(_CFG, "20260609")
    assert cols == {"Voters_Age": "string"}


def test_load_databricks_columns_dict_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "cols.json"
    p.write_text(json.dumps({"Voters_Age": "string"}), encoding="utf-8")
    monkeypatch.setenv("LOADER_DATABRICKS_COLUMNS_PATH", str(p))
    assert snapshot.load_databricks_columns(_CFG, "20260609") == {"Voters_Age": "string"}


def test_committed_snapshot_files_exist() -> None:
    # Task 4 must populate these with real prod data before merge.
    assert (snapshot.DATA_DIR / "prod_dump.sql").exists()
    assert (snapshot.DATA_DIR / "databricks_columns.json").exists()
