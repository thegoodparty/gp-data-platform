"""snapshot.load_prod_dump reads the committed file, honoring the env override."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema import snapshot

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_load_prod_dump_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "dump.sql"
    p.write_text('CREATE TABLE public."Voter" ("id" uuid);', encoding="utf-8")
    monkeypatch.setenv("LOADER_PROD_DUMP_PATH", str(p))
    assert 'public."Voter"' in snapshot.load_prod_dump(_CFG, "20260609")


def test_committed_snapshot_has_voter_table() -> None:
    text = (snapshot.DATA_DIR / "prod_dump.sql").read_text(encoding="utf-8")
    assert 'CREATE TABLE public."Voter"' in text
