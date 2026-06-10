"""resolve_writer_endpoint: env override > provision manifest > error."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.db import resolve_writer_endpoint

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_NEW_WRITER_ENDPOINT", "host-from-env")
    assert resolve_writer_endpoint(_CFG, "20260609") == "host-from-env"


def test_falls_back_to_provision_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_NEW_WRITER_ENDPOINT", raising=False)
    fake = SimpleNamespace(status="complete", writer_endpoint="host-from-manifest")
    monkeypatch.setattr(
        "loader.people_api.db.read_manifest",
        lambda cfg, run_date, step, model: fake,
    )
    assert resolve_writer_endpoint(_CFG, "20260609") == "host-from-manifest"


def test_raises_when_nothing_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_NEW_WRITER_ENDPOINT", raising=False)
    monkeypatch.setattr(
        "loader.people_api.db.read_manifest",
        lambda cfg, run_date, step, model: None,
    )
    with pytest.raises(RuntimeError, match="LOADER_NEW_WRITER_ENDPOINT"):
        resolve_writer_endpoint(_CFG, "20260609")
