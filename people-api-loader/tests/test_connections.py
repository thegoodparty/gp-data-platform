"""connect_prod / connect_new: both fetch an SSM SecureString and hand it to psycopg."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api import db
from loader.people_api.config import LoaderConfig


def _patch_ssm_and_psycopg(monkeypatch: pytest.MonkeyPatch, captured: dict) -> None:
    def _fake_ssm(cfg: object, name: str, **k: object) -> str:
        captured["name"] = name
        return "host=h dbname=d user=u"

    monkeypatch.setattr(db, "get_ssm_parameter", _fake_ssm)

    @contextmanager
    def _fake_connect(conninfo: str, **k: object):
        captured["conninfo"] = conninfo
        yield "CONN"

    monkeypatch.setattr(db.psycopg, "connect", _fake_connect)


def test_connect_prod_uses_db_conn_param(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch_ssm_and_psycopg(monkeypatch, captured)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev"
    assert captured["conninfo"] == "host=h dbname=d user=u"


def test_connect_new_uses_run_dated_conn_param(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch_ssm_and_psycopg(monkeypatch, captured)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}"),
    )
    with db.connect_new(cfg, "20260616") as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev-20260616"
    assert captured["conninfo"] == "host=h dbname=d user=u"
