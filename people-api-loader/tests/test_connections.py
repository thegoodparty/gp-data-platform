"""connect_prod / connect_new: fetch an SSM SecureString, tunnel if configured, hand to psycopg."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast

import pytest
from psycopg.conninfo import conninfo_to_dict

from loader.people_api import db
from loader.people_api.config import LoaderConfig


def _patch(monkeypatch: pytest.MonkeyPatch, captured: dict, *, bastion: bool) -> None:
    def _fake_ssm(cfg: object, name: str, **k: object) -> str:
        captured["name"] = name
        return "host=rds.internal dbname=d user=u port=5432"

    monkeypatch.setattr(db, "get_ssm_parameter", _fake_ssm)

    @contextmanager
    def _fake_tunnel(cfg: object, host: str, port: int):
        captured["target"] = (host, port)
        yield ("127.0.0.1", 54321) if bastion else (host, port)

    monkeypatch.setattr(db, "open_tunnel", _fake_tunnel)

    @contextmanager
    def _fake_connect(conninfo: str, **k: object):
        captured["conninfo"] = conninfo
        yield "CONN"

    monkeypatch.setattr(db.psycopg, "connect", _fake_connect)


def test_connect_prod_direct(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=False)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev"
    parts = conninfo_to_dict(captured["conninfo"])
    assert parts["host"] == "rds.internal"
    assert parts["port"] == "5432"


def test_connect_prod_tunneled(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=True)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["target"] == ("rds.internal", 5432)
    parts = conninfo_to_dict(captured["conninfo"])
    assert parts["host"] == "127.0.0.1"
    assert parts["port"] == "54321"


def test_connect_new_uses_run_dated_conn_param(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=False)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}"),
    )
    with db.connect_new(cfg, "20260616") as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev-20260616"
