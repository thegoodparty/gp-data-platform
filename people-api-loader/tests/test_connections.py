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
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(
            db_conn_param="people-db-connection-string-dev",
            bastion_enabled=False,
            db_statement_timeout_ms=0,
        ),
    )
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev"
    # Direct path keeps the SSM connection string's fields, with TCP keepalives baked in.
    parts = conninfo_to_dict(captured["conninfo"])
    assert parts["host"] == "rds.internal"
    assert parts["dbname"] == "d"
    assert parts["port"] == "5432"
    assert parts["keepalives"] == "1"


def test_connect_prod_tunneled(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=True)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(
            db_conn_param="people-db-connection-string-dev",
            bastion_enabled=True,
            db_statement_timeout_ms=0,
        ),
    )
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["target"] == ("rds.internal", 5432)
    parts = conninfo_to_dict(captured["conninfo"])
    # Dial the local forward via hostaddr; keep the real host for TLS SNI / cert verification.
    assert parts["hostaddr"] == "127.0.0.1"
    assert parts["host"] == "rds.internal"
    assert parts["port"] == "54321"


def test_connect_new_uses_run_dated_conn_param(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=False)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(
            new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}",
            bastion_enabled=False,
            db_statement_timeout_ms=0,
        ),
    )
    with db.connect_new(cfg, "20260616") as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev-20260616"


class _RecordingConn:
    """Minimal connection stand-in that records SET statements from _apply_session_settings."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _patch_recording(monkeypatch: pytest.MonkeyPatch, conn: _RecordingConn, captured_kw: dict) -> None:
    monkeypatch.setattr(db, "get_ssm_parameter", lambda cfg, name, **k: "host=h dbname=d user=u port=5432")

    @contextmanager
    def _fake_connect(conninfo: str, **k: object):
        captured_kw["conninfo"] = conninfo
        yield conn

    monkeypatch.setattr(db.psycopg, "connect", _fake_connect)


def test_statement_timeout_applied_and_keepalives_set(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _RecordingConn()
    captured_kw: dict = {}
    _patch_recording(monkeypatch, conn, captured_kw)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(db_conn_param="p", bastion_enabled=False, db_statement_timeout_ms=1800000),
    )
    with db.connect_prod(cfg):
        pass
    assert conn.executed == ["SET statement_timeout = 1800000"]
    # TCP keepalives are baked into the conninfo so a dead connection is detected, not hung on.
    assert conninfo_to_dict(captured_kw["conninfo"])["keepalives"] == "1"


def test_statement_timeout_skipped_when_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _RecordingConn()
    _patch_recording(monkeypatch, conn, {})
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(db_conn_param="p", bastion_enabled=False, db_statement_timeout_ms=0),
    )
    with db.connect_prod(cfg):
        pass
    assert conn.executed == []  # 0 disables the timeout (no SET issued)


def test_connect_new_defaults_to_autocommit() -> None:
    # build_indexes._vacuum_analyze relies on this default: VACUUM cannot run inside a transaction
    # block, and _vacuum_analyze never passes autocommit explicitly. If this default ever flips to
    # False, VACUUM would fail in prod — so pin the contract here.
    import inspect

    assert inspect.signature(db.connect_new).parameters["autocommit"].default is True
