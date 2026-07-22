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


# --- _conninfo_from_ssm: translate a Prisma-style `?schema=` param to libpq `search_path` ---


def test_conninfo_from_ssm_translates_schema_param_to_search_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        db,
        "get_ssm_parameter",
        lambda cfg, name, **k: "postgresql://u:p@host:5432/db?schema=green",
    )
    cfg = cast(LoaderConfig, SimpleNamespace())
    conninfo = db._conninfo_from_ssm(cfg, "some-param")
    parts = conninfo_to_dict(conninfo)
    assert "schema" not in conninfo
    assert parts["options"] == "-c search_path=green"
    assert parts["host"] == "host"
    assert parts["dbname"] == "db"
    assert parts["keepalives"] == "1"  # keepalives still baked in


def test_conninfo_from_ssm_passthrough_url_without_schema_param(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db, "get_ssm_parameter", lambda cfg, name, **k: "postgresql://u:p@host:5432/db")
    cfg = cast(LoaderConfig, SimpleNamespace())
    conninfo = db._conninfo_from_ssm(cfg, "some-param")
    parts = conninfo_to_dict(conninfo)
    assert "options" not in parts  # no schema param -> conninfo unchanged (no options added)
    assert parts["host"] == "host"
    assert parts["keepalives"] == "1"


def test_conninfo_from_ssm_passthrough_libpq_keyword_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        db, "get_ssm_parameter", lambda cfg, name, **k: "host=rds.internal dbname=d user=u port=5432"
    )
    cfg = cast(LoaderConfig, SimpleNamespace())
    conninfo = db._conninfo_from_ssm(cfg, "some-param")
    parts = conninfo_to_dict(conninfo)
    assert "options" not in parts
    assert parts["host"] == "rds.internal"
    assert parts["keepalives"] == "1"


@pytest.mark.parametrize("bad_schema", ["green; drop table x", "green\\", "green -c foo", "green bar"])
def test_conninfo_from_ssm_rejects_unsafe_schema_value(
    monkeypatch: pytest.MonkeyPatch, bad_schema: str
) -> None:
    from urllib.parse import quote

    monkeypatch.setattr(
        db,
        "get_ssm_parameter",
        lambda cfg, name, **k: f"postgresql://u:p@host:5432/db?schema={quote(bad_schema)}",
    )
    cfg = cast(LoaderConfig, SimpleNamespace())
    with pytest.raises(ValueError, match="schema"):
        db._conninfo_from_ssm(cfg, "some-param")
