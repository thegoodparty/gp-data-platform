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


def test_connect_new_raises_on_unconfigured_prod_db_user() -> None:
    from loader.people_api.db import connect_new

    cfg = cast(
        LoaderConfig,
        SimpleNamespace(prod_db_user="", prod_db_name="people_prod", prod_db_port=5432),
    )
    with (
        pytest.raises(RuntimeError, match="prod_db_user is not configured"),
        connect_new(cfg, "20260609", "ep"),
    ):
        pass


def test_connect_new_raises_on_unconfigured_prod_db_name() -> None:
    from loader.people_api.db import connect_new

    cfg = cast(
        LoaderConfig,
        SimpleNamespace(prod_db_user="people_admin", prod_db_name="", prod_db_port=5432),
    )
    with (
        pytest.raises(RuntimeError, match="prod_db_name is not configured"),
        connect_new(cfg, "20260609", "ep"),
    ):
        pass


def test_connect_prod_uses_ssm_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    # connect_prod fetches the SSM SecureString named by cfg.db_conn_param and hands the
    # decrypted connection string straight to psycopg.connect.
    from contextlib import contextmanager

    from loader.people_api import db

    captured: dict = {}

    def _fake_ssm(cfg: object, name: str, **k: object) -> str:
        captured["name"] = name
        return "host=h dbname=d user=u"

    monkeypatch.setattr(db, "get_ssm_parameter", _fake_ssm)

    @contextmanager
    def _fake_connect(conninfo: str, **k: object):
        captured["conninfo"] = conninfo
        yield "CONN"

    monkeypatch.setattr(db.psycopg, "connect", _fake_connect)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev"
    assert captured["conninfo"] == "host=h dbname=d user=u"
