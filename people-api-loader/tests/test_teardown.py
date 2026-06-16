"""teardown: dry-run by default; on --confirm deletes per-run resources idempotently."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import teardown as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_load_param_group=lambda rd: f"gp-people-db-{rd}-load",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
        new_master_secret_id=lambda rd: f"gp-people-db/{rd}/master",
    ),
)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


def _err(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "op")


class FakeRds:
    def __init__(self, missing: set[str] | None = None) -> None:
        self.calls: list[str] = []
        self._missing = missing or set()  # ops that should raise NotFound

    def delete_db_instance(self, **kw: Any) -> None:
        self.calls.append("delete_instance")
        if "instance" in self._missing:
            raise _err("DBInstanceNotFound")

    def modify_db_cluster(self, **kw: Any) -> None:
        self.calls.append("disable_protection")

    def delete_db_cluster(self, **kw: Any) -> None:
        self.calls.append("delete_cluster")

    def delete_db_cluster_parameter_group(self, **kw: Any) -> None:
        self.calls.append("delete_pg")

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


class FakeSecrets:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete_secret(self, **kw: Any) -> None:
        self.deleted.append(kw["SecretId"])


def test_teardown_dry_run_makes_no_aws_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(cfg: object) -> object:
        raise AssertionError("dry-run must not touch AWS")

    monkeypatch.setattr(step, "rds", _boom)
    monkeypatch.setattr(step, "secrets", _boom)
    step.run(_CFG, "20260616")  # confirm=False -> returns before any client


def test_teardown_confirm_deletes_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client, sm = FakeRds(), FakeSecrets()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "secrets", lambda cfg: sm)
    step.run(_CFG, "20260616", confirm=True)
    assert rds_client.calls == [
        "delete_instance",
        "disable_protection",
        "delete_cluster",
        "delete_pg",
        "delete_pg",
    ]
    assert sm.deleted == ["gp-people-db/20260616/master"]


def test_teardown_idempotent_on_missing_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    # A NotFound on a partially-torn-down run must be swallowed, not raise.
    rds_client, sm = FakeRds(missing={"instance"}), FakeSecrets()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "secrets", lambda cfg: sm)
    step.run(_CFG, "20260616", confirm=True)  # must not raise
    assert "delete_cluster" in rds_client.calls
