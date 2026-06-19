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
        new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}",
        export_prefix=lambda rd: f"voter_export_{rd}",
        s3_bucket="test-bucket",
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
        if "cluster" in self._missing:
            raise _err("DBClusterNotFoundFault")

    def delete_db_cluster(self, **kw: Any) -> None:
        self.calls.append("delete_cluster")
        if "cluster" in self._missing:
            raise _err("DBClusterNotFoundFault")

    def delete_db_cluster_parameter_group(self, **kw: Any) -> None:
        self.calls.append("delete_pg")

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


class FakeSsm:
    def __init__(self, missing: bool = False) -> None:
        self.deleted: list[str] = []
        self._missing = missing  # raise ParameterNotFound (param never written)

    def delete_parameter(self, **kw: Any) -> None:
        if self._missing:
            raise _err("ParameterNotFound")
        self.deleted.append(kw["Name"])


def test_teardown_dry_run_makes_no_aws_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(cfg: object) -> object:
        raise AssertionError("dry-run must not touch AWS")

    monkeypatch.setattr(step, "rds", _boom)
    monkeypatch.setattr(step, "ssm", _boom)
    step.run(_CFG, "20260616")  # confirm=False -> returns before any client


def test_teardown_confirm_deletes_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client, ssm_client = FakeRds(), FakeSsm()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ssm", lambda cfg: ssm_client)
    step.run(_CFG, "20260616", confirm=True)
    assert rds_client.calls == [
        "delete_instance",
        "disable_protection",
        "delete_cluster",
        "delete_pg",
        "delete_pg",
    ]
    assert ssm_client.deleted == ["people-db-connection-string-dev-20260616"]


def test_teardown_idempotent_on_missing_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    # A NotFound on a partially-torn-down run must be swallowed, not raise.
    rds_client, ssm_client = FakeRds(missing={"instance"}), FakeSsm()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ssm", lambda cfg: ssm_client)
    step.run(_CFG, "20260616", confirm=True)  # must not raise
    assert "delete_cluster" in rds_client.calls


def test_teardown_idempotent_on_missing_ssm_param(monkeypatch: pytest.MonkeyPatch) -> None:
    # SSM param never written (provision failed before that step): ParameterNotFound must be
    # swallowed so teardown stays idempotent (exercises the ignore_client_errors guard).
    rds_client, ssm_client = FakeRds(), FakeSsm(missing=True)
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ssm", lambda cfg: ssm_client)
    step.run(_CFG, "20260616", confirm=True)  # must not raise
    assert ssm_client.deleted == []


def test_teardown_idempotent_on_missing_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    # Cluster was never created: modify/delete both raise DBClusterNotFoundFault, which the
    # guards must swallow so teardown stays idempotent (covers the cluster-not-found branch).
    rds_client, ssm_client = FakeRds(missing={"cluster"}), FakeSsm()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ssm", lambda cfg: ssm_client)
    step.run(_CFG, "20260616", confirm=True)  # must not raise
    assert "delete_instance" in rds_client.calls
    assert ssm_client.deleted == ["people-db-connection-string-dev-20260616"]


class _FakePaginator:
    def __init__(self, pages: list[dict]) -> None:
        self._pages = pages

    def paginate(self, **kw: Any) -> Any:
        return iter(self._pages)


class FakeS3:
    def __init__(self, pages: list[dict], delete_errors: list[dict] | None = None) -> None:
        self._pages = pages
        self._delete_errors = delete_errors or []
        self.deleted_keys: list[str] = []

    def get_paginator(self, name: str) -> _FakePaginator:
        return _FakePaginator(self._pages)

    def delete_objects(self, Bucket: str, Delete: dict) -> dict:
        self.deleted_keys.extend(o["Key"] for o in Delete["Objects"])
        return {"Errors": self._delete_errors}


def test_delete_s3_prefix_removes_all_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"Contents": [{"Key": "voter_export_20260616/a"}, {"Key": "voter_export_20260616/b"}]},
        {"Contents": [{"Key": "voter_export_20260616/c"}]},
    ]
    s3_client = FakeS3(pages)
    monkeypatch.setattr(step, "s3", lambda cfg: s3_client)
    step._delete_s3_prefix(_CFG, "20260616")
    assert s3_client.deleted_keys == [
        "voter_export_20260616/a",
        "voter_export_20260616/b",
        "voter_export_20260616/c",
    ]


def test_delete_s3_prefix_raises_on_partial_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # delete_objects reports per-key failures in Errors without raising; we must not
    # silently count them as deleted.
    pages = [{"Contents": [{"Key": "voter_export_20260616/a"}]}]
    s3_client = FakeS3(pages, delete_errors=[{"Key": "voter_export_20260616/a", "Code": "AccessDenied"}])
    monkeypatch.setattr(step, "s3", lambda cfg: s3_client)
    with pytest.raises(RuntimeError, match="partial failure"):
        step._delete_s3_prefix(_CFG, "20260616")
