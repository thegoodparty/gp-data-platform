"""resize: flip to serverless v2 + serve params + lock-down, write manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import resize as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        serve_min_acu=0.5,
        serve_max_acu=128.0,
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
    ),
)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class FakeRds:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def modify_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("modify_cluster", kw))

    def modify_db_instance(self, **kw: Any) -> None:
        self.calls.append(("modify_instance", kw))

    def reboot_db_instance(self, **kw: Any) -> None:
        self.calls.append(("reboot", kw))

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


def test_resize_applies_serverless_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = FakeRds()
    captured: dict = {}
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.final_instance_class == "db.serverless"
    assert (manifest.min_acu, manifest.max_acu) == (0.5, 128.0)
    assert manifest.backup_retention_days == 14 and manifest.deletion_protection is True
    by = dict(rds_client.calls)
    assert by["modify_cluster"]["DBClusterParameterGroupName"] == "gp-people-db-20260616-serve"
    assert by["modify_cluster"]["ServerlessV2ScalingConfiguration"] == {
        "MinCapacity": 0.5,
        "MaxCapacity": 128.0,
    }
    assert by["modify_cluster"]["DeletionProtection"] is True
    assert by["modify_instance"]["DBInstanceClass"] == "db.serverless"


def test_resize_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done
