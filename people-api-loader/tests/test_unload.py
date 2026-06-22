"""unload: builds per-state INSERT OVERWRITE DIRECTORY, polls, assembles the manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import unload as step

_DDL = (
    'CREATE TABLE public."Voter" (\n'
    '    "id" UUID NOT NULL,\n'
    '    "State" TEXT NOT NULL,\n'
    '    "Mailing_HHGender_Description" TEXT\n'
    ");"
)

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        s3_bucket="b",
        aws_region="us-west-2",
        databricks_warehouse_id="wh-1",
        mart_fqns={"Voter": "cat.dbt.m_people_api__voter"},
        export_prefix=lambda rd: f"voter_export_{rd}",
    ),
)


class _FakeS3:
    """Lists a part file (+ a non-data marker) for FL, one part file for CA."""

    def get_paginator(self, name: str) -> Any:
        pages = {
            "voter_export_20260622/state=FL/": [
                {"Key": "voter_export_20260622/state=FL/part-0", "Size": 10},
                # a non-zero marker/sidecar that must NOT be recorded (would corrupt copy)
                {"Key": "voter_export_20260622/state=FL/_committed_123", "Size": 42},
            ],
            "voter_export_20260622/state=CA/": [{"Key": "voter_export_20260622/state=CA/part-0", "Size": 7}],
        }

        class _P:
            def paginate(self, Bucket: str, Prefix: str) -> Any:
                return iter([{"Contents": pages.get(Prefix, [])}])

        return _P()


def _patch(monkeypatch: pytest.MonkeyPatch, submitted: list[str]) -> None:
    monkeypatch.setattr(step, "STATES", ("FL", "CA"))
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(step, "s3", lambda cfg: _FakeS3())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _run_statement(cfg: object, sql: str, **kw: object) -> object:
        submitted.append(sql)
        return SimpleNamespace(result=SimpleNamespace(data_array=[["FL", "3"], ["CA", "2"]]))

    monkeypatch.setattr(step, "run_statement", _run_statement)


def test_unload_builds_per_state_sql_and_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    manifest = step.run(_CFG, "20260622")
    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    assert len(inserts) == 2
    assert any("state=FL/" in s and "NULL AS `Mailing_HHGender_Description`" in s for s in inserts)
    assert manifest.status == "complete"
    assert manifest.columns == ["id", "State", "Mailing_HHGender_Description"]
    assert manifest.column_types_pg == {"id": "UUID", "State": "TEXT", "Mailing_HHGender_Description": "TEXT"}
    assert manifest.per_state_row_counts == {"FL": 3, "CA": 2}
    assert {f.state for f in manifest.files} == {"FL", "CA"}
    assert any(f.s3_key.endswith("state=FL/part-0") and f.size_bytes == 10 for f in manifest.files)
    # the non-data marker file is filtered out (only part-* data files are recorded)
    assert all("_committed_" not in f.s3_key for f in manifest.files)
    assert len(manifest.files) == 2


def test_unload_state_filter_submits_one_state(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    step.run(_CFG, "20260622", state_filter="FL")
    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    assert len(inserts) == 1 and "state=FL/" in inserts[0]


def test_unload_skip_submit_builds_no_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    manifest = step.run(_CFG, "20260622", skip_submit=True)
    assert submitted == []
    assert manifest.status == "complete"


def test_unload_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260622") is done
