"""unload: per-table INSERT OVERWRITE (partitioned per-state + flat whole-mart), manifest assembly."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import unload as step

# All four target tables: Voter/DistrictVoter are State-partitioned; District/DistrictStats are flat.
_DDL = (
    'CREATE TABLE public."Voter" (\n'
    '    "id" UUID NOT NULL,\n'
    '    "State" TEXT NOT NULL,\n'
    '    "Mailing_HHGender_Description" TEXT\n'
    ");\n"
    'CREATE TABLE public."District" (\n'
    '    "id" UUID NOT NULL,\n'
    '    "name" TEXT\n'
    ");\n"
    'CREATE TABLE public."DistrictStats" (\n'
    '    "district_id" UUID NOT NULL,\n'
    '    "buckets" JSONB\n'
    ");\n"
    'CREATE TABLE public."DistrictVoter" (\n'
    '    "id" UUID NOT NULL,\n'
    '    "State" TEXT NOT NULL,\n'
    '    "district_id" UUID\n'
    ");"
)

_MART_FQNS = {
    "Voter": "cat.dbt.m_people_api__voter",
    "District": "cat.dbt.m_people_api__district",
    "DistrictStats": "cat.dbt.m_people_api__districtstats",
    "DistrictVoter": "cat.dbt.m_people_api__districtvoter",
}

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        s3_bucket="b",
        aws_region="us-west-2",
        databricks_warehouse_id="wh-1",
        mart_fqns=_MART_FQNS,
        export_prefix=lambda rd: f"voter_export_{rd}",
    ),
)


class _FakeS3:
    """One part file per requested prefix; the Voter/FL prefix also lists a non-data marker."""

    def get_paginator(self, name: str) -> Any:
        class _P:
            def paginate(self, Bucket: str, Prefix: str) -> Any:
                contents = [{"Key": f"{Prefix}part-0", "Size": 10}]
                if Prefix.endswith("Voter/state=FL/"):
                    # a non-zero marker/sidecar that must NOT be recorded (would corrupt copy)
                    contents.append({"Key": f"{Prefix}_committed_123", "Size": 42})
                return iter([{"Contents": contents}])

        return _P()


def _patch(monkeypatch: pytest.MonkeyPatch, submitted: list[str]) -> None:
    monkeypatch.setattr(step, "STATES", ("FL", "CA"))
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(step, "s3", lambda cfg: _FakeS3())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _run_statement(cfg: object, sql: str, **kw: object) -> object:
        submitted.append(sql)
        if "GROUP BY" in sql:  # count_by_state_statement (partitioned)
            return SimpleNamespace(result=SimpleNamespace(data_array=[["FL", "3"], ["CA", "2"]]))
        if sql.startswith("SELECT count(*)"):  # count_all_statement (flat)
            return SimpleNamespace(result=SimpleNamespace(data_array=[["5"]]))
        return SimpleNamespace(result=None)

    monkeypatch.setattr(step, "run_statement", _run_statement)


def _table(manifest: Any, name: str) -> Any:
    return next(t for t in manifest.tables if t.table == name)


def test_unload_builds_all_tables_partitioned_and_flat(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    manifest = step.run(_CFG, "20260622")
    assert manifest.status == "complete"
    # one UnloadTable per TABLE_SPECS entry, in that order (Voter first).
    assert [t.table for t in manifest.tables] == ["Voter", "District", "DistrictStats", "DistrictVoter"]

    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    # 2 partitioned tables x 2 states + 2 flat tables = 6 unload statements.
    assert len(inserts) == 6

    # Partitioned Voter: per-state under {prefix}/Voter/state={s}/, WHERE State, extras NULLed.
    voter = _table(manifest, "Voter")
    assert voter.partition_by == "State"
    assert voter.row_counts == {"FL": 3, "CA": 2}
    assert voter.columns == ["id", "State", "Mailing_HHGender_Description"]
    assert {f.state for f in voter.files} == {"FL", "CA"}
    assert all(f.table == "Voter" for f in voter.files)
    assert any(f.s3_key.endswith("Voter/state=FL/part-0") and f.size_bytes == 10 for f in voter.files)
    # the non-data marker file is filtered out (only part-* data files recorded)
    assert all("_committed_" not in f.s3_key for f in voter.files)
    assert len(voter.files) == 2
    voter_sql = [s for s in inserts if "/Voter/state=" in s]
    assert any(
        "/Voter/state=FL/" in s and "CAST(NULL AS STRING) AS `Mailing_HHGender_Description`" in s
        for s in voter_sql
    )
    assert all("WHERE `State`" in s and "to_json" not in s for s in voter_sql)

    # Flat District: one unload under {prefix}/District/data/, no WHERE, keyed "".
    district = _table(manifest, "District")
    assert district.partition_by is None
    assert district.row_counts == {"": 5}
    assert [f.state for f in district.files] == [""]
    assert district.files[0].s3_key.endswith("District/data/part-0")
    district_sql = next(s for s in inserts if "/District/data/" in s)
    assert "WHERE" not in district_sql

    # Flat DistrictStats: buckets struct-field rename transform applied, camelCase output keys.
    ds_sql = next(s for s in inserts if "/DistrictStats/data/" in s)
    assert "to_json" in ds_sql
    assert "presenceOfChildren" in ds_sql and "estimatedIncomeRange" in ds_sql
    assert _table(manifest, "DistrictStats").row_counts == {"": 5}


def test_unload_state_filter_touches_only_partitioned_states(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    wrote: list[object] = []
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.append(m) or "uri")
    step.run(_CFG, "20260622", state_filter="FL")
    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    # only the two partitioned tables' FL partition — flat tables are unloaded whole on a full run only.
    assert len(inserts) == 2
    assert all("state=FL/" in s for s in inserts)
    assert not any("/District/data/" in s or "/DistrictStats/data/" in s for s in submitted)
    # a state-filtered run must NOT persist the canonical manifest (would poison the skip-guard)
    assert wrote == []
    # and skips the full-mart per-state count on a --state run
    assert not any("GROUP BY" in s for s in submitted)


def test_unload_skip_submit_builds_no_calls_and_writes_no_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    wrote: list[object] = []
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.append(m) or "uri")
    manifest = step.run(_CFG, "20260622", skip_submit=True)
    assert submitted == []
    assert manifest.status == "complete"
    # a dry-run must NOT persist the manifest — an empty status=complete would poison a later run
    assert wrote == []


def test_unload_rejects_unknown_state(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    # an unknown --state must fail fast (interpolated into SQL + S3 path), not silently no-op
    with pytest.raises(ValueError, match="known state"):
        step.run(_CFG, "20260622", state_filter="ZZ")
    assert submitted == []


def test_unload_tolerates_none_result_on_count_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    # A statement response with no `result` attached must not raise on `.result.data_array` —
    # only the row counts stay empty.
    submitted: list[str] = []
    _patch(monkeypatch, submitted)

    def _run_statement(cfg: object, sql: str, **kw: object) -> object:
        submitted.append(sql)
        return SimpleNamespace(result=None)

    monkeypatch.setattr(step, "run_statement", _run_statement)

    manifest = step.run(_CFG, "20260622")

    assert manifest.status == "complete"
    assert _table(manifest, "Voter").row_counts == {}
    assert _table(manifest, "District").row_counts == {}


def test_unload_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260622") is done
