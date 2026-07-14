"""UnloadManifest shape: per-table sub-records (partitioned + flat) and table-stamped files."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from loader.people_api.manifests import UnloadFile, UnloadManifest, UnloadTable


def test_unload_manifest_round_trips_partitioned_and_flat_tables() -> None:
    manifest = UnloadManifest(
        run_date="20260622",
        status="complete",
        started_at=datetime(2026, 6, 22, tzinfo=UTC),
        tables=[
            UnloadTable(
                table="Voter",
                databricks_table="cat.dbt.m_people_api__voter",
                partition_by="State",
                columns=["id", "State"],
                column_types_pg={"id": "UUID", "State": "TEXT"},
                row_counts={"FL": 3, "CA": 2},
                files=[
                    UnloadFile(
                        table="Voter",
                        state="FL",
                        s3_key="voter_export_20260622/Voter/state=FL/part-0",
                        size_bytes=10,
                        row_count=0,
                    )
                ],
            ),
            UnloadTable(
                table="District",
                databricks_table="cat.dbt.m_people_api__district",
                partition_by=None,
                columns=["id", "name"],
                column_types_pg={"id": "UUID", "name": "TEXT"},
                row_counts={"": 42},
                files=[
                    UnloadFile(
                        table="District",
                        state="",
                        s3_key="voter_export_20260622/District/data/part-0",
                        size_bytes=7,
                        row_count=0,
                    )
                ],
            ),
        ],
    )

    reloaded = UnloadManifest.model_validate_json(manifest.model_dump_json())
    assert [t.table for t in reloaded.tables] == ["Voter", "District"]
    voter = reloaded.tables[0]
    assert voter.partition_by == "State"
    assert voter.row_counts == {"FL": 3, "CA": 2}
    assert voter.files[0].table == "Voter"
    district = reloaded.tables[1]
    assert district.partition_by is None
    assert district.row_counts == {"": 42}
    assert district.files[0].state == ""


def test_unload_file_requires_table() -> None:
    with pytest.raises(ValidationError):
        UnloadFile(state="FL", s3_key="k", size_bytes=1, row_count=0)  # ty: ignore[missing-argument]
