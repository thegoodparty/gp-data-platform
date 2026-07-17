"""People-API manifest schemas (pydantic).

Every step writes one of these to S3 at
`s3://{bucket}/voter_export_{date}/_manifest/{step}.json`. Step re-entry
reads the manifest first and no-ops if `status == "complete"`.

Shape is the loader's manifest contract. Keep in sync — external review
(the validate.md human-readable report) depends on these field names.

Step modules import everything they need from this module: both the
concrete manifest models and the read/write helpers (re-exported from
`loader.core.manifest.io` so step bodies have a single import surface).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from loader.core.manifest.io import (
    load_artifact_json,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.core.manifest.models import ManifestBase, Status

__all__ = [
    "CopyManifest",
    "CopyTableResult",
    "IndexManifest",
    "IndexSpec",
    "InspectManifest",
    "ManifestBase",
    "ProvisionManifest",
    "ResizeManifest",
    "SchemaManifest",
    "Status",
    "TableInspection",
    "UnloadFile",
    "UnloadManifest",
    "UnloadTable",
    "ValidateManifest",
    "ValidationCheck",
    "load_artifact_json",
    "manifest_uri",
    "put_artifact",
    "read_manifest",
    "write_manifest",
]


class TableInspection(BaseModel):
    table: str
    total_row_count: int
    # Empty when the table has no "State" column (e.g. District*); per-state is only
    # meaningful for state-partitioned tables like Voter.
    per_state_row_counts: dict[str, int] = Field(default_factory=dict)
    # max(updated_at) per state, ISO-8601 — the L2 snapshot freshness. Empty when the
    # table lacks "State" or "updated_at".
    per_state_snapshot_dates: dict[str, str] = Field(default_factory=dict)


class InspectManifest(ManifestBase):
    step: Literal["inspect"] = "inspect"
    prod_cluster_id: str
    # Per-table prod inspection (Voter + District family). Voter's per_state_row_counts
    # is the "old" baseline the validate step diffs the new cluster against.
    tables: list[TableInspection]


class UnloadFile(BaseModel):
    table: str
    state: str  # "" for a flat (non-partitioned) table's single unit
    s3_key: str
    size_bytes: int
    row_count: int


class UnloadTable(BaseModel):
    table: str
    databricks_table: str
    partition_by: str | None
    columns: list[str]
    column_types_pg: dict[str, str]
    # {state: n} for a partitioned table; {"": total} for a flat table.
    row_counts: dict[str, int] = Field(default_factory=dict)
    files: list[UnloadFile] = Field(default_factory=list)


class UnloadManifest(ManifestBase):
    step: Literal["unload"] = "unload"
    tables: list[UnloadTable]


class ProvisionManifest(ManifestBase):
    step: Literal["provision"] = "provision"
    cluster_id: str
    writer_instance_id: str
    writer_endpoint: str
    iam_role_arn: str
    vpc_endpoint_id: str
    load_parameter_group: str
    serve_parameter_group: str
    conn_param: str


class SchemaManifest(ManifestBase):
    step: Literal["schema"] = "schema"
    target_schema_s3_uri: str
    tables_created: list[str]
    column_diff_from_prod: dict[str, list[str]] = Field(default_factory=dict)


class CopyTableResult(BaseModel):
    table: str
    state: str
    expected_rows: int
    actual_rows: int
    files_loaded: int
    seconds_elapsed: float


class CopyManifest(ManifestBase):
    step: Literal["copy"] = "copy"
    results: list[CopyTableResult]


class IndexSpec(BaseModel):
    table: str
    index_name: str
    columns: list[str]
    unique: bool = False
    where: str | None = None


class IndexManifest(ManifestBase):
    step: Literal["indexes"] = "indexes"
    indexes: list[IndexSpec]
    constraints_added: list[str]
    analyzed_tables: list[str]
    # None means the coverage check was skipped (org_districts unreachable);
    # [] means it ran and found full coverage.
    l2type_coverage_missing: list[str] | None = None


class ResizeManifest(ManifestBase):
    step: Literal["resize"] = "resize"
    final_instance_class: str
    min_acu: float
    max_acu: float
    backup_retention_days: int
    deletion_protection: bool


class ValidationCheck(BaseModel):
    name: str
    passed: bool
    details: dict = Field(default_factory=dict)


class ValidateManifest(ManifestBase):
    step: Literal["validate"] = "validate"
    checks: list[ValidationCheck]
    all_passed: bool
