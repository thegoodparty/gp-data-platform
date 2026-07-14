"""Declarative target-schema spec: the serving structure the marts cannot describe.

Columns/types come from the marts (mart_introspect); this module owns the Postgres-side
decisions — partitioning, per-column type overrides, and (via the generated _serving_seed)
the PK/indexes/FKs. build_indexes and emit_ddl read from here."""

from __future__ import annotations

from dataclasses import dataclass, field

from loader.people_api.schema import _serving_seed as seed
from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey


@dataclass(frozen=True, slots=True)
class TableSpec:
    pg_table: str
    partition_by: str | None  # column name for LIST partitioning, or None for a plain table
    type_overrides: dict[str, str] = field(default_factory=dict)
    # App/Prisma-managed columns that exist in the serving table but not the mart, appended
    # as (name, pg_type, nullable). The mart is the source for everything else.
    extra_columns: list[tuple[str, str, bool]] = field(default_factory=list)
    # PK for tables the serving snapshot cannot describe (e.g. DistrictStats is not yet a serving
    # Postgres table, so _serving_seed has no entry). For serving tables this stays None and the
    # seed is authoritative. NEVER hand-edit _serving_seed.py; this is the escape hatch.
    primary_key: PrimaryKey | None = None


# The loader bulk-loads the full serving set onto the fresh cluster: Voter and DistrictVoter are
# LIST-partitioned (large, per-partition parallel index builds); District and DistrictStats are
# flat. The partition column is per-table: only the Voter mart emits capital "State"; the District
# family (DistrictVoter here) uses lowercase "state" (its mart's column and the serving/app
# convention), so a hardcoded "State" would break DistrictVoter's partition DDL. Columns/types come
# from the marts (mart_introspect); this module owns the
# Postgres-side decisions. DistrictStats is not yet a serving table, so its PK is carried here, not
# in the generated _serving_seed. Serving enforces no FKs, so none are created. Voter's single
# Prisma-only column the mart omits is Mailing_HHGender_Description; `id` is the mart's salted-uuid
# string stored as UUID.
TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(
        pg_table="Voter",
        partition_by="State",
        type_overrides={"id": "UUID"},
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    ),
    "District": TableSpec(
        pg_table="District",
        partition_by=None,
    ),
    "DistrictStats": TableSpec(
        pg_table="DistrictStats",
        partition_by=None,
        type_overrides={"buckets": "jsonb"},
        primary_key=PrimaryKey(
            table="DistrictStats", constraint="DistrictStats_pkey", columns=["district_id"]
        ),
    ),
    "DistrictVoter": TableSpec(
        pg_table="DistrictVoter",
        partition_by="state",  # lowercase: the DistrictVoter mart emits "state", not Voter's "State"
    ),
}


def is_partitioned(table: str) -> bool:
    return TABLE_SPECS[table].partition_by is not None


def partition_column(table: str) -> str | None:
    return TABLE_SPECS[table].partition_by


def primary_key_for(table: str) -> PrimaryKey | None:
    matches = [p for p in seed.PRIMARY_KEYS if p.table == table]
    if matches:
        return matches[0]
    return TABLE_SPECS[table].primary_key if table in TABLE_SPECS else None


def indexes_for(table: str) -> list[IndexDef]:
    return [i for i in seed.INDEXES if i.table == table]


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
