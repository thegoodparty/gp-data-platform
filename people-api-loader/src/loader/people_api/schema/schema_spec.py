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


TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(pg_table="Voter", partition_by="State", type_overrides={"id": "UUID"}),
    "District": TableSpec(pg_table="District", partition_by=None, type_overrides={"id": "UUID"}),
    "DistrictStats": TableSpec(pg_table="DistrictStats", partition_by=None),
    "DistrictVoter": TableSpec(
        pg_table="DistrictVoter",
        partition_by=None,
        type_overrides={"voter_id": "UUID", "district_id": "UUID"},
    ),
}


def primary_key_for(table: str) -> PrimaryKey | None:
    matches = [p for p in seed.PRIMARY_KEYS if p.table == table]
    return matches[0] if matches else None


def indexes_for(table: str) -> list[IndexDef]:
    return [i for i in seed.INDEXES if i.table == table]


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
