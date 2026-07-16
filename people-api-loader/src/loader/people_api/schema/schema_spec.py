"""Declarative target-schema spec: the serving structure the marts cannot describe.

Columns/types come from the marts (mart_introspect); this module owns the Postgres-side
decisions — partitioning, per-column type overrides, and (via the generated _serving_seed)
the PK/indexes/FKs. build_indexes and emit_ddl read from here."""

from __future__ import annotations

from dataclasses import dataclass, field

from loader.people_api.schema import _serving_seed as seed
from loader.people_api.schema import _serving_seed_extra as seed_extra
from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey


@dataclass(frozen=True, slots=True)
class TableSpec:
    pg_table: str
    partition_by: str | None  # column name for LIST partitioning, or None for a plain table
    type_overrides: dict[str, str] = field(default_factory=dict)
    # App/Prisma-managed columns that exist in the serving table but not the mart, appended
    # as (name, pg_type, nullable). The mart is the source for everything else.
    extra_columns: list[tuple[str, str, bool]] = field(default_factory=list)


# Scope: the loader bulk-loads only Voter. District/DistrictVoter are small, Prisma-defined,
# and built by the dbt write path (write__people_api_db.py); DistrictStats isn't a serving
# Postgres table at all. So generation is Voter-only. The single Prisma-layer column the mart
# omits is Mailing_HHGender_Description (a REMOVED_COLUMNS NULL placeholder);
# `id` is the mart's salted-uuid string, stored as UUID in Postgres.
TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(
        pg_table="Voter",
        partition_by="State",
        type_overrides={"id": "UUID"},
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    ),
}


def primary_key_for(table: str) -> PrimaryKey | None:
    matches = [p for p in seed.PRIMARY_KEYS if p.table == table]
    return matches[0] if matches else None


def indexes_for(table: str) -> list[IndexDef]:
    # extract-serving-structure regenerates the seed wholesale, so hand-added
    # entries live in _serving_seed_extra and merge here — regeneration cannot
    # silently drop them. If a future extraction starts returning one of them
    # (same name), the generated entry wins.
    generated = [i for i in seed.INDEXES if i.table == table]
    names = {i.name for i in generated}
    extras = [i for i in seed_extra.EXTRA_INDEXES if i.table == table and i.name not in names]
    return generated + extras


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
