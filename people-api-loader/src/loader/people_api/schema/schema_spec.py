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
    # PK for tables the serving snapshot cannot describe (e.g. DistrictStats is not in the `public`
    # serving replica the seed is extracted from; it lives in the Prisma `green` schema, so
    # _serving_seed has no entry). For serving tables this stays None and the seed is authoritative.
    # NEVER hand-edit _serving_seed.py; this is the escape hatch.
    primary_key: PrimaryKey | None = None
    # Mart column name -> serving column name, for a mart that is NOT the serving shape (e.g. the
    # DistrictVoter mart is a denormalized intermediate with extra `type`/`name` columns and a
    # lowercase `state`, while the Prisma serving table has only 5 columns with a capital "State").
    # When NON-EMPTY it is the AUTHORITATIVE column set: only mart columns whose name is a KEY are
    # kept (others dropped), each rendered/projected under its VALUE (serving name). When empty
    # (Voter/District/DistrictStats — their marts already match serving) columns pass through
    # unchanged. `type_overrides` keys are always MART column names.
    mart_column_map: dict[str, str] = field(default_factory=dict)


# The loader bulk-loads the full serving set onto the fresh cluster: Voter and DistrictVoter are
# LIST-partitioned by the serving "State" column (large, per-partition parallel index builds);
# District and DistrictStats are flat. Columns/types come from the marts (mart_introspect); this
# module owns the Postgres-side decisions. Most marts already match the Prisma serving shape, but
# the DistrictVoter mart is a denormalized intermediate (extra `type`/`name` columns, lowercase
# `state`) so its `mart_column_map` projects it to the 5-column Prisma serving shape and renames
# `state` -> "State" (matching the Prisma `@map("State")`); its serving partition column is "State"
# like Voter. DistrictStats is not in the `public` serving replica (it lives in the Prisma `green`
# schema), so its PK is carried here, not in the generated _serving_seed. Serving enforces no FKs,
# so none are created. Voter's single Prisma-only
# column the mart omits is Mailing_HHGender_Description; `id` is the mart's salted-uuid string
# stored as UUID.
TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(
        pg_table="Voter",
        partition_by="State",
        # State: the serving public."USState" enum (matches the serving cluster); the mart emits it as text
        # and Postgres coerces text -> enum on COPY.
        type_overrides={"id": "UUID", "State": '"USState"'},
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    ),
    "District": TableSpec(
        pg_table="District",
        partition_by=None,
        # id is the salted-uuid string in the mart; Prisma types it @db.Uuid, so store UUID.
        # state: stays TEXT (no override), unlike Voter/DistrictVoter. District includes one
        # country-scope row (type=Country, state="US") that the 51-value USState enum (50 states +
        # DC) cannot hold, so this column can't be coerced to the enum on COPY like the others.
        type_overrides={"id": "UUID"},
    ),
    "DistrictStats": TableSpec(
        pg_table="DistrictStats",
        partition_by=None,
        # buckets: mart struct -> jsonb. total_constituents columns: the mart emits them as bigint,
        # but Prisma types them Int, so store INTEGER to match the serving contract.
        type_overrides={
            "buckets": "jsonb",
            "total_constituents": "INTEGER",
            "total_constituents_with_cell_phone": "INTEGER",
        },
        primary_key=PrimaryKey(
            table="DistrictStats", constraint="DistrictStats_pkey", columns=["district_id"]
        ),
    ),
    # The DistrictVoter mart (voter_id, district_id, type, name, state, created_at, updated_at, all
    # strings) is NOT the serving shape. Prisma serving is (district_id, voter_id, created_at,
    # updated_at, "State") with UUID ids + a capital "State". mart_column_map selects the 5 serving
    # columns (dropping type/name) and renames mart `state` -> "State"; type_overrides (keyed by
    # MART name) set the UUID/timestamp/text types. Partition by the serving "State", like Voter.
    "DistrictVoter": TableSpec(
        pg_table="DistrictVoter",
        partition_by="State",
        type_overrides={
            "district_id": "UUID",
            "voter_id": "UUID",
            "created_at": "TIMESTAMPTZ",
            "updated_at": "TIMESTAMPTZ",
            # the serving public."USState" enum, matching Voter/District.
            "state": '"USState"',
        },
        mart_column_map={
            "district_id": "district_id",
            "voter_id": "voter_id",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "state": "State",
        },
    ),
}


# Serving-cluster columns absent from the current prod (swain) baseline — intended divergences that
# validate's schema-diff must not read as drift, like the partition column. Two kinds land here:
# loader-created-post-load columns whose DDL build_indexes owns ("geom"), and newly-projected mart
# columns the serving schema gained while the prod baseline still predates them
# ("hf_most_important_policy_item"). Keyed by serving table.
LOADER_ADDED_COLUMNS: dict[str, set[str]] = {"Voter": {"geom", "hf_most_important_policy_item"}}


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
    # Hand-added entries live in _serving_seed_extra so wholesale seed regeneration can't drop
    # them; they merge here, and a generated entry wins any name collision.
    generated = [i for i in seed.INDEXES if i.table == table]
    names = {i.name for i in generated}
    extras = [i for i in seed_extra.EXTRA_INDEXES if i.table == table and i.name not in names]
    return generated + extras


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
