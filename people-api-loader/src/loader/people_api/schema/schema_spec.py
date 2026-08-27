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
        # Everything below is a serving-contract (Prisma) type the voter mart does NOT match: the mart
        # types the election-participation flags as boolean, the ZIP/DPBC/ethnic/sequence codes as int,
        # and lat/long + voting-performance scores as double, but Prisma types all of them String. The
        # app reads through Prisma, so each serving column must be TEXT (the loader is the source of
        # truth for the serving schema; the schema-type validate check enforces this against prod).
        # NOTE: forcing TEXT fixes the column TYPE; it cannot recover precision the mart already dropped
        # (e.g. a ZipPlus4's leading zeros) — that needs the mart to emit these columns as strings.
        type_overrides={
            "id": "UUID",
            "State": '"USState"',
            # election-participation flags (mart boolean -> contract text)
            "AnyElection_2017": "TEXT",
            "AnyElection_2019": "TEXT",
            "AnyElection_2021": "TEXT",
            "AnyElection_2023": "TEXT",
            "AnyElection_2025": "TEXT",
            "General_2016": "TEXT",
            "General_2018": "TEXT",
            "General_2020": "TEXT",
            "General_2022": "TEXT",
            "General_2024": "TEXT",
            "General_2026": "TEXT",
            "Primary_2016": "TEXT",
            "Primary_2018": "TEXT",
            "Primary_2020": "TEXT",
            "Primary_2022": "TEXT",
            "Primary_2024": "TEXT",
            "Primary_2026": "TEXT",
            "PresidentialPrimary_2016": "TEXT",
            "PresidentialPrimary_2020": "TEXT",
            "PresidentialPrimary_2024": "TEXT",
            "OtherElection_2016": "TEXT",
            "OtherElection_2018": "TEXT",
            "OtherElection_2020": "TEXT",
            "OtherElection_2022": "TEXT",
            "OtherElection_2024": "TEXT",
            "OtherElection_2026": "TEXT",
            # ZIP / delivery-point / ethnic / sequence codes (mart integer -> contract text)
            "Residence_Addresses_ZipPlus4": "TEXT",
            "Residence_Addresses_DPBC": "TEXT",
            "Mailing_Addresses_Zip": "TEXT",
            "Mailing_Addresses_ZipPlus4": "TEXT",
            "Mailing_Addresses_DPBC": "TEXT",
            "CountyEthnic_LALEthnicCode": "TEXT",
            "SequenceOddEven": "TEXT",
            "SequenceZigZag": "TEXT",
            # 3-digit code (001-840) — text so the leading zero survives (the mart keeps it raw too).
            "FIPS": "TEXT",
            # geo coordinates + voting-performance scores (mart double precision -> contract text)
            "Residence_Addresses_Latitude": "TEXT",
            "Residence_Addresses_Longitude": "TEXT",
            "VotingPerformanceEvenYearGeneral": "TEXT",
            "VotingPerformanceEvenYearGeneralAndPrimary": "TEXT",
            "VotingPerformanceEvenYearPrimary": "TEXT",
            "VotingPerformanceMinorElection": "TEXT",
            # created_at/updated_at/Voter_Status_UpdatedAt: mart emits timestamptz, but the Voter
            # serving contract is timestamp WITHOUT time zone (like District/DistrictStats) — match it.
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
            "Voter_Status_UpdatedAt": "TIMESTAMP",
        },
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    ),
    "District": TableSpec(
        pg_table="District",
        partition_by=None,
        # id is the salted-uuid string in the mart; Prisma types it @db.Uuid, so store UUID.
        # created_at/updated_at: the mart emits timestamptz, but the District serving contract is
        # timestamp WITHOUT time zone (as for Voter/DistrictVoter/DistrictStats) — match the contract.
        # state: the serving public."USState" enum, matching prod. The one country-scope row
        # (type=Country, state="US") that the 51-value enum can't hold is dropped in the
        # m_people_api__district mart (prod never carried it; nothing references it), so every
        # District.state value lands within the enum and no Prisma enum change is needed.
        type_overrides={
            "id": "UUID",
            "state": '"USState"',
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    ),
    "DistrictStats": TableSpec(
        pg_table="DistrictStats",
        partition_by=None,
        # buckets: mart struct -> jsonb. total_constituents columns: the mart emits them as bigint,
        # but Prisma types them Int, so store INTEGER to match the serving contract. district_id: mart
        # text -> contract uuid. updated_at: mart timestamptz -> contract timestamp WITHOUT time zone.
        type_overrides={
            "buckets": "jsonb",
            "total_constituents": "INTEGER",
            "total_constituents_with_cell_phone": "INTEGER",
            "district_id": "UUID",
            "updated_at": "TIMESTAMP",
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
            # timestamp WITHOUT time zone, matching the prod contract (same as District/DistrictStats;
            # the mart emits timestamptz).
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
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
    # Voter-density heat map serving tables (voter-density-heatmap-handoff.md §7). Both are flat
    # (the app queries by district_id + resolution, never by state, so no LIST partitioning) and, like
    # DistrictStats, are NOT in the `public` serving replica the seed is extracted from — they live in
    # the Prisma `green` schema — so each carries its PK here on the spec (the seed has no entry) and
    # its non-PK index in _serving_seed_extra. The marts emit a lowercase `state`, so mart_column_map
    # renames it to the serving "State" (the green."USState" enum) exactly like DistrictVoter; the
    # count/sum aggregates are bigint in the mart but the Prisma contract is Int, so they are overridden
    # to INTEGER (as DistrictStats does for total_constituents). district_id is the mart's salted-uuid
    # string stored as UUID; updated_at is mart timestamptz -> contract timestamp WITHOUT time zone.
    "DistrictVoterDensity": TableSpec(
        pg_table="DistrictVoterDensity",
        partition_by=None,
        type_overrides={
            "district_id": "UUID",
            "voter_count": "INTEGER",
            "state": '"USState"',
            "updated_at": "TIMESTAMP",
        },
        mart_column_map={
            "district_id": "district_id",
            "resolution": "resolution",
            "h3_index": "h3_index",
            "lat": "lat",
            "lng": "lng",
            "voter_count": "voter_count",
            "state": "State",
            "updated_at": "updated_at",
        },
        primary_key=PrimaryKey(
            table="DistrictVoterDensity",
            constraint="DistrictVoterDensity_pkey",
            columns=["district_id", "resolution", "h3_index"],
        ),
    ),
    "DistrictVoterDensityMeta": TableSpec(
        pg_table="DistrictVoterDensityMeta",
        partition_by=None,
        type_overrides={
            "district_id": "UUID",
            "total_voters": "INTEGER",
            "geocoded_voters": "INTEGER",
            "rendered_voters": "INTEGER",
            "suppressed_cells": "INTEGER",
            "min_cell_count": "INTEGER",
            "state": '"USState"',
            "updated_at": "TIMESTAMP",
        },
        mart_column_map={
            "district_id": "district_id",
            "resolution": "resolution",
            "coverage": "coverage",
            "min_cell_count": "min_cell_count",
            "total_voters": "total_voters",
            "geocoded_voters": "geocoded_voters",
            "rendered_voters": "rendered_voters",
            "suppressed_cells": "suppressed_cells",
            "state": "State",
            "updated_at": "updated_at",
        },
        primary_key=PrimaryKey(
            table="DistrictVoterDensityMeta",
            constraint="DistrictVoterDensityMeta_pkey",
            columns=["district_id", "resolution"],
        ),
    ),
}


# Serving-cluster columns absent from the current prod (swain) baseline — intended divergences that
# validate's schema-diff must not read as drift, like the partition column. Two kinds land here:
# loader-created-post-load columns whose DDL build_indexes owns ("geom"), and newly-projected mart
# columns the serving schema gained while the prod baseline still predates them
# ("hf_most_important_policy_item"). Keyed by serving table.
LOADER_ADDED_COLUMNS: dict[str, set[str]] = {
    "Voter": {
        "geom",
        "hf_most_important_policy_item",
        "Voter_Turnout_Probability",
        "4H_Livestock_District",
        "Community_College",
        "Judicial_Chancery_Court",
        "Judicial_Justice_of_the_Peace",
        "Soil_and_Water_District",
        "Soil_and_Water_District_At_Large",
        "State_Board_of_Equalization",
        # Adopted 2026 maps, minted as their own district types. Present so
        # m_people_api__districtvoter emits links for them: it derives the columns it
        # unpivots by intersecting voter columns with district types.
        "Congressional_District_2026",
        "State_Senate_District_2026",
    }
}


# Columns whose serving TYPE intentionally differs from the prod contract, so the validate
# schema-type guardrail must not flag them as drift (the type analogue of LOADER_ADDED_COLUMNS).
# Keyed by serving table -> column names.
# Voter.State: served as the public."USState" enum (matching District/DistrictVoter and swain-db, so
# the app needs no code change — the 2026-07-21 decision), while the current prod baseline still
# stores Voter."State" as text. This is an intended forward migration, not drift.
# The four *_Addresses_*Direction columns: prod stores them INTEGER, which is why they are empty
# there. The values are N/S/E/W and no integer column can hold one. The mart now emits them as
# strings and this cluster serves them TEXT. Also a forward migration; drop these once a prod
# baseline built from this loader replaces the INTEGER one.
ACCEPTED_TYPE_DIVERGENCES: dict[str, set[str]] = {
    "Voter": {
        "State",
        "Mailing_Addresses_PrefixDirection",
        "Mailing_Addresses_SuffixDirection",
        "Residence_Addresses_PrefixDirection",
        "Residence_Addresses_SuffixDirection",
    }
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
    # Hand-added entries live in _serving_seed_extra so wholesale seed regeneration can't drop
    # them; they merge here, and a generated entry wins any name collision.
    generated = [i for i in seed.INDEXES if i.table == table]
    names = {i.name for i in generated}
    extras = [i for i in seed_extra.EXTRA_INDEXES if i.table == table and i.name not in names]
    return generated + extras


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
