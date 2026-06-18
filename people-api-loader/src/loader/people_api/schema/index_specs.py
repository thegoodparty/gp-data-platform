"""Index / PK / FK record types for the people-api schema.

These dataclasses are populated by `serving_structure` (from pg_catalog) into the
committed `_serving_seed`, composed by `schema_spec`, and re-issued by `build_indexes`:
primary keys first, then non-unique indexes (built in parallel), then FKs.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PrimaryKey:
    table: str
    constraint: str
    columns: list[str]


@dataclass(frozen=True, slots=True)
class IndexDef:
    table: str
    name: str
    sql: str  # full CREATE INDEX ... statement (prefer re-issuing verbatim)
    unique: bool
    columns: list[str]  # best-effort parsed list (used for manifest only)
    where: str | None


@dataclass(frozen=True, slots=True)
class ForeignKey:
    table: str
    constraint: str
    sql: str  # full ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... statement
