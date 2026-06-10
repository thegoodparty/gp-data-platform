"""Extract index / PK / FK specifications from a prod pg_dump.

pg_dump emits these as stand-alone statements after the CREATE TABLE
blocks:

    ALTER TABLE ONLY public."VoterTX"
        ADD CONSTRAINT "VoterTX_pkey" PRIMARY KEY ("LALVOTERID");

    CREATE INDEX "VoterTX_Voters_LastName_idx"
        ON public."VoterTX" USING btree ("Voters_LastName");

    CREATE INDEX "VoterTX_family_idx"
        ON public."VoterTX" USING btree ("Mailing_Families_FamilyID")
        WHERE ("Mailing_Families_FamilyID" IS NOT NULL);

We parse them into simple records so step 5 can re-issue them in order:
primary keys first, then non-unique indexes (built in parallel), then FKs.
"""

from __future__ import annotations

import re
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


_PK_RE = re.compile(
    r'ALTER TABLE\s+ONLY\s+public\."(?P<table>[^"]+)"\s*'
    r'ADD CONSTRAINT\s+"(?P<constraint>[^"]+)"\s+PRIMARY KEY\s*\((?P<cols>[^)]+)\)\s*;',
    re.IGNORECASE | re.DOTALL,
)

_FK_RE = re.compile(
    r'ALTER TABLE\s+ONLY\s+public\."(?P<table>[^"]+)"\s*'
    r'ADD CONSTRAINT\s+"(?P<constraint>[^"]+)"\s+FOREIGN KEY\s*\([^)]*\)\s*REFERENCES[^;]+;',
    re.IGNORECASE | re.DOTALL,
)

# CREATE [UNIQUE] INDEX "name" ON public."table" ... up to the terminating ';'.
_IDX_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r'"(?P<name>[^"]+)"\s+ON\s+(?:ONLY\s+)?public\."(?P<table>[^"]+)"'
    r"(?P<body>[^;]+);",
    re.IGNORECASE | re.DOTALL,
)

_IDX_COLS_RE = re.compile(r"\(([^)]+)\)")
_IDX_WHERE_RE = re.compile(r"\bWHERE\s+(.+?)(?:;|$)", re.IGNORECASE | re.DOTALL)


def _parse_index_columns(body: str) -> list[str]:
    m = _IDX_COLS_RE.search(body)
    if not m:
        return []
    raw = m.group(1)
    parts = [p.strip().strip('"') for p in raw.split(",")]
    return [p for p in parts if p]


def _parse_where(body: str) -> str | None:
    m = _IDX_WHERE_RE.search(body)
    return m.group(1).strip() if m else None


def parse_primary_keys(sql: str) -> list[PrimaryKey]:
    pks: list[PrimaryKey] = []
    for m in _PK_RE.finditer(sql):
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        pks.append(
            PrimaryKey(
                table=m.group("table"),
                constraint=m.group("constraint"),
                columns=[c for c in cols if c],
            )
        )
    return pks


def parse_indexes(sql: str) -> list[IndexDef]:
    out: list[IndexDef] = []
    for m in _IDX_RE.finditer(sql):
        full_sql = m.group(0).strip()
        out.append(
            IndexDef(
                table=m.group("table"),
                name=m.group("name"),
                sql=full_sql if full_sql.endswith(";") else full_sql + ";",
                unique=bool(m.group("unique")),
                columns=_parse_index_columns(m.group("body")),
                where=_parse_where(m.group("body")),
            )
        )
    return out


def parse_foreign_keys(sql: str) -> list[ForeignKey]:
    out: list[ForeignKey] = []
    for m in _FK_RE.finditer(sql):
        full_sql = m.group(0).strip()
        out.append(
            ForeignKey(
                table=m.group("table"),
                constraint=m.group("constraint"),
                sql=full_sql if full_sql.endswith(";") else full_sql + ";",
            )
        )
    return out
