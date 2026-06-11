"""Extract index / PK / FK specifications from a prod pg_dump.

pg_dump emits these as stand-alone statements after the CREATE TABLE
blocks:

    ALTER TABLE ONLY public."Voter"
        ADD CONSTRAINT "Voter_pkey" PRIMARY KEY ("id", "State");

    CREATE INDEX "Voter_LastName_idx"
        ON public."Voter" USING btree ("Voters_LastName");

    CREATE INDEX "Voter_family_idx"
        ON public."Voter" USING btree ("Mailing_Families_FamilyID")
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

_IDX_USING_RE = re.compile(r"\bUSING\s+\w+\s*\(", re.IGNORECASE)
_IDX_WHERE_RE = re.compile(r"\bWHERE\s+(.+?)(?:;|$)", re.IGNORECASE | re.DOTALL)


def _extract_balanced(text: str, open_pos: int) -> str:
    """Contents of the parenthesised group opening at ``text[open_pos]``.

    Tracks nesting so a functional column like ``lower("c")`` is captured whole
    and a trailing ``WHERE (...)`` predicate is not swallowed.
    """
    depth = 0
    for i in range(open_pos, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[open_pos + 1 : i]
    return text[open_pos + 1 :]  # unbalanced input; best effort


def _split_top_level(cols: str) -> list[str]:
    """Split a column list on commas that are not nested inside parentheses."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, c in enumerate(cols):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(cols[start:i])
            start = i + 1
    parts.append(cols[start:])
    return parts


def _parse_index_columns(body: str) -> list[str]:
    """Best-effort column list from an index body (manifest metadata only).

    Anchors on the ``USING <method> (...)`` clause and captures its full balanced
    paren group, so functional expressions (``lower("c")``) are kept intact and a
    partial-index ``WHERE (...)`` is not mistaken for a column. Falls back to the
    first ``(`` when there is no explicit ``USING``.
    """
    m = _IDX_USING_RE.search(body)
    open_pos = (m.end() - 1) if m else body.find("(")
    if open_pos < 0:
        return []
    raw = _extract_balanced(body, open_pos)
    return [p.strip().strip('"') for p in _split_top_level(raw) if p.strip()]


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
