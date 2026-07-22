"""Extract the serving cluster's PK / indexes / FKs from pg_catalog.

These are Postgres-specific (Databricks marts have no notion of them). The records
seed schema_spec via the `extract-serving-structure` CLI; build_indexes consumes the
committed result. Functions take a cursor so they're unit-testable without a live DB.
"""

from __future__ import annotations

import re
from typing import Any

from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey

_PK_COLS_RE = re.compile(r"PRIMARY KEY \((?P<cols>.+)\)")
_WHERE_RE = re.compile(r"\bWHERE\s+(?P<where>.+)$")
_UNIQUE_RE = re.compile(r"^CREATE UNIQUE INDEX", re.IGNORECASE)


def _split_cols(cols: str) -> list[str]:
    return [c.strip().strip('"') for c in cols.split(",")]


def _balanced(text: str, open_pos: int) -> str:
    """Contents of the parenthesised group opening at text[open_pos] (nesting-aware)."""
    depth = 0
    for i in range(open_pos, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[open_pos + 1 : i]
    return text[open_pos + 1 :]


def _split_top_level(cols: str) -> list[str]:
    """Split a column list on commas not nested inside parentheses (functional exprs)."""
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


def _index_columns(definition: str) -> list[str]:
    """Parse the indexed column list from a `pg_get_indexdef` string.

    Anchors on the `USING <method> (...)` group and captures it balanced, so a functional
    expression (`lower("c")`) stays intact and a trailing `WHERE (...)` isn't mistaken for a
    column. build_indexes rebuilds unique-index DDL from these columns (to append the
    partition key), so they must be populated, not left empty.
    """
    m = re.search(r"\bUSING\s+\w+\s*\(", definition, re.IGNORECASE)
    open_pos = (m.end() - 1) if m else definition.find("(")
    if open_pos < 0:
        return []
    raw = _balanced(definition, open_pos)
    return [p.strip().strip('"') for p in _split_top_level(raw) if p.strip()]


def extract_indexes(cur: Any, tables: list[str]) -> list[IndexDef]:
    """All non-PK indexes (standalone and unique-constraint-backing alike); only PK indexes
    are excluded (NOT indisprimary). build_indexes re-issues each as CREATE [UNIQUE] INDEX
    IF NOT EXISTS on the fresh cluster, so a unique-constraint-backing index is fine here."""
    cur.execute(
        """
        SELECT t.relname AS table, c.relname AS index_name, pg_get_indexdef(i.indexrelid) AS def,
               i.indisunique AS is_unique
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = current_schema() AND t.relname = ANY(%s) AND NOT i.indisprimary
        ORDER BY t.relname, c.relname
        """,
        (tables,),
    )
    out: list[IndexDef] = []
    for table, name, definition, is_unique in cur.fetchall():
        where_m = _WHERE_RE.search(definition)
        out.append(
            IndexDef(
                table=table,
                name=name,
                sql=definition if definition.rstrip().endswith(";") else definition + ";",
                unique=bool(is_unique) or bool(_UNIQUE_RE.match(definition)),
                columns=_index_columns(definition),
                where=where_m.group("where").strip() if where_m else None,
            )
        )
    return out


def extract_primary_keys(cur: Any, tables: list[str]) -> list[PrimaryKey]:
    cur.execute(
        """
        SELECT t.relname AS table, con.conname AS constraint,
               pg_get_constraintdef(con.oid) AS def
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY(%s) AND con.contype = 'p'
        ORDER BY t.relname
        """,
        (tables,),
    )
    out: list[PrimaryKey] = []
    for table, constraint, definition in cur.fetchall():
        m = _PK_COLS_RE.search(definition)
        cols = _split_cols(m.group("cols")) if m else []
        out.append(PrimaryKey(table=table, constraint=constraint, columns=cols))
    return out


def extract_foreign_keys(cur: Any, tables: list[str]) -> list[ForeignKey]:
    cur.execute(
        """
        SELECT t.relname AS table, con.conname AS constraint,
               'ALTER TABLE ONLY public.' || quote_ident(t.relname) ||
               ' ADD CONSTRAINT ' || quote_ident(con.conname) || ' ' ||
               pg_get_constraintdef(con.oid) AS def
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY(%s) AND con.contype = 'f'
        ORDER BY t.relname, con.conname
        """,
        (tables,),
    )
    return [
        ForeignKey(table=table, constraint=constraint, sql=definition.rstrip(";") + ";")
        for table, constraint, definition in cur.fetchall()
    ]
