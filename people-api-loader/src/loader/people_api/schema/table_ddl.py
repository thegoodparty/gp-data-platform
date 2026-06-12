"""Extract CREATE TABLE statements from a pg_dump --schema-only file.

`create-schema` applies tables only; pg_dump emits indexes/PKs as separate
statements (CREATE INDEX / ALTER TABLE ADD CONSTRAINT) which are intentionally
ignored here and applied later by `build-indexes`.
"""

from __future__ import annotations

import re

# Matches one `CREATE TABLE public."X" ( ... );` block, body non-greedy up to
# the closing `)` + `;`. Safe for pg_dump --schema-only output: the table's
# closing `);` sits on its own line, and interior column lines (e.g.
# `timestamp(3) without time zone`) never end a line with `);`, so no interior
# paren terminates the match. This relies on that pg_dump formatting, not on a
# universal SQL invariant.
_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:(?:public|"public")\.)?"(?P<name>[^"]+)"\s*' r"\(.*?\)\s*;",
    re.IGNORECASE | re.DOTALL,
)


def extract_create_tables(sql: str) -> dict[str, str]:
    """Return {table_name: full CREATE TABLE statement} parsed from a pg_dump."""
    out: dict[str, str] = {}
    for m in _CREATE_TABLE_RE.finditer(sql):
        out[m.group("name")] = m.group(0)
    return out


# A column line starts with either a "Quoted" identifier or a bare lowercase one
# (Prisma columns like `id`, `created_at`). Table-level constraint lines start with
# an uppercase keyword (PRIMARY, CONSTRAINT, ...) and are skipped by the
# lowercase-only bare branch.
_COLUMN_LINE_RE = re.compile(r'^\s*(?:"(?P<quoted>[^"]+)"|(?P<bare>[a-z_][a-zA-Z0-9_]*))\s')


def extract_column_names(create_table_sql: str) -> list[str]:
    """Ordered column names from a single CREATE TABLE block.

    The order is the table's physical (DDL) column order, which is exactly what a
    positional COPY maps against — so callers can turn it into an explicit column
    list instead of relying on raw positional alignment.
    """
    open_idx = create_table_sql.find("(")
    close_idx = create_table_sql.rfind(")")
    if open_idx < 0 or close_idx <= open_idx:
        return []
    body = create_table_sql[open_idx + 1 : close_idx]
    cols: list[str] = []
    for line in body.splitlines():
        m = _COLUMN_LINE_RE.match(line)
        if m:
            cols.append(m.group("quoted") or m.group("bare"))
    return cols
