"""Extract CREATE TABLE statements from a pg_dump --schema-only file.

`create-schema` applies tables only; pg_dump emits indexes/PKs as separate
statements (CREATE INDEX / ALTER TABLE ADD CONSTRAINT) which are intentionally
ignored here and applied later by `build-indexes`.
"""

from __future__ import annotations

import re

# Matches one `CREATE TABLE public."X" ( ... );` block. The body is non-greedy
# up to the first `)` that begins the closing line; a `)` inside the body (e.g.
# `timestamp(3)`) is never followed by `;` so it cannot terminate the match.
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
