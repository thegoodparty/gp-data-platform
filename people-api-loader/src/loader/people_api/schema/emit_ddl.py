"""Emit the target schema DDL for the new voter cluster.

Inputs:
- `prod_dump.sql` — `pg_dump --schema-only` output from the current prod
  cluster (captured by step 0). The floor: every column the app touches
  today is preserved, with its prod-verified PG type.
- `databricks_columns.json` — output of `databricks tables get ...` for
  `int__l2_nationwide_uniform`, captured in step 0. Used to hint the PG
  type for NEW columns we're adding.
- `VOTER_TARGET_COLUMNS` (loader.people_api.schema.voter_columns) — the curated
  column list. Defines ordering and which columns the new schema has.

Output: a `target_schema.sql` file with 51 `CREATE TABLE public."Voter{ST}"`
statements plus `public."VoterFile"` (the tracking table carried over
unchanged from prod).

**No indexes, no PKs, no FKs** — those come in step 5.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from loader.people_api.schema.voter_columns import (
    LEGACY_RENAMES,
    VOTER_TARGET_COLUMNS,
    TargetColumn,
)

# 50 states + DC. Source of truth: PLAN_LOADER.md § "Data Volume".
STATES: tuple[str, ...] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)


@dataclass(frozen=True, slots=True)
class ProdTableDef:
    name: str
    columns: list[tuple[str, str]]  # (col_name, full_type_def) in declaration order


# Matches one `CREATE TABLE public."X" (...);` block (multiline, non-greedy).
_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:(?:public|"public")\.)?"(?P<name>[^"]+)"\s*' r"\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)

# One column definition line inside a CREATE TABLE body. pg_dump prints each
# column on its own line, indented, with a trailing comma except for the
# final one. Constraints (CONSTRAINT, PRIMARY KEY, ...) start unquoted so we
# can detect and skip them.
_COLUMN_LINE_RE = re.compile(r'^\s*"(?P<col>[^"]+)"\s+(?P<typedef>[^,]+?)\s*,?\s*$')


def parse_prod_dump(sql: str) -> dict[str, ProdTableDef]:
    """Return {table_name: ProdTableDef} from a pg_dump --schema-only file."""
    tables: dict[str, ProdTableDef] = {}
    for match in _CREATE_TABLE_RE.finditer(sql):
        name = match.group("name")
        body = match.group("body")
        columns: list[tuple[str, str]] = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            # Skip table-level constraints emitted inside the CREATE TABLE.
            if line.startswith(("CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "EXCLUDE")):
                continue
            m = _COLUMN_LINE_RE.match(line)
            if not m:
                continue
            col = m.group("col")
            typedef = m.group("typedef").strip().rstrip(",")
            columns.append((col, typedef))
        tables[name] = ProdTableDef(name=name, columns=columns)
    return tables


def _databricks_type_to_pg(databricks_type: str, fallback: str) -> str:
    """Map a Databricks column type to a PG column type.

    Conservative: unknown types fall back to the hint from
    `VOTER_TARGET_COLUMNS` (typically TEXT). Only applies to *new* columns
    — existing columns keep their prod-verified type.
    """
    t = (databricks_type or "").strip().lower()
    if t in ("int", "integer", "bigint", "long", "smallint", "tinyint"):
        return "INTEGER"
    if t in ("boolean", "bool"):
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t in ("timestamp", "timestamp_ntz", "timestamp_ltz"):
        return "TIMESTAMP"
    if t in ("double", "float", "real"):
        return "DOUBLE PRECISION"
    if t.startswith("decimal") or t.startswith("numeric"):
        return "NUMERIC"
    if t.startswith(("string", "varchar", "char", "text")):
        return "TEXT"
    return fallback


def _pg_type_for_target(
    target: TargetColumn,
    prod_type: str | None,
    databricks_cols: dict[str, str],
) -> str:
    # Prod's authoritative type wins for columns that exist in prod — avoids
    # drifting types out from under the app.
    if prod_type:
        return prod_type

    # New column: prefer a Databricks-informed type.
    source_name = target.source or target.name
    databricks_type = databricks_cols.get(source_name)
    if databricks_type:
        return _databricks_type_to_pg(databricks_type, fallback=target.pg_type)
    return target.pg_type


def _resolve_prod_type(prod_cols: dict[str, str], target_name: str) -> str | None:
    """Look up a target column in prod, accounting for the rename table.

    If the target is one of the three corrected-spelling names, its prod
    equivalent is the typo'd legacy name. That column's prod type still
    applies — the rename is purely cosmetic on the column name.
    """
    if target_name in prod_cols:
        return prod_cols[target_name]
    # Reverse lookup: if target_name is a rename target, check under the
    # legacy typo'd name.
    for typo, corrected in LEGACY_RENAMES.items():
        if corrected == target_name and typo in prod_cols:
            return prod_cols[typo]
    return None


def emit_create_table(
    table: str,
    columns: Iterable[TargetColumn],
    prod_columns: dict[str, str],
    databricks_cols: dict[str, str],
) -> str:
    lines: list[str] = [f'CREATE TABLE public."{table}" (']
    col_lines: list[str] = []
    for target in columns:
        prod_type = _resolve_prod_type(prod_columns, target.name)
        pg_type = _pg_type_for_target(target, prod_type, databricks_cols)
        col_lines.append(f'    "{target.name}" {pg_type}')
    lines.append(",\n".join(col_lines))
    lines.append(");")
    return "\n".join(lines)


def emit_voter_file_table(prod_tables: dict[str, ProdTableDef]) -> str:
    """Carry the `VoterFile` tracking table over unchanged.

    The legacy loader writes rows here after each state finishes. Keeping
    the contract means a legacy re-run would still be recognized.
    """
    vf = prod_tables.get("VoterFile")
    if vf is None:
        # Fallback: use the contract documented in the plan.
        return (
            'CREATE TABLE public."VoterFile" (\n'
            '    "Filename" TEXT,\n'
            '    "State" TEXT,\n'
            '    "Lines" INTEGER,\n'
            '    "Loaded" TIMESTAMP,\n'
            '    "updatedAt" TIMESTAMP\n'
            ");"
        )
    lines = ['CREATE TABLE public."VoterFile" (']
    lines.append(",\n".join(f'    "{c}" {t}' for c, t in vf.columns))
    lines.append(");")
    return "\n".join(lines)


def emit_target_schema(
    prod_dump_sql: str,
    databricks_columns: dict[str, str],
    *,
    states: Iterable[str] = STATES,
) -> str:
    prod_tables = parse_prod_dump(prod_dump_sql)
    out: list[str] = [
        "-- Auto-generated by loader.people_api.schema.emit_ddl.",
        "-- Source of truth: loader.people_api.schema.voter_columns.VOTER_TARGET_COLUMNS.",
        "-- Existing-column types preserved from prod pg_dump; new-column",
        "-- types inferred from the Databricks source schema.",
        "-- NO indexes, PKs, or FKs here — step 5 adds those after COPY.",
        "",
        "BEGIN;",
        "",
    ]
    for state in states:
        table = f"Voter{state}"
        prod_def = prod_tables.get(table)
        prod_cols = dict(prod_def.columns) if prod_def else {}
        out.append(emit_create_table(table, VOTER_TARGET_COLUMNS, prod_cols, databricks_columns))
        out.append("")
    out.append(emit_voter_file_table(prod_tables))
    out.append("")
    out.append("COMMIT;")
    out.append("")
    return "\n".join(out)


def run(
    prod_dump_path: str | Path,
    databricks_columns_path: str | Path,
    out_path: str | Path,
) -> Path:
    prod_dump = Path(prod_dump_path).read_text(encoding="utf-8")
    databricks_cols = json.loads(Path(databricks_columns_path).read_text(encoding="utf-8"))
    if isinstance(databricks_cols, list):
        # Accept `[{"name": "x", "type": "string"}, ...]` or
        # `{"x": "string", ...}` forms.
        databricks_cols = {c["name"]: c.get("type", "") for c in databricks_cols}

    ddl = emit_target_schema(prod_dump, databricks_cols)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(ddl, encoding="utf-8")
    return out


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Emit the target voter-DB DDL.")
    p.add_argument("--prod-dump", required=True)
    p.add_argument("--databricks-columns", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    path = run(args.prod_dump, args.databricks_columns, args.out)
    print(f"wrote {path}")
