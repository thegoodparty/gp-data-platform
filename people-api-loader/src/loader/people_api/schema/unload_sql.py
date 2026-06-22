"""Pure SQL builders for the unload step (no I/O — unit-tested in isolation).

The unload projects the mart onto the `target_schema.sql` column order so file column order
matches the `copy` step's column list exactly. Columns the mart lacks (declared Prisma-layer
extras, e.g. the Mailing_HHGender_Description NULL placeholder) are emitted as NULL.
"""

from __future__ import annotations

# Spark CSV OPTIONS, pinned to mirror copy's PG `FORMAT csv` import (tab-delimited CSV, empty
# string = NULL, double-quote for both quote and escape, no header row). The NULL-vs-empty-string
# round-trip is load-bearing and relies on Spark's write defaults: nullValue='' writes NULL as an
# unquoted empty field while an actual "" empty string is written quoted (default emptyValue '""'),
# and PG `FORMAT csv, NULL ''` reads unquoted-empty as NULL but quoted-empty as ''. Embedded
# tab/newline/quote round-trip because the field is quoted and PG csv supports quoted multi-line
# values; embedded quotes are doubled ("") on both sides. test_format_contract.py pins this against
# copy's import options so a one-sided edit fails CI.
_CSV_OPTIONS = "'sep' = '\\t', 'header' = 'false', 'nullValue' = '', 'quote' = '\"', 'escape' = '\"'"


def select_exprs(ddl_columns: list[str], extra_columns: set[str]) -> list[str]:
    """Backtick-quoted SELECT expressions in DDL order; NULL AS for Prisma-only extras."""
    out: list[str] = []
    for col in ddl_columns:
        if col in extra_columns:
            out.append(f"NULL AS `{col}`")
        else:
            out.append(f"`{col}`")
    return out


def unload_statement(*, mart_fqn: str, select_exprs: list[str], state: str, s3_dir: str) -> str:
    cols = ", ".join(select_exprs)
    return (
        f"INSERT OVERWRITE DIRECTORY '{s3_dir}'\n"
        f"USING csv OPTIONS ({_CSV_OPTIONS})\n"
        f"SELECT {cols}\n"
        f"FROM {mart_fqn}\n"
        f"WHERE `State` = '{state}'"
    )


def count_by_state_statement(mart_fqn: str) -> str:
    return f"SELECT `State` AS state, count(*) AS n FROM {mart_fqn} GROUP BY `State`"
