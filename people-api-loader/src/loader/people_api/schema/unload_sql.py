"""Pure SQL builders for the unload step (no I/O — unit-tested in isolation).

The unload projects the mart onto the `target_schema.sql` column order so file column order
matches the `copy` step's column list exactly. Columns the mart lacks (declared Prisma-layer
extras, e.g. the Mailing_HHGender_Description NULL placeholder) are emitted as a typed NULL,
`CAST(NULL AS STRING)` — a bare `NULL` is Spark's VOID type, which the CSV writer rejects
(UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE). STRING is safe: the value is always NULL, and copy's PG
`FORMAT csv, NULL ''` import reads the empty field as NULL into the column's real (typed) DDL.
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


def select_exprs(
    ddl_columns: list[str],
    extra_columns: set[str],
    transforms: dict[str, str] | None = None,
) -> list[str]:
    """Backtick-quoted SELECT expressions in DDL order.

    `transforms` maps a column name to a full SQL expression that REPLACES the plain
    `` `col` `` projection (the expression must alias itself `` AS `col` ``); used for the
    DistrictStats buckets struct-field rename. Prisma-only extras (columns the mart lacks) are
    emitted as `CAST(NULL AS STRING)`, not a bare `NULL`: bare NULL is Spark's VOID type and the
    CSV writer rejects it. The value is always NULL, so STRING is a safe placeholder for the CSV
    round-trip into the real (typed) target column.
    """
    transforms = transforms or {}
    out: list[str] = []
    for col in ddl_columns:
        if col in transforms:
            out.append(transforms[col])
        elif col in extra_columns:
            out.append(f"CAST(NULL AS STRING) AS `{col}`")
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


def unload_statement_flat(*, mart_fqn: str, select_exprs: list[str], s3_dir: str) -> str:
    """Unload a flat (non-partitioned) table's whole mart in one file set — no `WHERE State`."""
    cols = ", ".join(select_exprs)
    return (
        f"INSERT OVERWRITE DIRECTORY '{s3_dir}'\n"
        f"USING csv OPTIONS ({_CSV_OPTIONS})\n"
        f"SELECT {cols}\n"
        f"FROM {mart_fqn}"
    )


def count_by_state_statement(mart_fqn: str) -> str:
    return f"SELECT `State` AS state, count(*) AS n FROM {mart_fqn} GROUP BY `State`"


def count_all_statement(mart_fqn: str) -> str:
    return f"SELECT count(*) AS n FROM {mart_fqn}"


# DistrictStats buckets: the active SQL mart (m_people_api__districtstats.sql) emits a `buckets`
# struct with fields `age`, `homeowner`, `education`, `presenceofchildren`, `estimatedincomerange`
# (all lowercased, no underscores). The app expects two of those keys in camelCase. Rebuild the
# struct with the two renamed and to_json it for the jsonb serving column. This mirrors the legacy
# DAG's _BUCKET_KEY_MAP and is deleted once the camelCase `_py` districtstats mart is enabled.
BUCKETS_TO_JSON_EXPR = (
    "to_json(named_struct("
    "'age', `buckets`.`age`, "
    "'homeowner', `buckets`.`homeowner`, "
    "'education', `buckets`.`education`, "
    "'presenceOfChildren', `buckets`.`presenceofchildren`, "
    "'estimatedIncomeRange', `buckets`.`estimatedincomerange`"
    ")) AS `buckets`"
)
