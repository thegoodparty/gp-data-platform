"""Read a dbt mart's column schema from Databricks Unity Catalog.

Uses the UC metadata API (no SQL warehouse): WorkspaceClient().tables.get(fqn).
WorkspaceClient() picks up auth from the standard databricks env/config
(DATABRICKS_HOST + token, or a profile)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MartColumn:
    name: str
    spark_type: str  # UC type_text, e.g. "string", "int", "decimal(10,2)"
    nullable: bool


def columns_from_uc_table(table: Any) -> list[MartColumn]:
    """Map a UC TableInfo to ordered MartColumns (sorted by column position)."""
    cols = sorted(table.columns, key=lambda c: c.position)
    return [MartColumn(name=c.name, spark_type=c.type_text, nullable=bool(c.nullable)) for c in cols]


def introspect_mart(fqn: str) -> list[MartColumn]:
    """Fetch `<catalog>.<schema>.<table>` column schema from Unity Catalog.

    Imported lazily so unit tests (which pass a fake table to columns_from_uc_table)
    don't require the databricks SDK or live credentials.
    """
    from databricks.sdk import WorkspaceClient

    table = WorkspaceClient().tables.get(full_name=fqn)
    if not getattr(table, "columns", None):
        raise RuntimeError(f"Unity Catalog table {fqn!r} returned no columns")
    return columns_from_uc_table(table)
