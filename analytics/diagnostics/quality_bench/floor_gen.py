# analytics/diagnostics/quality_bench/floor_gen.py
"""Assemble the common floor (design §6): static plumbing text + a mechanically
generated table inventory. The inventory is deliberately mechanical (names +
warehouse comments), never curated skill prose — the bare arm must know WHAT
exists, not what anything means."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd

INVENTORY_SQL = """
select table_name, comment
from goodparty_data_catalog.information_schema.tables
where table_schema = 'dbt'
order by table_name
"""

RunQuery = Callable[[str], pd.DataFrame]


def build_inventory_md(run_query: RunQuery) -> str:
    df = run_query(INVENTORY_SQL)
    lines = ["| table | description |", "|---|---|"]
    for _, row in df.iterrows():
        comment = row["comment"] if isinstance(row["comment"], str) and row["comment"] else "(no description)"
        lines.append(f"| {row['table_name']} | {comment} |")
    return "\n".join(lines)


def build_floor(static_path: Path, run_query: RunQuery) -> str:
    return static_path.read_text().replace("{{TABLE_INVENTORY}}", build_inventory_md(run_query))


if __name__ == "__main__":
    # Bootstrap sys.path for bare `python` invocation: pytest's pythonpath
    # config (lib, diagnostics) only applies under pytest, not here.
    import sys

    sys.path.insert(0, str(Path(__file__).parents[2] / "lib"))
    import databricks_conn as dbc

    here = Path(__file__).parent
    out = here / "floor.md"
    out.write_text(build_floor(here / "floor_static.md", dbc.run_query))
    print(f"wrote {out}")
