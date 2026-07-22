# analytics/diagnostics/quality_bench/floor_gen.py
"""Assemble the common floor: static plumbing text + a mechanically generated
table inventory. DATA-2164: the inventory carries identifiers and neutral
type/size metadata ONLY (no warehouse comments — dbt model descriptions are
curated prose, i.e. treatment content). The bare arm must know WHAT exists,
never what anything means; column-level detail is available to every arm via
live DESCRIBE / information_schema queries."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd

INVENTORY_SQL = """
select
  t.table_name,
  t.table_type,
  count(c.column_name) as n_columns
from goodparty_data_catalog.information_schema.tables t
left join goodparty_data_catalog.information_schema.columns c
  on c.table_schema = t.table_schema and c.table_name = t.table_name
where t.table_schema = 'dbt'
  and not endswith(t.table_name, '__dbt_tmp')
group by t.table_name, t.table_type
order by t.table_name
"""

RunQuery = Callable[[str], pd.DataFrame]


def build_inventory_md(run_query: RunQuery) -> str:
    df = run_query(INVENTORY_SQL)
    lines = ["| table | type | columns |", "|---|---|---|"]
    for _, row in df.iterrows():
        lines.append(f"| {row['table_name']} | {row['table_type']} | {int(row['n_columns'])} |")
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
