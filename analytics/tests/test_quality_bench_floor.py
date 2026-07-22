from pathlib import Path

import pandas as pd
from quality_bench import floor_gen

STATIC_DIR = Path(__file__).parent.parent / "diagnostics" / "quality_bench"


def fake_run_query(sql: str) -> pd.DataFrame:
    assert "information_schema" in sql
    assert "comment" not in sql.lower(), "inventory must not select curated comments"
    return pd.DataFrame(
        {
            "table_name": ["users_win_base", "stg_x"],
            "table_type": ["MANAGED", "VIEW"],
            "n_columns": [12, 3],
        }
    )


def test_build_inventory_md_is_identifiers_and_neutral_metadata_only():
    md = floor_gen.build_inventory_md(fake_run_query)
    lines = md.split("\n")
    assert lines[0] == "| table | type | columns |"
    assert "| users_win_base | MANAGED | 12 |" in md
    assert "| stg_x | VIEW | 3 |" in md
    for line in lines:
        assert line.startswith("|"), f"Malformed row: {line}"


def test_curated_comment_prose_cannot_reach_the_floor():
    """DATA-2164 floor leakage: even if the query returned prose-bearing
    columns, the renderer only emits name/type/count fields."""

    def poisoned(sql: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "table_name": ["t"],
                "table_type": ["MANAGED"],
                "n_columns": [1],
                "comment": ["governed 3-era activation logic, do not reuse"],
            }
        )

    md = floor_gen.build_inventory_md(poisoned)
    assert "governed 3-era" not in md


def test_build_floor_fills_template(tmp_path):
    static = tmp_path / "floor_static.md"
    static.write_text("head\n{{TABLE_INVENTORY}}\ntail")
    out = floor_gen.build_floor(static, fake_run_query)
    assert out.startswith("head\n| table |")
    assert out.endswith("tail")


def test_real_floor_static_has_allowlist_section():
    text = (STATIC_DIR / "floor_static.md").read_text()
    assert "## Shared operational facts" in text
    assert "{{TABLE_INVENTORY}}" in text
