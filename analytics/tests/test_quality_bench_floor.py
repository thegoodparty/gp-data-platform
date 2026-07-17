# analytics/tests/test_quality_bench_floor.py
from pathlib import Path

import pandas as pd
from quality_bench import floor_gen

STATIC_DIR = Path(__file__).parent.parent / "diagnostics" / "quality_bench"


def fake_run_query(sql: str) -> pd.DataFrame:
    assert "information_schema" in sql
    return pd.DataFrame(
        {
            "table_name": ["users_win_base", "stg_x", "stg_with_issues"],
            "comment": ["Win users mart", None, "Comment with\nnewline and | pipe"],
        }
    )


def test_build_inventory_md():
    md = floor_gen.build_inventory_md(fake_run_query)
    assert "users_win_base" in md
    assert "Win users mart" in md
    assert "(no description)" in md  # None comment rendered


def test_build_inventory_md_sanitizes_comments():
    """Comments with newlines and pipes must be escaped for markdown tables."""
    md = floor_gen.build_inventory_md(fake_run_query)
    lines = md.split("\n")
    # Each line should start with '|' for a valid markdown table
    for line in lines:
        assert line.startswith("|"), f"Malformed row: {line}"
    # Check that newline was collapsed to space and pipe was escaped
    assert "Comment with newline and \\| pipe" in md
    # Verify no unescaped pipes in comments (only column separators)
    content_lines = [line for line in lines if "stg_with_issues" in line]
    assert len(content_lines) == 1, "Comment with newline should not span multiple rows"


def test_build_floor_fills_inventory_only():
    floor = floor_gen.build_floor(STATIC_DIR / "floor_static.md", fake_run_query)
    assert "users_win_base" in floor
    assert "{{TABLE_INVENTORY}}" not in floor
    # arm-specific slots are left for prep_arms
    assert "{{LIB_PATH}}" in floor and "{{UV_PROJECT}}" in floor
