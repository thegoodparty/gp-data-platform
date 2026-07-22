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


def fake_run_query_verbose(sql: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "table_name": ["multi_sentence", "long_single"],
            "comment": [
                "Win activation mart. Joins campaigns to sessions and derives the "
                "activated flag using the governed 3-era logic; do not reuse.",
                "A" * 400,
            ],
        }
    )


def test_build_inventory_md_truncates_to_first_sentence_and_caps_length():
    md = floor_gen.build_inventory_md(fake_run_query_verbose)
    ms_line = next(line for line in md.split("\n") if "multi_sentence" in line)
    # Only the first sentence survives; the curated remainder is dropped.
    assert "Win activation mart" in ms_line
    assert "governed 3-era logic" not in ms_line
    assert ms_line.rstrip().endswith("…|") or "…" in ms_line
    # The 400-char single "sentence" is capped at 150 chars (+ ellipsis).
    ls_line = next(line for line in md.split("\n") if "long_single" in line)
    a_run = ls_line.count("A")
    assert a_run == 150, f"expected 150 A's, got {a_run}"
    assert "…" in ls_line


def test_build_floor_fills_inventory_only():
    floor = floor_gen.build_floor(STATIC_DIR / "floor_static.md", fake_run_query)
    assert "users_win_base" in floor
    assert "{{TABLE_INVENTORY}}" not in floor
    # arm-specific slots are left for prep_arms
    assert "{{LIB_PATH}}" in floor and "{{UV_PROJECT}}" in floor
