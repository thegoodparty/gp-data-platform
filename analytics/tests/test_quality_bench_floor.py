# analytics/tests/test_quality_bench_floor.py
from pathlib import Path

import pandas as pd
from quality_bench import floor_gen

STATIC_DIR = Path(__file__).parent.parent / "diagnostics" / "quality_bench"


def fake_run_query(sql: str) -> pd.DataFrame:
    assert "information_schema" in sql
    return pd.DataFrame({"table_name": ["users_win_base", "stg_x"], "comment": ["Win users mart", None]})


def test_build_inventory_md():
    md = floor_gen.build_inventory_md(fake_run_query)
    assert "users_win_base" in md
    assert "Win users mart" in md
    assert "(no description)" in md  # None comment rendered


def test_build_floor_fills_inventory_only():
    floor = floor_gen.build_floor(STATIC_DIR / "floor_static.md", fake_run_query)
    assert "users_win_base" in floor
    assert "{{TABLE_INVENTORY}}" not in floor
    # arm-specific slots are left for prep_arms
    assert "{{LIB_PATH}}" in floor and "{{UV_PROJECT}}" in floor
