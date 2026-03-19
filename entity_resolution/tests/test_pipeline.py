"""Tests for pipeline.load_and_prepare."""

import pandas as pd
from pipeline import load_and_prepare


def _make_input(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal prematch DataFrame."""
    defaults = {
        "unique_id": "id_0",
        "source_name": "src_a",
        "first_name": "jane",
        "last_name": "doe",
        "first_name_aliases": '["jane"]',
        "election_date": "2024-11-05",
        "party": None,
        "email": None,
        "phone": None,
        "state": "WI",
        "official_office_name": "city council",
        "district_identifier": None,
        "br_race_id": None,
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows).astype(str)


def test_load_and_prepare_multi_source():
    """N distinct source_name values -> N DataFrames, sorted by name."""
    df = _make_input(
        [
            {"unique_id": "1", "source_name": "charlie"},
            {"unique_id": "2", "source_name": "alpha"},
            {"unique_id": "3", "source_name": "bravo"},
            {"unique_id": "4", "source_name": "alpha"},
        ]
    )
    result = load_and_prepare(df)
    assert len(result) == 3
    assert list(result[0]["source_name"].unique()) == ["alpha"]
    assert list(result[1]["source_name"].unique()) == ["bravo"]
    assert list(result[2]["source_name"].unique()) == ["charlie"]
    assert len(result[0]) == 2  # alpha has 2 records


def test_load_and_prepare_null_normalization():
    """Empty strings, 'nan', 'null' all become None."""
    df = _make_input(
        [
            {"unique_id": "1", "source_name": "a", "email": ""},
            {"unique_id": "2", "source_name": "a", "email": "nan"},
            {"unique_id": "3", "source_name": "a", "email": "null"},
            {"unique_id": "4", "source_name": "b", "email": "real@test.com"},
        ]
    )
    result = load_and_prepare(df)
    a_df = result[0]  # source "a"
    assert a_df["email"].isna().all() or (a_df["email"] == None).all()  # noqa: E711
    b_df = result[1]  # source "b"
    assert b_df["email"].iloc[0] == "real@test.com"


def test_load_and_prepare_aliases_parsed():
    """JSON alias strings are parsed into Python lists."""
    df = _make_input(
        [
            {
                "unique_id": "1",
                "source_name": "a",
                "first_name_aliases": '["robert", "bob", "rob"]',
            },
            {
                "unique_id": "2",
                "source_name": "b",
                "first_name_aliases": '["jane"]',
            },
        ]
    )
    result = load_and_prepare(df)
    aliases_a = result[0]["first_name_aliases"].iloc[0]
    assert isinstance(aliases_a, list)
    assert "bob" in aliases_a
    assert len(aliases_a) == 3
