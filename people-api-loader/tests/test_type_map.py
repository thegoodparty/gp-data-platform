"""Spark/Delta type strings -> Postgres types."""

from __future__ import annotations

import pytest

from loader.people_api.schema.type_map import UnknownSparkType, to_pg_type


@pytest.mark.parametrize(
    ("spark", "pg"),
    [
        ("string", "TEXT"),
        ("int", "INTEGER"),
        ("integer", "INTEGER"),
        ("bigint", "BIGINT"),
        ("smallint", "SMALLINT"),
        ("double", "DOUBLE PRECISION"),
        ("float", "REAL"),
        ("boolean", "BOOLEAN"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMPTZ"),
        ("timestamp_ntz", "TIMESTAMP"),
        ("binary", "BYTEA"),
        ("decimal(10,2)", "NUMERIC(10,2)"),
        ("decimal(38, 0)", "NUMERIC(38,0)"),
        ("DECIMAL(5,4)", "NUMERIC(5,4)"),  # case-insensitive
        ("  string  ", "TEXT"),  # whitespace tolerant
    ],
)
def test_known_types(spark: str, pg: str) -> None:
    assert to_pg_type(spark) == pg


def test_unknown_type_raises() -> None:
    with pytest.raises(UnknownSparkType, match="struct"):
        to_pg_type("struct<a:int>")
