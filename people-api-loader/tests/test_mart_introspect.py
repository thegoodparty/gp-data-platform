"""Mart introspection turns UC table columns into ordered (name, spark_type, nullable)."""

from __future__ import annotations

from types import SimpleNamespace

from loader.people_api.schema.mart_introspect import MartColumn, columns_from_uc_table


def test_columns_from_uc_table_preserves_order_and_fields() -> None:
    fake_table = SimpleNamespace(
        columns=[
            SimpleNamespace(name="id", type_text="string", nullable=False, position=0),
            SimpleNamespace(name="Age_Int", type_text="int", nullable=True, position=1),
        ]
    )
    assert columns_from_uc_table(fake_table) == [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="Age_Int", spark_type="int", nullable=True),
    ]


def test_columns_sorted_by_position_when_unordered() -> None:
    fake_table = SimpleNamespace(
        columns=[
            SimpleNamespace(name="b", type_text="int", nullable=True, position=1),
            SimpleNamespace(name="a", type_text="int", nullable=True, position=0),
        ]
    )
    assert [c.name for c in columns_from_uc_table(fake_table)] == ["a", "b"]
