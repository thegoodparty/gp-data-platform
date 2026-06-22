"""emit_ddl renders pg_dump-shaped CREATE TABLE blocks from mart columns + spec overrides."""

from __future__ import annotations

from loader.people_api.schema.emit_ddl import render_create_table
from loader.people_api.schema.mart_introspect import MartColumn
from loader.people_api.schema.schema_spec import TableSpec


def test_render_applies_types_and_overrides_and_quotes() -> None:
    cols = [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="State", spark_type="string", nullable=False),
        MartColumn(name="Age_Int", spark_type="int", nullable=True),
    ]
    spec = TableSpec(pg_table="Voter", partition_by="State", type_overrides={"id": "UUID"})
    ddl = render_create_table(spec, cols)
    assert ddl == (
        'CREATE TABLE public."Voter" (\n'
        '    "id" UUID NOT NULL,\n'
        '    "State" TEXT NOT NULL,\n'
        '    "Age_Int" INTEGER\n'
        ");"
    )


def test_render_appends_prisma_extra_columns() -> None:
    cols = [MartColumn(name="id", spark_type="string", nullable=False)]
    spec = TableSpec(
        pg_table="Voter",
        partition_by="State",
        type_overrides={"id": "UUID"},
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    )
    ddl = render_create_table(spec, cols)
    assert ddl == (
        'CREATE TABLE public."Voter" (\n    "id" UUID NOT NULL,\n    "Mailing_HHGender_Description" TEXT\n);'
    )


def test_render_round_trips_through_extract_create_tables() -> None:
    from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

    cols = [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="name", spark_type="string", nullable=True),
    ]
    spec = TableSpec(pg_table="District", partition_by=None, type_overrides={"id": "UUID"})
    ddl = render_create_table(spec, cols)
    parsed = extract_create_tables(ddl)
    assert "District" in parsed
    assert extract_column_names(parsed["District"]) == ["id", "name"]
