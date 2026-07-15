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


def test_render_districtvoter_projects_and_renames_to_serving_shape() -> None:
    # The mart is denormalized (extra type/name, lowercase state); mart_column_map keeps only the
    # 5 serving columns and renames mart `state` -> "State".
    from loader.people_api.schema.schema_spec import TABLE_SPECS

    mart_cols = [
        MartColumn(name="voter_id", spark_type="string", nullable=False),
        MartColumn(name="district_id", spark_type="string", nullable=False),
        MartColumn(name="type", spark_type="string", nullable=False),
        MartColumn(name="name", spark_type="string", nullable=False),
        MartColumn(name="state", spark_type="string", nullable=False),
        MartColumn(name="created_at", spark_type="string", nullable=False),
        MartColumn(name="updated_at", spark_type="string", nullable=False),
    ]
    ddl = render_create_table(TABLE_SPECS["DistrictVoter"], mart_cols)
    assert ddl == (
        'CREATE TABLE public."DistrictVoter" (\n'
        '    "voter_id" UUID NOT NULL,\n'
        '    "district_id" UUID NOT NULL,\n'
        '    "State" TEXT NOT NULL,\n'
        '    "created_at" TIMESTAMPTZ NOT NULL,\n'
        '    "updated_at" TIMESTAMPTZ NOT NULL\n'
        ");"
    )
    # denormalized-only mart columns are dropped, and lowercase "state" never appears
    assert '"type"' not in ddl and '"name"' not in ddl and '"state"' not in ddl


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


def test_renders_all_four_tables(monkeypatch) -> None:
    from types import SimpleNamespace

    from loader.people_api.schema import emit_ddl

    marts = {
        "Voter": [
            MartColumn(name="id", spark_type="string", nullable=False),
            MartColumn(name="State", spark_type="string", nullable=False),
        ],
        "District": [
            MartColumn(name="id", spark_type="string", nullable=False),
            MartColumn(name="state", spark_type="string", nullable=False),
        ],
        "DistrictStats": [
            MartColumn(name="district_id", spark_type="string", nullable=False),
            MartColumn(name="buckets", spark_type="struct<...>", nullable=True),
        ],
        "DistrictVoter": [
            MartColumn(name="district_id", spark_type="string", nullable=False),
            MartColumn(name="voter_id", spark_type="string", nullable=False),
            MartColumn(name="state", spark_type="string", nullable=False),
        ],
    }

    cfg = SimpleNamespace(
        mart_fqns={
            "Voter": "cat.schema.m_people_api__voter",
            "District": "cat.schema.m_people_api__district",
            "DistrictStats": "cat.schema.m_people_api__districtstats",
            "DistrictVoter": "cat.schema.m_people_api__districtvoter",
        }
    )

    # Exact-match dispatch keyed on cfg.mart_fqns values to avoid suffix collision
    # (e.g., "districtvoter".endswith("voter") would incorrectly match Voter)
    fqn_to_pg = {v: k for k, v in cfg.mart_fqns.items()}

    def fake_introspect(fqn: str) -> list[MartColumn]:
        if fqn not in fqn_to_pg:
            raise AssertionError(f"Unknown FQN: {fqn}")
        return marts[fqn_to_pg[fqn]]

    monkeypatch.setattr(emit_ddl, "introspect_mart", fake_introspect)

    sql = emit_ddl.render_target_schema(cfg)  # type: ignore
    assert 'CREATE TABLE public."Voter" (' in sql
    assert 'CREATE TABLE public."District" (' in sql
    assert 'CREATE TABLE public."DistrictStats" (' in sql
    assert 'CREATE TABLE public."DistrictVoter" (' in sql
    # buckets override -> jsonb (case as written in schema_spec)
    assert '"buckets" jsonb' in sql
    # DistrictVoter-specific column to catch dispatch/rendering regression
    assert '"voter_id" UUID NOT NULL' in sql
