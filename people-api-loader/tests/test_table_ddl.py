"""extract_create_tables returns CREATE TABLE statements only (no indexes/PK)."""

from __future__ import annotations

from loader.people_api.schema.table_ddl import (
    extract_column_names,
    extract_column_types,
    extract_create_tables,
)

_DUMP = """
CREATE TABLE public."Voter" (
    "LALVOTERID" text NOT NULL,
    "State" text NOT NULL,
    created_at timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id uuid NOT NULL
);

ALTER TABLE ONLY public."Voter"
    ADD CONSTRAINT "Voter_pkey" PRIMARY KEY (id);

CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");
"""


def test_extracts_voter_table_only() -> None:
    tables = extract_create_tables(_DUMP)
    assert set(tables) == {"Voter"}
    stmt = tables["Voter"]
    assert stmt.startswith('CREATE TABLE public."Voter" (')
    assert stmt.rstrip().endswith(");")
    # columns present, but no index/PK statements pulled in
    assert '"LALVOTERID" text NOT NULL' in stmt
    assert "timestamp(3)" in stmt  # a ')' inside the body must not terminate the match
    assert "CREATE INDEX" not in stmt
    assert "ADD CONSTRAINT" not in stmt


def test_extract_column_names_in_ddl_order() -> None:
    # Quoted and bare (Prisma) columns, in physical order; constraint lines excluded.
    cols = extract_column_names(extract_create_tables(_DUMP)["Voter"])
    assert cols == ["LALVOTERID", "State", "created_at", "id"]


def test_extract_column_types_from_generated_ddl() -> None:
    # The emit-ddl-generated shape: quoted columns, uppercase PG types (NOT NULL stripped).
    ddl = (
        'CREATE TABLE public."Voter" (\n'
        '    "id" UUID NOT NULL,\n'
        '    "Age_Int" INTEGER,\n'
        '    "Estimated_Income" NUMERIC(12,2),\n'
        '    "State" TEXT NOT NULL\n'
        ");"
    )
    assert extract_column_types(ddl) == {
        "id": "UUID",
        "Age_Int": "INTEGER",
        "Estimated_Income": "NUMERIC(12,2)",
        "State": "TEXT",
    }


def test_extract_column_types_captures_quoted_enum_type() -> None:
    # State/state is typed against the public."USState" enum: a quoted type identifier. The
    # unquoted-only pattern would silently drop this column (and thus its FORCE_NULL handling).
    ddl = (
        'CREATE TABLE public."Voter" (\n'
        '    "id" UUID NOT NULL,\n'
        '    "State" "USState",\n'
        '    "LALVOTERID" TEXT\n'
        ");"
    )
    types = extract_column_types(ddl)
    assert types["State"] == '"USState"'
    assert types == {"id": "UUID", "State": '"USState"', "LALVOTERID": "TEXT"}


def test_extract_column_types_captures_lowercase_type() -> None:
    # A type_override can emit a lowercase PG type (DistrictStats' buckets -> jsonb). It must still
    # be captured; a uppercase-only pattern would silently drop it (and thus its FORCE_NULL handling).
    ddl = 'CREATE TABLE public."DistrictStats" (\n    "district_id" TEXT NOT NULL,\n    "buckets" jsonb\n);'
    assert extract_column_types(ddl) == {"district_id": "TEXT", "buckets": "jsonb"}
