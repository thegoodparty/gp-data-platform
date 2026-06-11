"""extract_create_tables returns CREATE TABLE statements only (no indexes/PK)."""

from __future__ import annotations

from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

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
