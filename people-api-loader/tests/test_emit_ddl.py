"""emit_ddl produces CREATE TABLE per state with no indexes/PKs."""

from __future__ import annotations

from loader.people_api.schema.emit_ddl import STATES, emit_target_schema

_PROD_DUMP = """
CREATE TABLE public."VoterTX" (
    "LALVOTERID" text,
    "Voters_FirstName" text,
    "Voters_Age" text
);
CREATE TABLE public."VoterFile" (
    "Filename" text,
    "State" text,
    "Lines" integer,
    "Loaded" timestamp,
    "updatedAt" timestamp
);
"""


def test_states_count() -> None:
    assert len(STATES) == 51
    assert "TX" in STATES and "DC" in STATES


def test_emit_target_schema_shape() -> None:
    ddl = emit_target_schema(_PROD_DUMP, databricks_columns={})
    # One CREATE TABLE per state + VoterFile, wrapped in a transaction.
    # "VoterFile" also matches the prefix, so total is len(STATES) + 1.
    assert ddl.count('CREATE TABLE public."Voter') == len(STATES) + 1  # 51 Voter{ST} + VoterFile
    assert 'CREATE TABLE public."VoterFile"' in ddl
    assert "BEGIN;" in ddl and "COMMIT;" in ddl
    # No indexes / PKs in the emitted schema.
    assert "CREATE INDEX" not in ddl.upper()
    assert "PRIMARY KEY" not in ddl.upper()
