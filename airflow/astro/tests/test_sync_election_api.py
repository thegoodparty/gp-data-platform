"""Unit tests for the sync_election_api DAG row-transform helpers.

The transforms are pure tuple-position mappings, so a column-order mistake
silently corrupts Postgres data. These tests pin the field positions
explicitly. Mocks airflow/databricks/etc. so the file collects without the
Astro runtime installed.
"""

import sys
from datetime import datetime
from unittest.mock import MagicMock

import pytest

# Stub external modules so the DAG file can be imported in any environment.
_STUBS = (
    "airflow",
    "airflow.decorators",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
    "paramiko",
    "pendulum",
    "sshtunnel",
    "psycopg2",
    "psycopg2.extras",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from dags.sync_election_api import (  # noqa: E402
    DTI_COLUMNS,
    EOS_COLUMNS,
    OFFICE_HOLDER_SOURCE_COLUMNS,
    OFFICE_HOLDER_TARGET_COLUMNS,
    PERSON_SOURCE_COLUMNS,
    PERSON_TARGET_COLUMNS,
    PT_COLUMNS,
    ZTP_SOURCE_COLUMNS,
    ZTP_TARGET_COLUMNS,
    _office_holder_constraint_ddl,
    _office_holder_quality_gate,
    _office_holder_transform_row,
    _person_quality_gate,
    _person_spine_post_swap_ddl,
    _person_spine_pre_swap_ddl,
    _person_transform_row,
    _pt_quality_gate,
    _ztp_transform_row,
)


def test_ztp_transform_row_field_positions():
    """Each input field lands at its expected index in the output tuple."""
    source_values = {
        "position_id": "pos-1",
        "name": "Mayor",
        "br_database_id": 12345,
        "zip_code": "90210",
        "election_year": 2026,
        "election_date": "2026-11-03",
        "display_office_level": "City",
        "office_type": "Mayor",
        "state": "CA",
        "district": None,
        "voters_in_zip": 15688,
        "voters_in_zip_district": 9714,
        "pct_districtzip_to_zip": 0.619,
    }
    # ZTP_SOURCE_COLUMNS pins the input order; the dict is just for
    # readability — build the tuple by indexing into it.
    row = tuple(source_values[c] for c in ZTP_SOURCE_COLUMNS)

    out = _ztp_transform_row(row)

    assert len(out) == len(ZTP_TARGET_COLUMNS)
    out_by_name = dict(zip(ZTP_TARGET_COLUMNS, out, strict=False))

    # Generated fields
    assert isinstance(out_by_name["id"], str) and len(out_by_name["id"]) == 36
    assert isinstance(out_by_name["updated_at"], datetime)

    # Pass-through fields land in the correct positions
    for col, expected in source_values.items():
        assert out_by_name[col] == expected, f"{col} did not pass through"


def test_ztp_transform_row_id_is_deterministic():
    """uuid5(zip_code|position_id|election_date) — same input, same id."""
    row = tuple(
        {
            "position_id": "pos-1",
            "name": "Mayor",
            "br_database_id": 12345,
            "zip_code": "90210",
            "election_year": 2026,
            "election_date": "2026-11-03",
            "display_office_level": "City",
            "office_type": "Mayor",
            "state": "CA",
            "district": None,
            "voters_in_zip": 15688,
            "voters_in_zip_district": 9714,
            "pct_districtzip_to_zip": 0.619,
        }[c]
        for c in ZTP_SOURCE_COLUMNS
    )

    id_first = _ztp_transform_row(row)[0]
    id_second = _ztp_transform_row(row)[0]
    assert id_first == id_second


def test_ztp_source_columns_match_transform_arity():
    """Guards against ZTP_SOURCE_COLUMNS / _ztp_transform_row drift."""
    row = tuple(range(len(ZTP_SOURCE_COLUMNS)))
    out = _ztp_transform_row(row)
    assert len(out) == len(ZTP_TARGET_COLUMNS)


def test_dti_columns_pinned():
    """Pin DTI_COLUMNS to catch silent column reorderings.

    The DistrictTopIssue bulk-insert path passes values to
    psycopg2.extras.execute_values positionally, so swapping or dropping a
    DTI_COLUMNS entry would land the wrong value into Postgres without any
    error at insert time. Pin the list so reorderings show up as a failing
    test instead of a corrupted DistrictTopIssue table.
    """
    assert DTI_COLUMNS == [
        "id",
        "updated_at",
        "district_id",
        "issue",
        "issue_label",
        "score",
        "is_local",
        "is_regional",
        "is_state",
        "is_federal",
        "issue_rank",
    ]


def test_eos_columns_pinned():
    """Pin EOS_COLUMNS to catch silent column reorderings.

    Elected_Office_Support loads with no row transform: EOS_COLUMNS drives both
    the Databricks SELECT order and the positional Postgres insert, so a
    reordering would land values in the wrong columns without an insert-time
    error. The mart's column order must match this list.
    """
    assert EOS_COLUMNS == [
        "elected_office_id",
        "support_constituents",
        "total_constituents",
        "created_at",
        "updated_at",
    ]


def test_pt_columns_pinned():
    """Pin PT_COLUMNS to catch silent column reorderings.

    Projected_Turnout loads with no row transform: PT_COLUMNS drives both the
    Databricks SELECT order and the positional Postgres insert. id/district_id
    are both uuids and created_at/updated_at/inference_at are all timestamps,
    so a swap within either group would corrupt data without any insert-time
    error. The order mirrors the legacy dbt writer's column mapping for this
    table.
    """
    assert PT_COLUMNS == [
        "id",
        "created_at",
        "updated_at",
        "election_year",
        "election_code",
        "projected_turnout",
        "inference_at",
        "model_version",
        "district_id",
    ]


def test_pt_quality_gate_refuses_duplicate_keys():
    """Any duplicate (district_id, election_year, election_code) key refuses
    the swap — the invariant the swap delivery exists to guarantee."""
    with pytest.raises(ValueError, match="duplicate"):
        _pt_quality_gate(loaded_count=800_000, dup_keys=1, prior_key_count=800_000, null_keys=0)


def test_pt_quality_gate_refuses_coverage_collapse():
    """Staging under half the prior distinct-key count refuses the swap."""
    with pytest.raises(ValueError, match="refusing to swap"):
        _pt_quality_gate(loaded_count=300_000, dup_keys=0, prior_key_count=800_000, null_keys=0)


def test_pt_quality_gate_allows_dedupe_cutover():
    """Keys-vs-keys baseline: a deduped load matching the prior key count
    passes even when the prior table's RAW row count was much larger (bloat
    from the legacy upsert path must not refuse a legitimate cutover)."""
    _pt_quality_gate(loaded_count=800_000, dup_keys=0, prior_key_count=800_000, null_keys=0)


def test_pt_quality_gate_boundary_ratio_passes():
    """Exactly 0.5 passes — same boundary semantics as the other groups."""
    _pt_quality_gate(loaded_count=400_000, dup_keys=0, prior_key_count=800_000, null_keys=0)


def test_pt_quality_gate_cold_start_floor():
    """No prior table: implausibly small loads refuse, plausible loads pass."""
    with pytest.raises(ValueError, match="Cold-start"):
        _pt_quality_gate(loaded_count=99_999, dup_keys=0, prior_key_count=0, null_keys=0)
    _pt_quality_gate(loaded_count=100_000, dup_keys=0, prior_key_count=0, null_keys=0)


def test_pt_quality_gate_refuses_null_keys():
    """Any staged row with a NULL key column refuses the swap.

    Belt-and-braces: the staging LIKE-clone inherits NOT NULL from the live
    table so such a row should fail at load time, but the gate proves the
    property directly instead of relying on the inherited constraint.
    """
    with pytest.raises(ValueError, match="NULL"):
        _pt_quality_gate(loaded_count=800_000, dup_keys=0, prior_key_count=800_000, null_keys=1)


def test_person_columns_pinned():
    """Pin PERSON_SOURCE_COLUMNS to catch silent column reorderings.

    The Person bulk-insert is positional and most columns are same-typed text,
    so a reorder would land values in the wrong columns without an insert-time
    error. The mart's SELECT order must match this list.
    """
    assert PERSON_SOURCE_COLUMNS == [
        "id",
        "br_person_id",
        "slug",
        "first_name",
        "middle_name",
        "last_name",
        "nickname",
        "suffix",
        "full_name",
        "bio_text",
        "headshot_url",
        "website_url",
        "linkedin_url",
        "facebook_url",
        "twitter_url",
        "instagram_url",
        "email",
        "phone",
        "degrees",
        "experiences",
        "state",
    ]
    assert [*PERSON_SOURCE_COLUMNS, "updated_at"] == PERSON_TARGET_COLUMNS


def test_person_transform_row_appends_timestamp_only():
    """Pass-through except a single appended updated_at."""
    row = tuple(range(len(PERSON_SOURCE_COLUMNS)))
    out = _person_transform_row(row)
    assert len(out) == len(PERSON_TARGET_COLUMNS)
    assert out[: len(row)] == row
    assert isinstance(out[-1], datetime)


def test_office_holder_columns_pinned():
    """Pin OFFICE_HOLDER_SOURCE_COLUMNS to catch silent column reorderings."""
    assert OFFICE_HOLDER_SOURCE_COLUMNS == [
        "id",
        "br_office_holder_id",
        "position_name",
        "normalized_position_name",
        "office_title",
        "party_names",
        "start_at",
        "end_at",
        "term_date_specificity",
        "is_current",
        "is_appointed",
        "is_vacant",
        "number_of_seats",
        "next_election_date",
        "mailing_address_line_1",
        "mailing_address_line_2",
        "mailing_city",
        "mailing_state",
        "mailing_zip",
        "office_phone",
        "office_email",
        "website_url",
        "linkedin_url",
        "facebook_url",
        "twitter_url",
        "instagram_url",
        "sub_area_name",
        "sub_area_value",
        "state",
        "geo_id",
        "mtfcc",
        "person_id",
        "position_id",
    ]
    assert [*OFFICE_HOLDER_SOURCE_COLUMNS, "updated_at"] == OFFICE_HOLDER_TARGET_COLUMNS


def test_office_holder_transform_decodes_party_names():
    """party_names arrives as a JSON string (the mart emits to_json because
    the Databricks reader doesn't round-trip complex types) and must land as a
    Python list for psycopg2 to adapt to text[]; everything else passes
    through, plus an appended updated_at."""
    idx = OFFICE_HOLDER_SOURCE_COLUMNS.index("party_names")
    row = list(range(len(OFFICE_HOLDER_SOURCE_COLUMNS)))
    row[idx] = '["Republican", "Conservative"]'

    out = _office_holder_transform_row(tuple(row))

    assert len(out) == len(OFFICE_HOLDER_TARGET_COLUMNS)
    assert out[idx] == ["Republican", "Conservative"]
    assert isinstance(out[-1], datetime)
    passthrough = [v for i, v in enumerate(out[:-1]) if i != idx]
    assert passthrough == [v for i, v in enumerate(row) if i != idx]


def test_office_holder_transform_keeps_null_party_names():
    row = [None] * len(OFFICE_HOLDER_SOURCE_COLUMNS)
    out = _office_holder_transform_row(tuple(row))
    assert out[OFFICE_HOLDER_SOURCE_COLUMNS.index("party_names")] is None


def test_person_quality_gate():
    """NULL slugs and coverage collapses refuse; plausible loads pass."""
    with pytest.raises(ValueError, match="NULL slug"):
        _person_quality_gate(loaded_count=500_000, null_slugs=1, prior_count=500_000)
    with pytest.raises(ValueError, match="refusing to swap"):
        _person_quality_gate(loaded_count=200_000, null_slugs=0, prior_count=500_000)
    with pytest.raises(ValueError, match="Cold-start"):
        _person_quality_gate(loaded_count=99_999, null_slugs=0, prior_count=0)
    _person_quality_gate(loaded_count=500_000, null_slugs=0, prior_count=500_000)
    _person_quality_gate(loaded_count=100_000, null_slugs=0, prior_count=0)


def test_office_holder_quality_gate():
    """NULL person_ids and coverage collapses refuse; plausible loads pass."""
    with pytest.raises(ValueError, match="NULL person_id"):
        _office_holder_quality_gate(loaded_count=500_000, null_person_ids=1, prior_count=500_000)
    with pytest.raises(ValueError, match="refusing to swap"):
        _office_holder_quality_gate(loaded_count=200_000, null_person_ids=0, prior_count=500_000)
    with pytest.raises(ValueError, match="Cold-start"):
        _office_holder_quality_gate(loaded_count=99_999, null_person_ids=0, prior_count=0)
    _office_holder_quality_gate(loaded_count=520_000, null_person_ids=0, prior_count=500_000)


def test_office_holder_constraint_ddl_matches_prisma_migration():
    """Pin the staged FK DDL to the constraint semantics the election-api
    Prisma migrations created (20260708163759_add_person_officeholder +
    20260710150000_officeholder_person_fk_cascade). The person FK must
    reference the STAGED Person table (so it rides the rename swap and
    validates the two staged loads against each other), and the position
    orphan-nulling must precede the position FK so validation cannot fail on
    rows its SET NULL semantics would have cleared."""
    statements = _office_holder_constraint_ddl()
    ddl = "\n".join(statements)

    person_fk = next(s for s in statements if "OfficeHolder_new_person_id_fkey" in s)
    assert 'REFERENCES "staging"."Person_new"(id)' in person_fk
    assert "ON DELETE CASCADE" in person_fk  # dependent child of Person

    position_fk = next(s for s in statements if "OfficeHolder_new_position_id_fkey" in s)
    assert 'REFERENCES "public"."Position"(id)' in position_fk
    assert "ON DELETE SET NULL" in position_fk

    null_orphans = next(i for i, s in enumerate(statements) if s.startswith("UPDATE"))
    assert "SET position_id = NULL" in statements[null_orphans]
    assert null_orphans < statements.index(position_fk)

    assert '"OfficeHolder_new_pkey"' in ddl


def test_person_spine_candidacy_fk_rewiring():
    """Candidacy sits outside the swap group, so its FK to Person is dropped
    before the renames and re-created after them — with orphan person_ids
    nulled first (its ON DELETE SET NULL semantics) so validation cannot
    fail the swap."""
    pre = "\n".join(_person_spine_pre_swap_ddl())
    assert 'DROP CONSTRAINT IF EXISTS "Candidacy_person_id_fkey"' in pre

    statements = _person_spine_post_swap_ddl()
    candidacy_fk = next(s for s in statements if "ADD CONSTRAINT" in s)
    assert '"Candidacy_person_id_fkey"' in candidacy_fk
    assert "ON DELETE SET NULL" in candidacy_fk
    null_orphans = next(i for i, s in enumerate(statements) if "SET person_id = NULL" in s)
    assert null_orphans < statements.index(candidacy_fk)
