"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema.schema_spec import TABLE_SPECS, indexes_for, primary_key_for


def test_spec_is_voter_only_with_overrides_partition_and_prisma_column() -> None:
    # Scope is Voter-only: the District family is built by the dbt write path and
    # DistrictStats isn't a serving table.
    assert set(TABLE_SPECS) == {"Voter"}
    voter = TABLE_SPECS["Voter"]
    assert voter.partition_by == "State"
    assert voter.type_overrides["id"] == "UUID"
    # The one serving column the mart omits is declared as a Prisma-layer extra.
    assert ("Mailing_HHGender_Description", "TEXT", True) in voter.extra_columns


def test_lookup_helpers_filter_by_table() -> None:
    # Works whether the committed seed is the empty placeholder or populated.
    assert isinstance(indexes_for("Voter"), list)
    assert all(i.table == "Voter" for i in indexes_for("Voter"))
    pk = primary_key_for("Voter")
    assert pk is None or pk.table == "Voter"
