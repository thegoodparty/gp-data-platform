"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema.schema_spec import TABLE_SPECS, indexes_for, primary_key_for


def test_specs_cover_four_tables_with_overrides_and_partition() -> None:
    assert set(TABLE_SPECS) == {"Voter", "District", "DistrictStats", "DistrictVoter"}
    voter = TABLE_SPECS["Voter"]
    assert voter.partition_by == "State"
    assert voter.type_overrides["id"] == "UUID"
    assert TABLE_SPECS["District"].partition_by is None


def test_lookup_helpers_filter_by_table() -> None:
    # Works whether the committed seed is the empty placeholder or populated.
    assert isinstance(indexes_for("Voter"), list)
    assert all(i.table == "Voter" for i in indexes_for("Voter"))
    pk = primary_key_for("Voter")
    assert pk is None or pk.table == "Voter"
