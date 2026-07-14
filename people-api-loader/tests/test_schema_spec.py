"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema import schema_spec as ss


def test_all_four_tables_specced() -> None:
    assert set(ss.TABLE_SPECS) == {"Voter", "District", "DistrictStats", "DistrictVoter"}


def test_partition_flags() -> None:
    assert ss.is_partitioned("Voter") is True
    assert ss.is_partitioned("DistrictVoter") is True
    assert ss.is_partitioned("District") is False
    assert ss.is_partitioned("DistrictStats") is False
    # Only the Voter mart emits capital "State"; the District family (incl. DistrictVoter)
    # is lowercase "state" — the partition column is spec-driven per table.
    assert ss.partition_column("Voter") == "State"
    assert ss.partition_column("DistrictVoter") == "state"
    assert ss.partition_column("District") is None


def test_districtstats_spec() -> None:
    spec = ss.TABLE_SPECS["DistrictStats"]
    assert spec.partition_by is None
    assert spec.type_overrides.get("buckets") == "jsonb"
    # not a serving table -> PK carried on the spec, not the seed
    assert spec.primary_key is not None
    assert spec.primary_key.columns == ["district_id"]


def test_primary_key_for_spec_fallback() -> None:
    # DistrictStats is absent from _serving_seed; the spec PK is returned.
    pk = ss.primary_key_for("DistrictStats")
    assert pk is not None and pk.columns == ["district_id"]


def test_primary_key_for_seed_wins_when_present() -> None:
    # District IS in the seed; spec carries no PK, seed value is used.
    pk = ss.primary_key_for("District")
    assert pk is not None and pk.columns == ["id"]
