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
    # Both partitioned tables use the serving "State" column (DistrictVoter's mart `state` is
    # renamed to "State" via mart_column_map — see test_districtvoter_spec).
    assert ss.partition_column("Voter") == "State"
    assert ss.partition_column("DistrictVoter") == "State"
    assert ss.partition_column("District") is None


def test_districtvoter_spec() -> None:
    # The DistrictVoter mart is denormalized; mart_column_map projects it to the 5-column Prisma
    # serving shape and renames mart `state` -> serving "State".
    spec = ss.TABLE_SPECS["DistrictVoter"]
    assert spec.partition_by == "State"
    assert spec.mart_column_map == {
        "district_id": "district_id",
        "voter_id": "voter_id",
        "created_at": "created_at",
        "updated_at": "updated_at",
        "state": "State",
    }
    assert spec.type_overrides == {
        "district_id": "UUID",
        "voter_id": "UUID",
        "created_at": "TIMESTAMPTZ",
        "updated_at": "TIMESTAMPTZ",
        "state": "TEXT",
    }
    # The other tables' marts already match serving -> no column map.
    assert ss.TABLE_SPECS["Voter"].mart_column_map == {}
    assert ss.TABLE_SPECS["District"].mart_column_map == {}
    assert ss.TABLE_SPECS["DistrictStats"].mart_column_map == {}


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
