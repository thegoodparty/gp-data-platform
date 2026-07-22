"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema import _serving_seed as seed
from loader.people_api.schema import _serving_seed_extra as seed_extra
from loader.people_api.schema import schema_spec as ss
from loader.people_api.schema.index_specs import IndexDef


def test_all_four_tables_specced() -> None:
    assert set(ss.TABLE_SPECS) == {"Voter", "District", "DistrictStats", "DistrictVoter"}


def test_lookup_helpers_filter_by_table() -> None:
    # Works whether the committed seed is the empty placeholder or populated.
    assert isinstance(ss.indexes_for("Voter"), list)
    assert all(i.table == "Voter" for i in ss.indexes_for("Voter"))
    pk = ss.primary_key_for("Voter")
    assert pk is None or pk.table == "Voter"


def test_hand_added_extras_merge_and_survive_regeneration() -> None:
    # The extras live outside the generated seed (extract-serving-structure
    # overwrites _serving_seed.py wholesale) and must be present in the
    # composed spec via the extras path — the generated seed must NOT carry
    # them, or _serving_seed_extra.py becomes dead code whose edits are
    # silently ignored.
    generated_names = {i.name for i in seed.INDEXES}
    idxs = ss.indexes_for("Voter")
    names = [i.name for i in idxs]
    for expected in (
        "Voter_firstname_lower_trgm_idx",
        "Voter_lastname_lower_trgm_idx",
        "Voter_last_first_id_idx",
        "Voter_firstname_lower_idx",
        "Voter_lastname_lower_idx",
    ):
        assert expected not in generated_names
        assert expected in names
    assert len(names) == len(set(names))
    trgm = next(i for i in idxs if i.name == "Voter_firstname_lower_trgm_idx")
    assert 'USING gin (lower("FirstName") gin_trgm_ops)' in trgm.sql


def test_extras_merge_and_name_collision_suppression(monkeypatch) -> None:
    # Both dedup branches, isolated from the committed contents: an extra whose
    # name is absent from the generated seed merges in; a colliding name is
    # suppressed and the generated entry wins.
    novel = IndexDef(
        table="Voter",
        name="Voter_extra_only_idx",
        sql='CREATE INDEX "Voter_extra_only_idx" ON public."Voter" USING btree ("Age");',
        unique=False,
        columns=["Age"],
        where=None,
    )
    generated_name = seed.INDEXES[0].name
    colliding = IndexDef(
        table="Voter",
        name=generated_name,
        sql='CREATE INDEX "collide" ON public."Voter" USING btree ("Age");',
        unique=False,
        columns=["Age"],
        where=None,
    )
    monkeypatch.setattr(seed_extra, "EXTRA_INDEXES", [novel, colliding])

    idxs = ss.indexes_for("Voter")
    assert any(i.name == "Voter_extra_only_idx" for i in idxs)
    winner = next(i for i in idxs if i.name == generated_name)
    assert winner.sql != colliding.sql
    assert [i.name for i in idxs].count(generated_name) == 1


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
        "state": '"USState"',
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
