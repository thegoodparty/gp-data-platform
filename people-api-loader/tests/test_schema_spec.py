"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema import _serving_seed as seed
from loader.people_api.schema import _serving_seed_extra as seed_extra
from loader.people_api.schema.index_specs import IndexDef
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


def test_hand_added_extras_merge_and_survive_regeneration() -> None:
    # The extras live outside the generated seed (extract-serving-structure
    # overwrites _serving_seed.py wholesale) and must be present in the
    # composed spec via the extras path — the generated seed must NOT carry
    # them, or _serving_seed_extra.py becomes dead code whose edits are
    # silently ignored.
    generated_names = {i.name for i in seed.INDEXES}
    idxs = indexes_for("Voter")
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

    idxs = indexes_for("Voter")
    assert any(i.name == "Voter_extra_only_idx" for i in idxs)
    winner = next(i for i in idxs if i.name == generated_name)
    assert winner.sql != colliding.sql
    assert [i.name for i in idxs].count(generated_name) == 1
