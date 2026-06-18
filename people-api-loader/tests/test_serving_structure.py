"""Serving-structure extraction maps pg_catalog rows to index/PK/FK dataclasses."""

from __future__ import annotations

from loader.people_api.schema.serving_structure import (
    extract_foreign_keys,
    extract_indexes,
    extract_primary_keys,
)


class _FakeCursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def execute(self, sql: str, params: object = None) -> None:
        self._executed = (sql, params)

    def fetchall(self) -> list[tuple]:
        return self._rows


def test_extract_indexes_skips_pk_and_keeps_definition() -> None:
    cur = _FakeCursor(
        [
            (
                "Voter",
                "Voter_LastName_idx",
                'CREATE INDEX "Voter_LastName_idx" ON public."Voter" USING btree ("LastName")',
                False,
            ),
            (
                "Voter",
                "Voter_family_idx",
                'CREATE INDEX "Voter_family_idx" ON public."Voter" '
                'USING btree ("Mailing_Families_FamilyID") WHERE ("Mailing_Families_FamilyID" IS NOT NULL)',
                False,
            ),
        ]
    )
    idxs = extract_indexes(cur, ["Voter"])
    assert [i.name for i in idxs] == ["Voter_LastName_idx", "Voter_family_idx"]
    assert idxs[1].where == '("Mailing_Families_FamilyID" IS NOT NULL)'
    assert idxs[1].unique is False


def test_extract_primary_keys() -> None:
    cur = _FakeCursor([("Voter", "Voter_pkey", 'PRIMARY KEY (id, "State")')])
    pks = extract_primary_keys(cur, ["Voter"])
    assert pks[0].constraint == "Voter_pkey"
    assert pks[0].columns == ["id", "State"]


def test_extract_foreign_keys() -> None:
    cur = _FakeCursor(
        [
            (
                "DistrictVoter",
                "DistrictVoter_voter_fkey",
                'ALTER TABLE ONLY public."DistrictVoter" ADD CONSTRAINT "DistrictVoter_voter_fkey" '
                'FOREIGN KEY (voter_id) REFERENCES public."Voter"(id)',
            )
        ]
    )
    fks = extract_foreign_keys(cur, ["DistrictVoter"])
    assert fks[0].constraint == "DistrictVoter_voter_fkey"
    assert "FOREIGN KEY" in fks[0].sql
