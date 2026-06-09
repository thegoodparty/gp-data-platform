"""Parser tests for index_specs — pure regex over a fixture pg_dump."""

from __future__ import annotations

from loader.people_api.schema.index_specs import (
    parse_foreign_keys,
    parse_indexes,
    parse_primary_keys,
)

_FIXTURE = """
ALTER TABLE ONLY public."VoterTX"
    ADD CONSTRAINT "VoterTX_pkey" PRIMARY KEY ("LALVOTERID");

CREATE INDEX "VoterTX_Voters_LastName_idx" ON public."VoterTX" USING btree ("Voters_LastName");

CREATE UNIQUE INDEX "VoterTX_uniq_idx" ON public."VoterTX" USING btree ("LALVOTERID");

CREATE INDEX "VoterTX_family_idx" ON public."VoterTX" USING btree ("Mailing_Families_FamilyID")
    WHERE ("Mailing_Families_FamilyID" IS NOT NULL);

ALTER TABLE ONLY public."DistrictVoterTX"
    ADD CONSTRAINT "dv_fk" FOREIGN KEY ("voterId") REFERENCES public."VoterTX"("LALVOTERID");
"""


def test_parse_primary_keys() -> None:
    pks = parse_primary_keys(_FIXTURE)
    assert len(pks) == 1
    assert pks[0].table == "VoterTX"
    assert pks[0].constraint == "VoterTX_pkey"
    assert pks[0].columns == ["LALVOTERID"]


def test_parse_indexes_unique_and_where() -> None:
    idxs = {i.name: i for i in parse_indexes(_FIXTURE)}
    assert idxs["VoterTX_Voters_LastName_idx"].unique is False
    assert idxs["VoterTX_Voters_LastName_idx"].columns == ["Voters_LastName"]
    assert idxs["VoterTX_uniq_idx"].unique is True
    assert idxs["VoterTX_family_idx"].where is not None
    assert "IS NOT NULL" in idxs["VoterTX_family_idx"].where


def test_parse_foreign_keys() -> None:
    fks = parse_foreign_keys(_FIXTURE)
    assert len(fks) == 1
    assert fks[0].table == "DistrictVoterTX"
    assert fks[0].constraint == "dv_fk"
    assert fks[0].sql.strip().endswith(";")
