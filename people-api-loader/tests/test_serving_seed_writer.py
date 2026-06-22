"""The seed writer renders importable Python literals for the dataclasses."""

from __future__ import annotations

from loader.people_api.schema.index_specs import IndexDef, PrimaryKey
from loader.people_api.schema.serving_seed_writer import render_seed_module


def test_render_seed_module_is_valid_python_with_records() -> None:
    pks = [PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])]
    idxs = [
        IndexDef(table="Voter", name="Voter_x", sql="CREATE INDEX ...;", unique=False, columns=[], where=None)
    ]
    src = render_seed_module(pks, idxs, [])
    ns: dict = {}
    exec(compile(src, "_serving_seed.py", "exec"), ns)  # must be importable
    assert ns["PRIMARY_KEYS"][0].constraint == "Voter_pkey"
    assert ns["INDEXES"][0].name == "Voter_x"
    assert ns["FOREIGN_KEYS"] == []
