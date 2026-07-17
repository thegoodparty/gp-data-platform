from pathlib import Path

import pytest
from quality_bench import bank

MANIFEST = """\
questions:
  - id: q01
    product: win
    trap: denominator
    split: calibration
    prompt_file: q01.md
    key_file: q01_key.yaml
"""

KEY = """\
id: q01
as_of: "2026-07-20"
numbers:
  - name: total_users_jan
    value: 9880
    tolerance_pct: 0.5
required_resolutions:
  denominator: cumulative_registered_upcoming_election
mandatory_sources:
  - id: provenance_csv
    pattern: "instrumentation_data/.*provenance"
    description: must open the omni provenance CSV
severity1_patterns:
  - "win users are a subset of serve"
required_assumptions:
  - denominator
intent_card: |
  Monthly board update; wants cumulative base.
"""


def test_load_manifest(tmp_path: Path):
    p = tmp_path / "manifest.yaml"
    p.write_text(MANIFEST)
    qs = bank.load_manifest(p)
    assert len(qs) == 1
    q = qs[0]
    assert (q.id, q.product, q.trap, q.split) == ("q01", "win", "denominator", "calibration")


def test_load_manifest_rejects_bad_split(tmp_path: Path):
    p = tmp_path / "manifest.yaml"
    p.write_text(MANIFEST.replace("calibration", "holdover"))
    with pytest.raises(ValueError, match="split"):
        bank.load_manifest(p)


def test_load_key(tmp_path: Path):
    p = tmp_path / "q01_key.yaml"
    p.write_text(KEY)
    key = bank.load_key(p)
    assert key.numbers[0].name == "total_users_jan"
    assert key.numbers[0].tolerance_pct == 0.5
    assert key.required_resolutions["denominator"] == "cumulative_registered_upcoming_election"
    assert key.mandatory_sources[0].id == "provenance_csv"
    assert "subset of serve" in key.severity1_patterns[0]
    assert "cumulative base" in key.intent_card


def test_load_key_requires_numbers(tmp_path: Path):
    p = tmp_path / "k.yaml"
    p.write_text("id: q01\nas_of: '2026-07-20'\nnumbers: []\n")
    with pytest.raises(ValueError, match="numbers"):
        bank.load_key(p)
