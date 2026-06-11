"""states: verify the STATES tuple has the right 51 entries."""

from __future__ import annotations

from loader.people_api.schema.states import STATES


def test_length() -> None:
    assert len(STATES) == 51


def test_tx_present() -> None:
    assert "TX" in STATES


def test_dc_present() -> None:
    assert "DC" in STATES


def test_no_duplicates() -> None:
    assert len(set(STATES)) == 51
