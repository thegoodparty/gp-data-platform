from __future__ import annotations

from pathlib import Path

import pytest

from retl.csv_destination import CsvDestination, CsvDestinationConfig, config_from_env


def test_csv_destination_writes_tracking_key_and_payload_rows(tmp_path: Path) -> None:
    """Catches: the CSV output missing a row, or writing payload/tracking_key in the wrong columns."""
    output_path = tmp_path / "preview.csv"
    destination = CsvDestination(CsvDestinationConfig(output_path=output_path))

    destination.deliver(
        "hubspot_leads", [("p1", '{"a":1}'), ("p2", '{"a":2}')], on_batch_confirmed=lambda _c: None
    )

    lines = output_path.read_text().splitlines()
    assert lines == ["tracking_key,payload", 'p1,"{""a"":1}"', 'p2,"{""a"":2}"']


def test_csv_destination_never_calls_on_batch_confirmed(tmp_path: Path) -> None:
    """Catches: a preview run marking anyone as sent -- the CSV destination must never write a log."""
    destination = CsvDestination(CsvDestinationConfig(output_path=tmp_path / "preview.csv"))
    calls: list[dict[str, str]] = []

    destination.deliver("hubspot_leads", [("p1", "{}")], on_batch_confirmed=calls.append)

    assert calls == []


def test_config_from_env_requires_output_path() -> None:
    """Catches: an unset CSV output path being silently accepted instead of failing fast."""
    with pytest.raises(ValueError, match="RETL_CSV_OUTPUT_PATH"):
        config_from_env({})
