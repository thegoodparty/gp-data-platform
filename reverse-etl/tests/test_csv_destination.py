from __future__ import annotations

from pathlib import Path

import pytest

from retl.csv_destination import (
    CsvDestination,
    CsvDestinationConfig,
    CsvExportConfig,
    CsvExportDestination,
    config_from_env,
    export_config_from_env,
)


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


def test_csv_export_writes_a_new_file_per_run_and_confirms_every_row(tmp_path: Path) -> None:
    """Catches: an export overwriting the previous run's file, or not logging what it wrote --
    either would make a rerun resend rows the destination already has."""
    destination = CsvExportDestination(CsvExportConfig(output_dir=tmp_path))
    calls: list[dict[str, str]] = []

    first = destination.deliver("hubspot_leads", [("p1", '{"a":1}')], on_batch_confirmed=calls.append)
    second = destination.deliver("hubspot_leads", [("p2", '{"a":2}')], on_batch_confirmed=calls.append)

    files = sorted(tmp_path.glob("hubspot_leads_*.csv"))
    assert len(files) == 2
    assert [f.read_text().splitlines()[1] for f in files] == ['p1,"{""a"":1}"', 'p2,"{""a"":2}"']
    assert calls == [{"p1": '{"a":1}'}, {"p2": '{"a":2}'}]
    assert first.confirmed == {"p1": '{"a":1}'}
    assert second.confirmed == {"p2": '{"a":2}'}


def test_csv_export_writes_no_file_when_there_is_nothing_to_send(tmp_path: Path) -> None:
    """Catches: a no-op run leaving an empty file behind, which a downstream import would
    treat as a real (empty) delivery."""
    destination = CsvExportDestination(CsvExportConfig(output_dir=tmp_path))
    calls: list[dict[str, str]] = []

    result = destination.deliver("hubspot_leads", [], on_batch_confirmed=calls.append)

    assert list(tmp_path.iterdir()) == []
    assert calls == []
    assert result.confirmed == {}


def test_csv_export_config_requires_an_existing_output_dir(tmp_path: Path) -> None:
    """Catches: an unset or mistyped export dir failing only after the diff was computed."""
    with pytest.raises(ValueError, match="RETL_CSV_EXPORT_DIR"):
        export_config_from_env({})
    with pytest.raises(ValueError, match="RETL_CSV_EXPORT_DIR"):
        export_config_from_env({"RETL_CSV_EXPORT_DIR": str(tmp_path / "missing")})
    assert export_config_from_env({"RETL_CSV_EXPORT_DIR": str(tmp_path)}).output_dir == tmp_path
