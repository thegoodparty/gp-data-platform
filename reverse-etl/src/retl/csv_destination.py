"""The CSV destinations: a preview that is never logged, and an export that is.

`CsvDestination` (`--destination csv`) is the rehearsal: a preview run must not mark
anyone as sent, so it never writes any log row. That guarantee holds because its
`deliver` never calls `on_batch_confirmed` -- there is no config flag to get wrong.

`CsvExportDestination` (`--destination csv_export`) is a real delivery: each run writes
only its diff to a new timestamped file and logs every row it wrote, so a rerun sends
nothing new even if an earlier file was deleted. The file is the delivery; keeping or
importing it is the consumer's job.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from .destinations import DeliveryResult, OnBatchConfirmed


@dataclass(frozen=True)
class CsvDestinationConfig:
    output_path: Path


def config_from_env(env: Mapping[str, str]) -> CsvDestinationConfig:
    raw_path = env.get("RETL_CSV_OUTPUT_PATH", "")
    if not raw_path:
        raise ValueError("RETL_CSV_OUTPUT_PATH is not set")
    return CsvDestinationConfig(output_path=Path(raw_path))


def _write_rows(handle: TextIO, rows: Sequence[tuple[str, str]]) -> None:
    writer = csv.writer(handle)
    writer.writerow(["tracking_key", "payload"])
    writer.writerows(rows)


class CsvDestination:
    def __init__(self, config: CsvDestinationConfig):
        self._config = config

    def deliver(
        self,
        flow_id: str,  # unused: part of the Destination contract; a CSV row needs no flow label
        rows: Sequence[tuple[str, str]],
        *,
        on_batch_confirmed: OnBatchConfirmed,  # unused: never called, see module docstring
    ) -> DeliveryResult:
        with self._config.output_path.open("w", newline="") as handle:
            _write_rows(handle, rows)
        return DeliveryResult(confirmed={}, errors=[])


@dataclass(frozen=True)
class CsvExportConfig:
    output_dir: Path


def export_config_from_env(env: Mapping[str, str]) -> CsvExportConfig:
    raw_dir = env.get("RETL_CSV_EXPORT_DIR", "")
    if not raw_dir:
        raise ValueError("RETL_CSV_EXPORT_DIR is not set")
    output_dir = Path(raw_dir)
    if not output_dir.is_dir():
        # Checked up front: a missing dir would otherwise fail only after the diff ran.
        raise ValueError(f"RETL_CSV_EXPORT_DIR {raw_dir!r} is not an existing directory")
    return CsvExportConfig(output_dir=output_dir)


class CsvExportDestination:
    def __init__(self, config: CsvExportConfig):
        self._config = config

    def deliver(
        self,
        flow_id: str,
        rows: Sequence[tuple[str, str]],
        *,
        on_batch_confirmed: OnBatchConfirmed,
    ) -> DeliveryResult:
        if not rows:
            # A no-op run leaves no file: an empty file would read as a real delivery.
            return DeliveryResult(confirmed={}, errors=[])

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        final_path = self._config.output_dir / f"{flow_id}_{stamp}.csv"
        partial_path = final_path.with_name(f".{final_path.name}.partial")
        # Written under a hidden name, then renamed: a consumer watching the dir never
        # picks up a half-written file. "x" refuses to overwrite an earlier export.
        with partial_path.open("x", newline="") as handle:
            _write_rows(handle, rows)
        partial_path.rename(final_path)

        # Logged only once the file is complete. If the log append then fails, the next
        # run re-exports these rows: a duplicate, never a silently dropped row.
        confirmed = dict(rows)
        on_batch_confirmed(confirmed)
        return DeliveryResult(confirmed=confirmed, errors=[])
