"""The CSV destination: each run's diff, written to a new file, and logged.

A real delivery like any other: each run writes only its diff to a new timestamped
file and logs every row it wrote, so a rerun sends nothing new even if an earlier
file was deleted. The file is the delivery; keeping or importing it is the
consumer's job. A rehearsal that must not mark anyone as sent is `--dry-run`, which
applies to every destination alike.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .destinations import DeliveryResult, OnBatchConfirmed


@dataclass(frozen=True)
class CsvDestinationConfig:
    output_dir: Path


def config_from_env(env: Mapping[str, str]) -> CsvDestinationConfig:
    raw_dir = env.get("RETL_CSV_OUTPUT_DIR", "")
    if not raw_dir:
        raise ValueError("RETL_CSV_OUTPUT_DIR is not set")
    output_dir = Path(raw_dir)
    if not output_dir.is_dir():
        # Checked up front: a missing dir would otherwise fail only after the diff ran.
        raise ValueError(f"RETL_CSV_OUTPUT_DIR {raw_dir!r} is not an existing directory")
    return CsvDestinationConfig(output_dir=output_dir)


class CsvDestination:
    def __init__(self, config: CsvDestinationConfig):
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
            writer = csv.writer(handle)
            writer.writerow(["tracking_key", "payload"])
            writer.writerows(rows)
        partial_path.rename(final_path)

        # Logged only once the file is complete. If the log append then fails, the next
        # run re-exports these rows: a duplicate, never a silently dropped row.
        confirmed = dict(rows)
        on_batch_confirmed(confirmed)
        return DeliveryResult(confirmed=confirmed, errors=[])
