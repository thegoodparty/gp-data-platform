"""The CSV destination: the same diff, written to a file, never logged.

A rehearsal or preview run must not mark anyone as sent, so this destination never
writes any log row. That guarantee holds because `deliver` never calls
`on_batch_confirmed` -- there is no config flag to get wrong.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .destinations import DeliveryResult, OnBatchConfirmed


@dataclass(frozen=True)
class CsvDestinationConfig:
    output_path: Path


def config_from_env(env: Mapping[str, str]) -> CsvDestinationConfig:
    raw_path = env.get("RETL_CSV_OUTPUT_PATH", "")
    if not raw_path:
        raise ValueError("RETL_CSV_OUTPUT_PATH is not set")
    return CsvDestinationConfig(output_path=Path(raw_path))


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
            writer = csv.writer(handle)
            writer.writerow(["tracking_key", "payload"])
            writer.writerows(rows)
        return DeliveryResult(confirmed={}, errors=[])
