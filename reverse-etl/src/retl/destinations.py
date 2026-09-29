"""The shared destination contract.

The caller (run.py) owns the log: each destination gets an `on_batch_confirmed`
callback that hands each confirmed batch to the log as it is confirmed (run.py
buffers and flushes them). A dry run never reaches a destination at all, so "a
rehearsal never writes any log" holds by construction in run.py.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class RowError:
    """The sanctioned error surface: never contact values, never tokens."""

    flow_id: str
    tracking_key: str | None
    error_code: str
    property: str | None
    retryable: bool


@dataclass(frozen=True)
class DeliveryResult:
    confirmed: dict[str, str]
    errors: list[RowError] = field(default_factory=list)


OnBatchConfirmed = Callable[[dict[str, str]], None]


class Destination(Protocol):
    def deliver(
        self,
        flow_id: str,
        rows: Sequence[tuple[str, str]],
        *,
        on_batch_confirmed: OnBatchConfirmed,
    ) -> DeliveryResult: ...
