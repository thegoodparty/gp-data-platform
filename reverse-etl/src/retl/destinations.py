"""The shared destination contract.

The caller (run.py) owns the log: each destination gets an `on_batch_confirmed`
callback so an append happens exactly where a batch is confirmed, and the CSV
destination never calls it -- "a preview never writes any log" holds by
construction rather than by a config flag someone could get wrong.
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
