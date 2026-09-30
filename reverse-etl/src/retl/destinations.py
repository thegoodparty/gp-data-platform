"""The shared destination contract.

The caller (run.py) owns the log: a destination calls `on_batch_confirmed` with each
batch the moment it is confirmed, and never logs anything itself.
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
