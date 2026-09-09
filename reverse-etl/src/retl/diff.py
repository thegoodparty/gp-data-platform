"""The diff kernel: latest-per-key anti-join against sent_log.

The design's kernel is a single SQL anti-join over a relation that already carries
a `payload` column. This package builds that column with its own serializer
(payload.py) so a payload stays byte-identical across runs, then compares it here
against `latest_sent` (read from the flow's own log table in sent_log.py).
Only the comparison step moves from SQL to Python; the semantics -
latest-per-tracking_key within the flow's table, never all history - are the same.
"""

from __future__ import annotations

from collections.abc import Mapping


def compute_to_send(
    desired_payloads: Mapping[str, str], latest_sent_payloads: Mapping[str, str]
) -> dict[str, str]:
    """Rows whose serialized payload is new or has changed since the last logged send.

    A key with no logged payload, or whose current payload differs from the latest
    logged one, is included. Comparing against only the LATEST row per key (never all
    history) is deliberate: subtracting all history would suppress a value that
    returns to a previous state, e.g. a score that goes 3 -> 4 -> 3 would find its old
    payload in the log and never resend, leaving the destination on 4 forever.
    """
    return {
        tracking_key: payload
        for tracking_key, payload in desired_payloads.items()
        if latest_sent_payloads.get(tracking_key) != payload
    }


def orphaned_keys(latest_sent_payloads: Mapping[str, str], desired_payloads: Mapping[str, str]) -> set[str]:
    """Logged keys absent from the current desired-state population.

    Watches for a person-id remint (entity resolution reclustering someone under a
    new id): the old id stops appearing in the model but stays in sent_log, so it
    shows up here until an id-mismatch hold on the model side can arm.
    """
    return set(latest_sent_payloads) - set(desired_payloads)
