"""Canonical payload serialization: the diff's fingerprint.

The payload is the exact set of destination properties a flow's model says a record
should carry, normalized and serialized so two runs over unchanged source data
produce byte-identical output. Sorted keys make the result independent of the
source query's column order; null/blank omission matches HubSpot's "empty string
clears a property" behavior, so we never accidentally emit a value that would
clear something we did not mean to touch.

The row's column names ARE the destination property names: the model resolves
ownership and picks names, and the source config carries only the model, the key
column, and the excluded columns, so there is no rename step here.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

JsonScalar = str | int | float | bool


def _normalize_value(value: Any) -> JsonScalar | None:
    """Reduce one raw column value to a JSON-safe scalar, or None to drop it."""
    if value is None:
        return None
    if isinstance(value, bool | str | int):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def build_payload(row: Mapping[str, Any], *, excluded_columns: frozenset[str]) -> dict[str, JsonScalar]:
    """Return `row`'s mapped properties, minus `excluded_columns`, nulls, and blanks."""
    payload: dict[str, JsonScalar] = {}
    for column, raw_value in row.items():
        if column in excluded_columns:
            continue
        normalized = _normalize_value(raw_value)
        if normalized is None or normalized == "":
            continue  # blank: HubSpot reads '' on a property as "clear this property"
        payload[column] = normalized
    return payload


def serialize_payload(payload: Mapping[str, JsonScalar]) -> str:
    """Sorted-key JSON text: the exact string compared against sent_log.payload."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
