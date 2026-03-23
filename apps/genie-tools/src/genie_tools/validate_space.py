from __future__ import annotations

import json
from pathlib import Path
from typing import Any

KNOWN_GENIE_SECTION_KEYS = frozenset(
    {
        "benchmarks",
        "benchmark_questions",
        "components",
        "config",
        "data_sources",
        "example_questions",
        "instructions",
        "joins",
        "queries",
        "tables",
        "verified_queries",
    }
)


def validate_space_payload(payload: Any) -> None:
    """Run lightweight structural validation on a serialized Genie space export."""
    if not isinstance(payload, dict):
        raise ValueError("Top-level JSON must be an object")

    if not payload:
        raise ValueError("Top-level JSON object is empty")

    structured_keys = [
        key for key, value in payload.items() if isinstance(value, (dict, list))
    ]
    if not structured_keys:
        raise ValueError(
            "Top-level JSON object does not contain any structured Genie config sections"
        )

    present_known_sections = sorted(KNOWN_GENIE_SECTION_KEYS.intersection(payload))
    if not present_known_sections:
        raise ValueError(
            "Top-level JSON object does not contain any recognized Genie sections"
        )

    for key in present_known_sections:
        if not isinstance(payload[key], (dict, list)):
            raise ValueError(
                f"Top-level section {key!r} must be a JSON object or array"
            )


def validate_space_file(path: Path) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    validate_space_payload(payload)
