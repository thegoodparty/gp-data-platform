from __future__ import annotations

from typing import Any


def _sort_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {key: _sort_obj(obj[key]) for key in sorted(obj)}
    if isinstance(obj, list):
        return [_sort_obj(item) for item in obj]
    return obj


def normalize_space_config(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a conservative, diff-friendly Genie config representation."""
    return _sort_obj(payload)
