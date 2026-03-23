from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .normalize_space import normalize_space_config
from .redact_space import redact_obj

METADATA_FIELDS = (
    "description",
    "display_name",
    "parent_path",
    "space_id",
    "title",
    "warehouse_id",
)


@dataclass(frozen=True)
class FetchedSpace:
    """Parsed Genie payload plus lightweight metadata collected at export time."""

    payload: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ExportArtifacts:
    """Filesystem paths written by an export run."""

    raw_path: Path
    normalized_path: Path
    redacted_path: Path | None = None
    metadata_path: Path | None = None


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _coerce_json_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): _coerce_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_coerce_json_value(item) for item in value]

    sentinel = object()
    enum_value = getattr(value, "value", sentinel)
    if enum_value is not sentinel and isinstance(enum_value, (str, int, float, bool)):
        return enum_value
    return str(value)


def _parse_serialized_space(serialized_space: Any) -> tuple[dict[str, Any], str]:
    if isinstance(serialized_space, str):
        try:
            payload = json.loads(serialized_space)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "serialized_space was returned but did not contain valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError("serialized_space JSON must decode to an object")
        return payload, serialized_space

    if isinstance(serialized_space, dict):
        return serialized_space, json.dumps(serialized_space, ensure_ascii=False)

    raise RuntimeError(
        "serialized_space returned an unexpected type; expected a JSON string export"
    )


def _build_metadata(
    *,
    host: str | None,
    payload: dict[str, Any],
    serialized_space: str,
    space: Any,
    requested_space_id: str,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "requested_space_id": requested_space_id,
        "exported_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "serialized_space_sha256": hashlib.sha256(
            serialized_space.encode("utf-8")
        ).hexdigest(),
        "top_level_keys": sorted(payload),
    }
    if host:
        metadata["databricks_host"] = host

    for field_name in METADATA_FIELDS:
        value = getattr(space, field_name, None)
        if value is not None:
            metadata[field_name] = _coerce_json_value(value)

    return metadata


def fetch_space_record(space_id: str) -> FetchedSpace:
    """Fetch a Genie space and parse its serialized configuration payload."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sdk is required. Install it in your Python environment first."
        ) from exc

    workspace_client = WorkspaceClient()
    space = workspace_client.genie.get_space(
        space_id=space_id,
        include_serialized_space=True,
    )
    serialized_space = getattr(space, "serialized_space", None)
    if not serialized_space:
        raise RuntimeError(
            "No serialized_space returned. Ensure the caller has CAN EDIT on the space "
            "and that Genie serialized export is enabled for this workspace."
        )

    payload, serialized_text = _parse_serialized_space(serialized_space)
    host = getattr(getattr(workspace_client, "config", None), "host", None)
    metadata = _build_metadata(
        host=host,
        payload=payload,
        serialized_space=serialized_text,
        space=space,
        requested_space_id=space_id,
    )
    return FetchedSpace(payload=payload, metadata=metadata)


def export_space_bundle(
    *,
    space_id: str,
    out_dir: Path,
    write_redacted: bool = False,
    write_metadata: bool = False,
) -> ExportArtifacts:
    fetched_space = fetch_space_record(space_id)
    normalized_payload = normalize_space_config(fetched_space.payload)

    raw_path = out_dir / "raw" / f"{space_id}.json"
    normalized_path = out_dir / "normalized" / f"{space_id}.json"
    write_json(raw_path, fetched_space.payload)
    write_json(normalized_path, normalized_payload)

    redacted_path: Path | None = None
    if write_redacted:
        redacted_path = out_dir / "redacted" / f"{space_id}.json"
        write_json(redacted_path, redact_obj(normalized_payload))

    metadata_path: Path | None = None
    if write_metadata:
        metadata_path = out_dir / "metadata" / f"{space_id}.json"
        write_json(metadata_path, fetched_space.metadata)

    return ExportArtifacts(
        raw_path=raw_path,
        normalized_path=normalized_path,
        redacted_path=redacted_path,
        metadata_path=metadata_path,
    )
