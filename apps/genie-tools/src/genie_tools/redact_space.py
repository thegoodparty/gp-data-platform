from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

CUSTOM_REDACTIONS_ENV = "GENIE_TOOLS_REDACTIONS_JSON"
EXACT_KEY_PLACEHOLDERS = {
    "workspace_id": "<WORKSPACE_ID>",
    "warehouse_id": "<WAREHOUSE_ID>",
    "sql_warehouse_id": "<WAREHOUSE_ID>",
    "space_id": "<SPACE_ID>",
    "genie_space_id": "<SPACE_ID>",
    "host": "<DATABRICKS_HOST>",
}
TEXTUAL_ENV_REPLACEMENTS = {
    "DATABRICKS_HOST": "<DATABRICKS_HOST>",
    "DATABRICKS_WAREHOUSE_ID": "<WAREHOUSE_ID>",
    "DATABRICKS_GENIE_SPACE_ID": "<SPACE_ID>",
    "DATABRICKS_WORKSPACE_ID": "<WORKSPACE_ID>",
}
ABSOLUTE_PATH_RE = re.compile(
    r"(?:/Users/[^\s\"']+|/Workspace/[^\s\"']+|/Volumes/[^\s\"']+|dbfs:/[^\s\"']+)"
)
DATABRICKS_URL_RE = re.compile(
    r"https://[A-Za-z0-9._:-]+"
    r"(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com|databricksapps\.com)"
    r"(?:/[^\s\"']*)?"
)
OPAQUE_ID_RE = re.compile(
    r"^(?:[0-9a-f]{16,}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
    re.IGNORECASE,
)
BACKTICK_FQN_RE = re.compile(
    r"`([A-Za-z_][A-Za-z0-9_]*)`\.`([A-Za-z_][A-Za-z0-9_]*)`\.`([A-Za-z_][A-Za-z0-9_]*)`"
)
PLAIN_FQN_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b"
)
PLAIN_FQN_EXACT_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$"
)
SQL_CONTEXT_RE = re.compile(
    r"\b(select|from|join|update|into|delete|insert|merge|table|with|using|create|replace|alter|drop)\b",
    re.IGNORECASE,
)


@dataclass
class _RedactionContext:
    opaque_id_map: dict[str, str] = field(default_factory=dict)
    text_replacements: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.text_replacements:
            return

        self.text_replacements.update(_load_custom_replacements())
        for env_name, placeholder in TEXTUAL_ENV_REPLACEMENTS.items():
            env_value = os.getenv(env_name)
            if env_value:
                self.text_replacements[env_value] = placeholder

    def opaque_id_placeholder(self, value: str) -> str:
        placeholder = self.opaque_id_map.get(value)
        if placeholder is None:
            placeholder = f"<GENIE_ID_{len(self.opaque_id_map) + 1:02d}>"
            self.opaque_id_map[value] = placeholder
        return placeholder


def _load_custom_replacements() -> dict[str, str]:
    raw_value = os.getenv(CUSTOM_REDACTIONS_ENV)
    if not raw_value:
        return {}

    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{CUSTOM_REDACTIONS_ENV} must be valid JSON mapping source strings to placeholders"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            f"{CUSTOM_REDACTIONS_ENV} must be a JSON object mapping source strings to placeholders"
        )

    replacements: dict[str, str] = {}
    for old_value, new_value in payload.items():
        if not isinstance(old_value, str) or not isinstance(new_value, str):
            raise ValueError(f"{CUSTOM_REDACTIONS_ENV} keys and values must be strings")
        if old_value:
            replacements[old_value] = new_value
    return replacements


def _looks_like_absolute_path(value: str) -> bool:
    return value.startswith(("/Users/", "/Workspace/", "/Volumes/", "dbfs:/"))


def _replace_catalog_schema_prefixes(text: str, *, replace_plain_fqn: bool) -> str:
    text = BACKTICK_FQN_RE.sub(
        lambda match: f"`<CATALOG>`.`<SCHEMA>`.`{match.group(3)}`",
        text,
    )
    if not replace_plain_fqn:
        return text
    return PLAIN_FQN_RE.sub(
        lambda match: f"<CATALOG>.<SCHEMA>.{match.group(3)}",
        text,
    )


def _should_replace_plain_fqn(text: str, key_hint: str | None) -> bool:
    if PLAIN_FQN_EXACT_RE.fullmatch(text):
        return True
    if key_hint == "identifier":
        return True
    return SQL_CONTEXT_RE.search(text) is not None


def _placeholder_for_key(
    key_hint: str | None, value: Any, context: _RedactionContext
) -> str | None:
    if not key_hint:
        return None

    key = key_hint.lower()
    if value in ("", None):
        return None

    if key in EXACT_KEY_PLACEHOLDERS:
        return EXACT_KEY_PLACEHOLDERS[key]
    if key == "id" and isinstance(value, str) and OPAQUE_ID_RE.fullmatch(value):
        return context.opaque_id_placeholder(value)
    if key.endswith("_url") or key == "url":
        return "<URL>"
    if key.endswith("_host"):
        return "<DATABRICKS_HOST>"
    if key.endswith("_path") or key == "path":
        if isinstance(value, str) and _looks_like_absolute_path(value):
            return "<ABSOLUTE_PATH>"
    if key.endswith("warehouse_id"):
        return "<WAREHOUSE_ID>"
    if key.endswith("workspace_id"):
        return "<WORKSPACE_ID>"
    if key.endswith("space_id"):
        return "<SPACE_ID>"
    return None


def _replace_text(
    text: str, key_hint: str | None = None, context: _RedactionContext | None = None
) -> str:
    if context is None:
        context = _RedactionContext()

    original_text = text
    direct_placeholder = _placeholder_for_key(key_hint, text, context)
    if direct_placeholder is not None:
        return direct_placeholder

    text = DATABRICKS_URL_RE.sub("<URL>", text)
    text = ABSOLUTE_PATH_RE.sub("<ABSOLUTE_PATH>", text)
    text = _replace_catalog_schema_prefixes(
        text,
        replace_plain_fqn=_should_replace_plain_fqn(original_text, key_hint),
    )
    for old_value in sorted(context.text_replacements, key=len, reverse=True):
        text = text.replace(old_value, context.text_replacements[old_value])
    return text


def redact_obj(
    obj: Any,
    key_hint: str | None = None,
    context: _RedactionContext | None = None,
) -> Any:
    """Redact environment-specific values while keeping Genie semantics intact."""
    if context is None:
        context = _RedactionContext()

    placeholder = _placeholder_for_key(key_hint, obj, context)
    if placeholder is not None and isinstance(obj, (str, int, float)):
        return placeholder

    if isinstance(obj, dict):
        return {
            key: redact_obj(value, key_hint=key, context=context)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [redact_obj(value, key_hint=key_hint, context=context) for value in obj]
    if isinstance(obj, str):
        return _replace_text(text=obj, key_hint=key_hint, context=context)
    return obj
