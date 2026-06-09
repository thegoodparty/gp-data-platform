"""Profile per-stage tokens and wall-clock time for a Win-analytics pipeline run.

Reads a Claude Code session transcript (JSONL) and attributes tokens and time
to the pipeline stages, splitting wall-clock into model-active vs human-idle.
Stage boundaries come from artifact markers. Sub-agent internal tokens are not
in the transcript and are reported as a blind spot.
See analytics/planning/2026-06-09-pipeline-profiler-design.md.

Stage definitions (these four map 1:1 to the pipeline stages in the
win-analytics-process skill's references/pipeline.md, which is the authority):

  framing      Pipeline stage 1 (Frame). From the win-analytics skill load to
               the `_brief.yaml` write. Includes the scoping conversation and the
               framing-approval gate: the human-idle here is largely you
               answering scoping questions and approving the brief.
  execution    Pipeline stage 2 (Execute) plus the stage-G results checkpoint.
               From the brief write to the first reviewer dispatch. Includes
               building the analysis artifact AND the results-checkpoint gate,
               which has no artifact marker, so the idle spent waiting for
               results approval is attributed here, not to a separate stage.
  review       Pipeline stages 3 and + (the product-data-scientist and
               product-manager reviewers). From the first reviewer dispatch to
               the last reviewer result. Reviewers run in parallel, so this is a
               max-span. Tokens here are only the parent's dispatch+ingest cost;
               reviewer internal tokens are the blind spot.
  calibration  Pipeline closing step. From the first `CALIBRATION_*.md` write to
               the first human message after it (trailing off-topic work
               excluded).
"""

from __future__ import annotations

import os
import re
from datetime import datetime

STAGES = ["framing", "execution", "review", "calibration"]
SKILL_NAMES = {"win-analytics-process", "win-analytics-knowledge"}
REVIEWER_TYPES = {"product-data-scientist", "product-manager"}


def _parse_ts(ts: str) -> datetime:
    """Parse an ISO 8601 transcript timestamp to a datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _is_human_message(record: dict) -> bool:
    """True when the record is a real human turn (text), not a tool result or meta."""
    if record.get("type") != "user" or record.get("isMeta"):
        return False
    message = record.get("message")
    if not isinstance(message, dict):
        return False
    return isinstance(message.get("content"), str)


_CALIBRATION_RE = re.compile(r"CALIBRATION_.*\.md$")


def _iter_tool_uses(record: dict) -> list[tuple[str, dict, str]]:
    """Return (name, input, id) for each tool_use block in an assistant record."""
    if record.get("type") != "assistant":
        return []
    message = record.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, list):
        return []
    out = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_use":
            out.append((block.get("name", ""), block.get("input") or {}, block.get("id", "")))
    return out


_TOKEN_KEYS = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")


def _token_totals(records: list[dict]) -> dict:
    """Sum usage token counts across assistant records in the slice."""
    totals = {k: 0 for k in _TOKEN_KEYS}
    for rec in records:
        if rec.get("type") != "assistant":
            continue
        usage = rec.get("message", {}).get("usage") if isinstance(rec.get("message"), dict) else None
        if not isinstance(usage, dict):
            continue
        for k in _TOKEN_KEYS:
            totals[k] += usage.get(k, 0) or 0
    return totals


def _split_time(records: list[dict]) -> tuple[float, float]:
    """Return (model_active_seconds, human_idle_seconds) across the slice."""
    active = 0.0
    idle = 0.0
    prev_ts = None
    for rec in records:
        ts = rec.get("timestamp")
        if not ts:
            continue
        cur = _parse_ts(ts)
        if prev_ts is not None:
            delta = (cur - prev_ts).total_seconds()
            if _is_human_message(rec):
                idle += delta
            else:
                active += delta
        prev_ts = cur
    return active, idle


def _classify_marker(name: str, tool_input: dict) -> str | None:
    """Map a tool_use to a stage-boundary marker, or None."""
    if name == "Skill" and tool_input.get("skill") in SKILL_NAMES:
        return "skill_load"
    if name in ("Agent", "Task") and tool_input.get("subagent_type") in REVIEWER_TYPES:
        return "reviewer_dispatch"
    if name in ("Write", "Edit"):
        path = str(tool_input.get("file_path", ""))
        base = os.path.basename(path)
        if base.endswith("_brief.yaml"):
            return "brief_write"
        if _CALIBRATION_RE.search(base):
            return "calibration_write"
    return None
