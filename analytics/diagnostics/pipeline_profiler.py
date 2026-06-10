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

import argparse
import glob
import json
import os
import re
from dataclasses import dataclass, field
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


# These names must match the token fields on StageMetrics exactly: profile_run
# calls StageMetrics(**tokens) where tokens is built by _token_totals using these keys.
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


def _marker_indices(records: list[dict]) -> dict:
    """Return {marker_type: first_index} for each marker found."""
    found = {}
    for i, rec in enumerate(records):
        for name, tool_input, _tid in _iter_tool_uses(rec):
            marker = _classify_marker(name, tool_input)
            if marker and marker not in found:
                found[marker] = i
    return found


def _first_of(*vals: int | None, default: int) -> int:
    """Return the first non-None value among vals, else default."""
    return next((v for v in vals if v is not None), default)


def _first_human_after(records: list[dict], start: int) -> int:
    """Index of the first human message strictly after `start`, else len(records)."""
    for i in range(start + 1, len(records)):
        if _is_human_message(records[i]):
            return i
    return len(records)


def _reviewer_result_end_index(records: list[dict]) -> int | None:
    """Index of the last tool_result matching any reviewer dispatch id (max-span end)."""
    reviewer_ids = set()
    for rec in records:
        for name, tool_input, tid in _iter_tool_uses(rec):
            if _classify_marker(name, tool_input) == "reviewer_dispatch":
                reviewer_ids.add(tid)
    if not reviewer_ids:
        return None
    last = None
    for i, rec in enumerate(records):
        message = rec.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            for block in content:
                if (
                    isinstance(block, dict)
                    and block.get("type") == "tool_result"
                    and block.get("tool_use_id") in reviewer_ids
                ):
                    last = i
    return last


def segment_stages(records: list[dict]) -> tuple[dict, str]:
    """Return ({stage: (start_idx, end_idx)}, confidence).

    end_idx is exclusive (Python-slice style): records[start:end] yields the
    stage's records.
    """
    idx = _marker_indices(records)
    n = len(records)
    skill = idx.get("skill_load", 0)
    brief = idx.get("brief_write")
    reviewer = idx.get("reviewer_dispatch")
    calib = idx.get("calibration_write")
    review_end = _reviewer_result_end_index(records)

    confidence = "ok" if ("skill_load" in idx and brief is not None) else "low"

    # framing
    framing_end = _first_of(brief, reviewer, calib, default=n)
    framing = (skill, framing_end)
    # execution
    exec_start = brief if brief is not None else framing_end
    exec_end = _first_of(reviewer, calib, default=n)
    execution = (exec_start, max(exec_start, exec_end))
    # review
    if reviewer is not None:
        # +1 because review_end is the index of the last reviewer-result record and the range end is exclusive
        rev_end = (review_end + 1) if review_end is not None else _first_of(calib, default=n)
        review = (reviewer, max(reviewer, rev_end))
    else:
        review = (exec_end, exec_end)  # empty
    # calibration
    if calib is not None:
        calibration = (calib, _first_human_after(records, calib))
    else:
        calibration = (n, n)  # empty

    spans = {"framing": framing, "execution": execution, "review": review, "calibration": calibration}
    # Markers are assumed to occur in pipeline order. If they don't (e.g. a
    # contaminated or re-entered run writes a calibration file before reviewers
    # dispatch), the non-empty spans overlap and tokens/time would be
    # double-counted, so downgrade confidence rather than report a misleading "ok".
    prev_end = 0
    for stage in STAGES:
        start, end = spans[stage]
        if start >= end:
            continue  # empty stage
        if start < prev_end:
            confidence = "low"
        prev_end = max(prev_end, end)
    return spans, confidence


@dataclass
class StageMetrics:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    model_active_seconds: float = 0.0
    human_idle_seconds: float = 0.0


@dataclass
class RunDecisions:
    notebook_writes: int = 0
    analysis_script_writes: int = 0
    notebook_build_writes: int = 0
    reviewer_counts: dict[str, int] = field(default_factory=dict)
    process_design_edits: list[str] = field(default_factory=list)


_PROCESS_OWNING_BASENAMES = {"product-data-scientist.md", "product-manager.md", "run-product-analysis.md"}


def _is_process_owning(path: str) -> bool:
    return (
        "/win-analytics-process/" in path
        or "/analytics/lib/" in path
        or "/analytics/runbook/" in path
        or os.path.basename(path) in _PROCESS_OWNING_BASENAMES
    )


def _collect_decisions(records: list[dict]) -> RunDecisions:
    """Observational per-run flags: deliverable artifacts, reviewer counts, and
    process-owning edits at/after the calibration marker (the process-design
    calibration track; noisy on contaminated runs)."""
    d = RunDecisions()
    calib_idx = _marker_indices(records).get("calibration_write")
    for i, rec in enumerate(records):
        for name, tool_input, _tid in _iter_tool_uses(rec):
            if name in ("Agent", "Task"):
                st = tool_input.get("subagent_type")
                if st in REVIEWER_TYPES:
                    d.reviewer_counts[st] = d.reviewer_counts.get(st, 0) + 1
            elif name in ("Write", "Edit"):
                path = str(tool_input.get("file_path", ""))
                base = os.path.basename(path)
                if path.endswith(".ipynb"):
                    d.notebook_writes += 1
                elif base.startswith("build_") and base.endswith(".py"):
                    d.notebook_build_writes += 1
                elif path.endswith(".py") and any(
                    seg in path for seg in ("/ad_hoc/", "/projects/", "/notebooks/")
                ):
                    d.analysis_script_writes += 1
                if (
                    calib_idx is not None
                    and i >= calib_idx
                    and _is_process_owning(path)
                    and not _CALIBRATION_RE.search(base)
                ):
                    d.process_design_edits.append(base)
    return d


@dataclass
class RunProfile:
    path: str
    confidence: str
    stages: dict[str, StageMetrics] = field(default_factory=dict)
    decisions: RunDecisions | None = None


def load_records(path: str) -> list[dict]:
    """Read a transcript JSONL, skipping blank, malformed, or non-object lines."""
    records = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                records.append(obj)
    return records


def _min(seconds: float) -> float:
    return round(seconds / 60.0, 1)


def _pct(part: float, whole: float) -> int:
    return round(100.0 * part / whole) if whole else 0


def format_report(profiles: list[RunProfile]) -> str:
    """Render per-run tables (with a total row + share of model-active) plus a
    combined per-stage mean summary as Markdown."""
    lines = ["# Pipeline run profile", ""]
    header = (
        "| stage | model_active_min | human_idle_min | wall_clock_min | "
        "%_of_active | input_tok | output_tok | cache_tok |"
    )
    sep = "|---|---|---|---|---|---|---|---|"
    for prof in profiles:
        total_active = sum(prof.stages.get(s, StageMetrics()).model_active_seconds for s in STAGES)
        lines += [f"## {os.path.basename(prof.path)} (confidence: {prof.confidence})", "", header, sep]
        tot = StageMetrics()
        tot_cache = 0
        for stage in STAGES:
            m = prof.stages.get(stage, StageMetrics())
            cache = m.cache_creation_input_tokens + m.cache_read_input_tokens
            wall = m.model_active_seconds + m.human_idle_seconds
            lines.append(
                f"| {stage} | {_min(m.model_active_seconds)} | {_min(m.human_idle_seconds)} | "
                f"{_min(wall)} | {_pct(m.model_active_seconds, total_active)} | "
                f"{m.input_tokens} | {m.output_tokens} | {cache} |"
            )
            tot.model_active_seconds += m.model_active_seconds
            tot.human_idle_seconds += m.human_idle_seconds
            tot.input_tokens += m.input_tokens
            tot.output_tokens += m.output_tokens
            tot_cache += cache
        tot_wall = tot.model_active_seconds + tot.human_idle_seconds
        lines.append(
            f"| **total** | {_min(tot.model_active_seconds)} | {_min(tot.human_idle_seconds)} | "
            f"{_min(tot_wall)} | {_pct(tot.model_active_seconds, total_active)} | {tot.input_tokens} | {tot.output_tokens} | {tot_cache} |"
        )
        d = prof.decisions
        if d is not None:
            revs = ", ".join(f"{k}×{v}" for k, v in sorted(d.reviewer_counts.items())) or "none"
            proc = ", ".join(sorted(set(d.process_design_edits))) or "none"
            lines.append(
                f"**Decisions:** deliverable — notebook ×{d.notebook_writes} · "
                f"analysis-script ×{d.analysis_script_writes} · nb-build ×{d.notebook_build_writes}  |  "
                f"reviewers — {revs}  |  process-design edits — {proc}"
            )
        lines.append("")
    # combined summary
    n = len(profiles) or 1
    lines += [
        "## Combined (mean across runs)",
        "",
        "| stage | mean_model_active_min | mean_human_idle_min | mean_wall_clock_min |",
        "|---|---|---|---|",
    ]
    for stage in STAGES:
        active = sum(p.stages.get(stage, StageMetrics()).model_active_seconds for p in profiles) / n
        idle = sum(p.stages.get(stage, StageMetrics()).human_idle_seconds for p in profiles) / n
        lines.append(f"| {stage} | {_min(active)} | {_min(idle)} | {_min(active + idle)} |")
    lines += [
        "",
        "> Note: reviewer internal tokens are not in the transcript (sub-agent sidechains "
        "are not recorded), so review-stage token counts reflect only the parent context. "
        "Review wall-clock is the parallel max-span (dispatch to last reviewer result).",
    ]
    return "\n".join(lines)


def profile_run(path: str) -> RunProfile:
    """Profile one transcript into per-stage token + time metrics."""
    records = load_records(path)
    spans, confidence = segment_stages(records)
    stages = {}
    for stage, (start, end) in spans.items():
        sl = records[start:end]
        tokens = _token_totals(sl)
        active, idle = _split_time(sl)
        stages[stage] = StageMetrics(model_active_seconds=active, human_idle_seconds=idle, **tokens)
    return RunProfile(path=path, confidence=confidence, stages=stages, decisions=_collect_decisions(records))


def _resolve_paths(patterns: list[str]) -> list[str]:
    """Expand each pattern as a glob; pass through literal paths."""
    out = []
    for pat in patterns:
        matches = glob.glob(pat)
        out.extend(sorted(matches) if matches else [pat])
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Profile Win-analytics pipeline runs from transcripts.")
    parser.add_argument("paths", nargs="+", help="Transcript JSONL paths or glob patterns.")
    args = parser.parse_args(argv)
    paths = _resolve_paths(args.paths)
    profiles = [profile_run(p) for p in paths]
    print(format_report(profiles))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
