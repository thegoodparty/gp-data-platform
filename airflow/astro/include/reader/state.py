"""The reader's cross-run state: one cursor per tool, a bounded dedupe set, and the
per-pipeline streak.

It lives in a single Airflow Variable as JSON. A Variable and not a file because
Astro gives a task no durable local disk between runs, and a Variable is already
how this repo passes small state and configuration. Nothing secret goes in it.

Two rules the rest of the reader depends on:

- `last_read_at` is carried forward, not refreshed, by a failed read. Rule 4 reads
  it, so a refreshed-on-attempt time would mean a revoked credential never fires.
- A failed tool's cursor does not move, so the next run replays the window.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from include.pipelines.models import Pipeline
from include.reader.models import Streak

STATE_VARIABLE = "data_pipeline_reader_state"

# Per tool. It only has to cover one window overlap, and an unbounded list in a
# Variable grows until the Variable is too large to write.
RECENT_IDS_CAP = 200

# How far back a window reaches past the cursor. A run finishing just inside the
# previous window can still be picked up, so a restart mid-window has no gap.
DEFAULT_OVERLAP = 3600


class VariableAccessor(Protocol):
    def __call__(self, key: str, default_var: Any = None) -> Any: ...


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _format(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


class ReaderState:
    """Everything that survives a reader run."""

    def __init__(
        self,
        cursor: dict[str, datetime] | None = None,
        recent_run_ids: dict[str, list[str]] | None = None,
        streaks: dict[str, Streak] | None = None,
        last_read_at: dict[str, datetime] | None = None,
    ):
        self.cursor = dict(cursor or {})
        self.recent_run_ids = {k: list(v) for k, v in (recent_run_ids or {}).items()}
        self.streaks = dict(streaks or {})
        self.last_read_at = dict(last_read_at or {})

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ReaderState):
            return NotImplemented
        return (
            self.cursor == other.cursor
            and self.recent_run_ids == other.recent_run_ids
            and self.streaks == other.streaks
            and self.last_read_at == other.last_read_at
        )

    def __repr__(self) -> str:
        return (
            f"ReaderState(cursor={sorted(self.cursor)}, recent={ {k: len(v) for k, v in self.recent_run_ids.items()} }, "
            f"streaks={sorted(self.streaks)}, last_read_at={sorted(self.last_read_at)})"
        )

    # --- cursor and dedupe ------------------------------------------------

    def window_start(self, tool: str, overlap_seconds: int = DEFAULT_OVERLAP) -> datetime | None:
        """Where to start reading: the cursor, minus the overlap."""
        cursor = self.cursor.get(tool)
        if cursor is None:
            return None
        return cursor - timedelta(seconds=overlap_seconds)

    def already_seen(self, tool: str, run_id: int | str) -> bool:
        return str(run_id) in self.recent_run_ids.get(tool, [])

    def remember(self, tool: str, run_ids: list[int | str]) -> None:
        """Add to the dedupe set, newest last, capped."""
        existing = self.recent_run_ids.get(tool, [])
        merged = existing + [str(r) for r in run_ids if str(r) not in existing]
        self.recent_run_ids[tool] = merged[-RECENT_IDS_CAP:]

    def advance_cursor(self, tool: str, finished_at: datetime | None) -> None:
        """Advance on finish time, never backwards: two runs finishing out of order
        must not move the cursor back over a window already read."""
        if finished_at is None:
            return
        current = self.cursor.get(tool)
        if current is None or finished_at > current:
            self.cursor[tool] = finished_at

    def record_read(self, tool: str, read_ok: bool, now: datetime) -> None:
        """Move `last_read_at` only on a full successful read; otherwise carry it forward."""
        if read_ok:
            self.last_read_at[tool] = now

    # --- serialisation ----------------------------------------------------

    def to_json(self) -> str:
        return json.dumps(
            {
                "cursor": {k: _format(v) for k, v in self.cursor.items()},
                "recent_run_ids": self.recent_run_ids,
                "streaks": {
                    name: {
                        "failed_since_last_success": streak.failed_since_last_success,
                        "last_real_success_at": _format(streak.last_real_success_at),
                    }
                    for name, streak in self.streaks.items()
                },
                "last_read_at": {k: _format(v) for k, v in self.last_read_at.items()},
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str | None) -> "ReaderState":
        """A missing, empty or unreadable Variable is an empty state: a cold start,
        which the caller rebuilds from the tool's own history."""
        if not text:
            return cls()
        try:
            document = json.loads(text)
        except json.JSONDecodeError:
            return cls()
        if not isinstance(document, dict):
            return cls()
        return cls(
            cursor={
                k: v for k, v in ((k, _parse(v)) for k, v in (document.get("cursor") or {}).items()) if v
            },
            recent_run_ids={
                k: [str(i) for i in v] for k, v in (document.get("recent_run_ids") or {}).items()
            },
            streaks={
                name: Streak(
                    failed_since_last_success=bool(body.get("failed_since_last_success")),
                    last_real_success_at=_parse(body.get("last_real_success_at")),
                )
                for name, body in (document.get("streaks") or {}).items()
            },
            last_read_at={
                k: v
                for k, v in ((k, _parse(v)) for k, v in (document.get("last_read_at") or {}).items())
                if v
            },
        )


def load_state(variable_get: VariableAccessor) -> ReaderState:
    """Read the Variable. Never raises for a missing or corrupt value."""
    try:
        raw = variable_get(STATE_VARIABLE, None)
    except KeyError:
        # Airflow's Variable.get raises KeyError for a missing key when no default
        # is honoured; a cold start is normal, not an error.
        return ReaderState()
    return ReaderState.from_json(raw)


def save_state(variable_set: VariableAccessor, state: ReaderState) -> None:
    variable_set(STATE_VARIABLE, state.to_json())


def streak_for(state: ReaderState, pipeline: str) -> Streak | None:
    """The saved streak, or None so the caller rebuilds it from history."""
    return state.streaks.get(pipeline)


def apply_collector_payload(
    state: ReaderState, pipeline: "Pipeline", payload: dict[str, Any] | None, now: datetime
) -> None:
    """Fold one collector's payload for one pipeline into the state.

    Only a pipeline whose read succeeded advances anything. A failed read leaves the
    cursor and `last_read_at` exactly where they were, so the next run replays the
    window and rule 4 sees the staleness rather than a refreshed attempt.
    """
    if not payload or not payload.get("read_ok"):
        return
    tool = pipeline.tool
    state.advance_cursor(tool, _parse(payload.get("cursor")))
    state.remember(tool, [r["run_id"] for r in payload.get("records") or [] if "run_id" in r])
    streak = payload.get("streak")
    if streak:
        state.streaks[pipeline.pipeline] = Streak(
            failed_since_last_success=bool(streak.get("failed_since_last_success")),
            last_real_success_at=_parse(streak.get("last_real_success_at")),
        )
    state.record_read(tool, read_ok=True, now=now)
