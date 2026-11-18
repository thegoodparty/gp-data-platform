"""State, cursor and dedupe, including the two rules rule 4 depends on.

The Variable is faked: no test touches Airflow's real Variable, so none of these
can reach the metastore.
"""

from datetime import UTC, datetime, timedelta

from include.pipelines.models import Pipeline, RealSuccess
from include.reader.models import Streak
from include.reader.state import (
    RECENT_IDS_CAP,
    ReaderState,
    apply_collector_payload,
    load_state,
    save_state,
    streak_for,
)

T0 = datetime(2026, 11, 18, 2, 40, tzinfo=UTC)


class _Store(dict):
    """A fake Variable store: get takes a default, set overwrites, like Airflow's."""

    def get(self, key, default_var=None):
        return dict.get(self, key, default_var)

    def set(self, key, value):
        self[key] = value


def _fake_variable(initial: dict[str, str] | None = None):
    store = _Store(initial or {})
    return store.get, store.set


def test_state_round_trips():
    get, set_ = _fake_variable()
    state = ReaderState(
        cursor={"dbt_cloud": T0},
        recent_run_ids={"dbt_cloud": ["1", "2"]},
        streaks={"dbt_nightly_build": Streak(True, T0)},
        last_read_at={"dbt_cloud": T0},
    )
    save_state(set_, state)
    assert load_state(get) == state


def test_a_missing_variable_is_an_empty_state_not_an_error():
    get, _ = _fake_variable()
    state = load_state(get)
    assert state == ReaderState()


def test_an_unreadable_variable_is_an_empty_state():
    """A hand-edited Variable must degrade to a cold start, not to a failed DAG."""
    get, _ = _fake_variable({"data_pipeline_reader_state": "{not json"})
    assert load_state(get) == ReaderState()


def test_cursor_advances_to_finish_time_not_creation_time():
    state = ReaderState()
    state.advance_cursor("dbt_cloud", T0)
    # A long run finishing after a later-created one keeps the cursor at the later finish.
    state.advance_cursor("dbt_cloud", T0 - timedelta(hours=1))
    assert state.cursor["dbt_cloud"] == T0


def test_window_start_is_the_cursor_minus_the_overlap():
    state = ReaderState(cursor={"dbt_cloud": T0})
    assert state.window_start("dbt_cloud", overlap_seconds=3600) == T0 - timedelta(hours=1)
    assert ReaderState().window_start("dbt_cloud") is None


def test_already_seen_run_is_not_resent():
    state = ReaderState()
    state.remember("dbt_cloud", [7])
    assert state.already_seen("dbt_cloud", 7) is True
    assert state.already_seen("dbt_cloud", "7") is True
    assert state.already_seen("dbt_cloud", 8) is False


def test_remember_does_not_duplicate_and_is_capped():
    state = ReaderState()
    state.remember("dbt_cloud", [1, 2, 3])
    state.remember("dbt_cloud", [2, 3, 4])
    assert state.recent_run_ids["dbt_cloud"] == ["1", "2", "3", "4"]
    state.remember("dbt_cloud", list(range(1000, 1000 + RECENT_IDS_CAP + 50)))
    assert len(state.recent_run_ids["dbt_cloud"]) == RECENT_IDS_CAP
    # The cap keeps the newest, which is the end of the window still being read.
    assert state.recent_run_ids["dbt_cloud"][-1] == str(1000 + RECENT_IDS_CAP + 49)


def test_restart_mid_window_produces_no_gap():
    """The cursor is behind, the overlap covers the gap, and the dedupe set means
    the run already sent is not sent twice."""
    state = ReaderState(cursor={"dbt_cloud": T0}, recent_run_ids={"dbt_cloud": ["10"]})
    window = state.window_start("dbt_cloud")
    missed = T0 - timedelta(minutes=20)  # finished before the cursor, never read
    assert window is not None and missed >= window
    assert state.already_seen("dbt_cloud", 10) is True


def test_cursor_does_not_advance_on_a_failed_read():
    """save_state writes the tool's old cursor unchanged when its read failed."""
    get, set_ = _fake_variable()
    state = ReaderState(cursor={"dbt_cloud": T0}, last_read_at={"dbt_cloud": T0})
    # A failed read neither moves the cursor nor the read time.
    state.record_read("dbt_cloud", read_ok=False, now=T0 + timedelta(hours=1))
    save_state(set_, state)
    reloaded = load_state(get)
    assert reloaded.cursor["dbt_cloud"] == T0
    assert reloaded.last_read_at["dbt_cloud"] == T0


def test_last_read_at_frozen_after_three_failed_cycles():
    """Three cycles with a revoked token leave last_read_at at the last good read.
    A refreshed-on-attempt time would mean rule 4 could never fire."""
    get, set_ = _fake_variable()
    state = ReaderState(last_read_at={"dbt_cloud": T0})
    save_state(set_, state)
    for cycle in range(1, 4):
        state = load_state(get)
        state.record_read("dbt_cloud", read_ok=False, now=T0 + timedelta(minutes=5 * cycle))
        save_state(set_, state)
    assert load_state(get).last_read_at["dbt_cloud"] == T0


def test_last_read_at_moves_on_a_good_read():
    state = ReaderState(last_read_at={"dbt_cloud": T0})
    state.record_read("dbt_cloud", read_ok=True, now=T0 + timedelta(minutes=5))
    assert state.last_read_at["dbt_cloud"] == T0 + timedelta(minutes=5)


def test_streak_for_returns_none_so_the_caller_rebuilds():
    """A pipeline with no saved entry must be distinguishable from a saved
    healthy one, or a cold start would report a recovery that never happened."""
    assert streak_for(ReaderState(), "dbt_nightly_build") is None
    saved = ReaderState(streaks={"dbt_nightly_build": Streak(False, T0)})
    assert streak_for(saved, "dbt_nightly_build") == Streak(False, T0)


# --- folding a collector's payload in ---------------------------------------


_P = Pipeline(
    pipeline="dbt_nightly_build",
    tool="dbt_cloud",
    display_name="dbt Cloud nightly production build",
    owner="data-engineering",
    schedule="2 */12 * * *",
    allowed_age_seconds=26 * 3600,
    real_success=RealSuccess(status="run_status_success", trigger="scheduled"),
    runbook="https://example.invalid/runbook",
)


def test_a_good_read_advances_the_cursor_dedupe_streak_and_read_time():
    state = ReaderState()
    payload = {
        "read_ok": True,
        "cursor": T0.isoformat(),
        "records": [{"run_id": 11, "status": "error"}],
        "streak": {"failed_since_last_success": True, "last_real_success_at": None},
    }
    apply_collector_payload(state, _P, payload, T0)
    assert state.cursor["dbt_cloud"] == T0
    assert state.recent_run_ids["dbt_cloud"] == ["11"]
    assert state.streaks["dbt_nightly_build"] == Streak(True, None)
    assert state.last_read_at["dbt_cloud"] == T0


def test_a_failed_read_touches_nothing():
    """The cursor, the dedupe set, the streak and last_read_at all stay put, so the
    next run replays the window and rule 4 can see the staleness."""
    state = ReaderState(
        cursor={"dbt_cloud": T0},
        recent_run_ids={"dbt_cloud": ["1"]},
        streaks={"dbt_nightly_build": Streak(True, T0)},
        last_read_at={"dbt_cloud": T0},
    )
    before = ReaderState(state.cursor, state.recent_run_ids, state.streaks, state.last_read_at)
    apply_collector_payload(state, _P, {"read_ok": False, "cursor": (T0 + timedelta(days=1)).isoformat()}, T0)
    apply_collector_payload(state, _P, None, T0)
    assert state == before


def test_a_failed_read_does_not_advance_through_the_round_trip():
    get, set_ = _fake_variable()
    state = ReaderState(cursor={"dbt_cloud": T0}, last_read_at={"dbt_cloud": T0})
    save_state(set_, state)
    for _ in range(3):
        state = load_state(get)
        failed = {"read_ok": False, "cursor": (T0 + timedelta(days=1)).isoformat()}
        apply_collector_payload(state, _P, failed, T0 + timedelta(minutes=5))
        save_state(set_, state)
    reloaded = load_state(get)
    assert reloaded.cursor["dbt_cloud"] == T0
    assert reloaded.last_read_at["dbt_cloud"] == T0
