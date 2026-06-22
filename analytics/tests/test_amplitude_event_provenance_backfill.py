import os
import subprocess
from datetime import UTC, datetime

import amplitude_event_provenance_backfill as bf
import pytest
from amplitude_event_provenance_backfill import (
    PROVENANCE_COLUMNS,
    build_git_log_argv,
    build_provenance_row,
    build_watermark_merge_sql,
    classify_code_status,
    collect_provenance,
    compile_event_pattern,
    find_events,
    parse_git_log,
    parse_pr_number,
    present_at_head,
    resolve_omni_repo,
    slugify_event,
)

SEP = "\x1f"


def _epoch(date):
    """Synthetic midnight-UTC epoch for a YYYY-MM-DD date, so date order == ts order."""
    return str(int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()))


def _header(full, short, date, subject, ts=None):
    # ts (commit epoch, %at) sits between the short date and the subject; defaults to the
    # date's midnight so existing single-date streams keep their chronological ordering.
    return "\x00" + SEP.join([full, short, date, ts or _epoch(date), subject])


# --------------------------------------------------------------------------- #
# parse_pr_number / classify_code_status
# --------------------------------------------------------------------------- #


def test_parse_pr_number_pulls_squash_merge_suffix():
    assert parse_pr_number("feat: add events (#1234)") == "1234"


def test_parse_pr_number_pulls_merge_commit_form():
    # omni's dominant convention is merge commits, not squash suffixes.
    assert parse_pr_number("Merge pull request #1892 from thegoodparty/feat/briefings") == "1892"


def test_parse_pr_number_none_when_no_suffix():
    assert parse_pr_number("WEB-3658: fullstory events") is None
    assert parse_pr_number("") is None
    assert parse_pr_number(None) is None


def test_classify_code_status_present_when_in_head():
    assert classify_code_status(True, True) == "present"
    assert classify_code_status(True, False) == "present"


def test_classify_code_status_removed_when_absent_with_history():
    assert classify_code_status(False, True) == "removed"


def test_classify_code_status_not_found_when_absent_without_history():
    assert classify_code_status(False, False) == "not_found_in_code"


def test_classify_code_status_unknown_when_git_unavailable():
    assert classify_code_status(None, True) == "unknown"


# --------------------------------------------------------------------------- #
# slugify_event -- punctuation-robust matching key
# --------------------------------------------------------------------------- #


def test_slugify_event_lowercases_and_separates_on_punctuation():
    assert slugify_event("Polls - Poll Results Overview Viewed") == "polls_poll_results_overview_viewed"


def test_slugify_event_apostrophe_variants_collapse_to_same_slug():
    # The whole point: the Amplitude name and the code literal may differ only by
    # an apostrophe; both must yield the same slug so they reconcile downstream.
    with_apos = slugify_event("Onboarding - Office Step: Click Can't See Office")
    without = slugify_event("Onboarding - Office Step: Click Cant See Office")
    assert with_apos == without == "onboarding_office_step_click_cant_see_office"


def test_slugify_event_handles_curly_quotes_and_trailing_punct():
    assert slugify_event("Pro Upgrade - Guidance: Click let’s go!") == "pro_upgrade_guidance_click_lets_go"


def test_slugify_event_already_snake_case_passthrough():
    assert slugify_event("pro_upgrade_complete") == "pro_upgrade_complete"


# --------------------------------------------------------------------------- #
# find_events
# --------------------------------------------------------------------------- #


def test_find_events_matches_map_value_literal():
    pattern = compile_event_pattern(["Polls - Create Poll Clicked", "Polls - Poll Question Completed"])
    line = "  createPollClicked: 'Polls - Create Poll Clicked',"
    assert find_events(line, pattern) == {"Polls - Create Poll Clicked"}


def test_find_events_empty_when_no_known_literal():
    pattern = compile_event_pattern(["Polls - Create Poll Clicked"])
    assert find_events("  const x = doThing()", pattern) == set()


def test_find_events_prefers_longest_to_avoid_substring_shadowing():
    pattern = compile_event_pattern(["Completed", "Pledge Completed"])
    line = "  pledge: 'Pledge Completed',"
    assert find_events(line, pattern) == {"Pledge Completed"}


def test_find_events_matches_double_quoted_and_multiple():
    pattern = compile_event_pattern(["pro_upgrade_complete", "onboarding_complete"])
    line = '  PRO: "pro_upgrade_complete", ONB: "onboarding_complete",'
    assert find_events(line, pattern) == {"pro_upgrade_complete", "onboarding_complete"}


# --------------------------------------------------------------------------- #
# parse_git_log -- the single-pass history walk
# --------------------------------------------------------------------------- #

# Newest-first, as real `git log` emits. Commit 3 reindents Event A (a move:
# it appears on both - and + lines, so it must net to no change).
_STREAM = [
    _header("cccc", "cccc", "2025-04-01", "refactor: reindent (#333)"),
    "diff --git a/x.ts b/x.ts",
    "--- a/x.ts",
    "+++ b/x.ts",
    "@@ -1,3 +1,3 @@",
    "-  a: 'Event A',",
    "+    a: 'Event A',",
    _header("bbbb", "bbbb", "2025-03-01", "feat: tweak (#222)"),
    "diff --git a/x.ts b/x.ts",
    "@@ -1,2 +1,2 @@",
    "-  b: 'Event B',",
    "+  c: 'Event C',",
    _header("aaaa", "aaaa", "2025-02-01", "WEB-1: add events"),
    "diff --git a/x.ts b/x.ts",
    "@@ -0,0 +1,2 @@",
    "+  a: 'Event A',",
    "+  b: 'Event B',",
]


def _parsed():
    return parse_git_log(_STREAM, compile_event_pattern(["Event A", "Event B", "Event C"]))


def test_parse_git_log_instrumented_is_earliest_add():
    acc = _parsed()
    assert acc["Event A"]["instrumented"]["commit"] == "aaaa"
    assert acc["Event A"]["instrumented"]["date"] == "2025-02-01"
    assert acc["Event A"]["instrumented"]["pr"] is None


def test_parse_git_log_retire_is_latest_net_removal():
    acc = _parsed()
    assert acc["Event B"]["instrumented"]["commit"] == "aaaa"
    assert acc["Event B"]["retired"]["commit"] == "bbbb"
    assert acc["Event B"]["retired"]["pr"] == "222"


def test_parse_git_log_reindent_move_is_not_a_change():
    acc = _parsed()
    assert acc["Event A"]["retired"] is None
    assert acc["Event A"]["last_change"]["commit"] == "aaaa"


def test_parse_git_log_event_added_later_has_no_retire():
    acc = _parsed()
    assert acc["Event C"]["instrumented"]["commit"] == "bbbb"
    assert acc["Event C"]["retired"] is None


def test_parse_git_log_omits_events_never_seen():
    acc = parse_git_log(_STREAM, compile_event_pattern(["Event A", "Nonexistent Event"]))
    assert "Nonexistent Event" not in acc


# Two net-changes to the same event on the SAME calendar day, newest-first as git emits.
# The evening commit (re-add) is seen first; the morning commit (original add) is the true
# instrumentation point. Date-only comparison can't tell them apart and keeps the first seen.
_SAME_DAY_STREAM = [
    _header("evening", "evening", "2025-05-01", "feat: re-add the literal (#2)", ts="1714588200"),
    "diff --git a/x.ts b/x.ts",
    "@@ -0,0 +1 @@",
    "+  x: 'Event X',",
    _header("morning", "morning", "2025-05-01", "feat: add the literal (#1)", ts="1714554600"),
    "diff --git a/x.ts b/x.ts",
    "@@ -0,0 +1 @@",
    "+  x: 'Event X',",
]


def test_parse_git_log_same_day_instrumented_breaks_tie_by_commit_time():
    # Earliest commit of the day must win 'instrumented', not the first one in stream order.
    acc = parse_git_log(_SAME_DAY_STREAM, compile_event_pattern(["Event X"]))
    assert acc["Event X"]["instrumented"]["commit"] == "morning"


# --------------------------------------------------------------------------- #
# build_provenance_row
# --------------------------------------------------------------------------- #

UPDATED = "2026-06-17T00:00:00"
COMMIT_A = {
    "commit": "aaaa",
    "short": "aaaa",
    "date": "2025-02-01",
    "subject": "WEB-1: add events",
    "pr": None,
}
COMMIT_B = {
    "commit": "bbbb",
    "short": "bbbb",
    "date": "2025-03-01",
    "subject": "feat: remove (#222)",
    "pr": "222",
}


def test_build_row_present_event_has_instrumented_and_no_retire():
    acc = {"instrumented": COMMIT_A, "retired": None, "last_change": COMMIT_A}
    row = build_provenance_row("Event A", acc, present_in_head=True, updated_at=UPDATED)
    assert row["instrumented_commit"] == "aaaa"
    assert row["instrumented_pr"] is None
    assert row["instrumented_date"] == "2025-02-01"
    assert row["retired_commit"] is None
    assert row["last_code_change_date"] == "2025-02-01"
    assert row["updated_at"] == UPDATED


def test_build_row_includes_slug():
    row = build_provenance_row("Polls - Create Poll Clicked", None, present_in_head=True, updated_at=UPDATED)
    assert row["event_type"] == "Polls - Create Poll Clicked"
    assert row["event_type_slug"] == "polls_create_poll_clicked"


def test_build_row_removed_event_populates_retire():
    acc = {"instrumented": COMMIT_A, "retired": COMMIT_B, "last_change": COMMIT_B}
    row = build_provenance_row("Event B", acc, present_in_head=False, updated_at=UPDATED)
    assert row["retired_commit"] == "bbbb"
    assert row["retired_pr"] == "222"
    assert row["retired_date"] == "2025-03-01"


def test_build_row_not_found_event_is_all_null():
    row = build_provenance_row("Sign Up Clicked", None, present_in_head=False, updated_at=UPDATED)
    assert row["instrumented_commit"] is None
    assert row["retired_commit"] is None
    assert row["last_code_change_date"] is None


def test_build_row_present_but_predates_window_does_not_guess():
    row = build_provenance_row("Old Event", None, present_in_head=True, updated_at=UPDATED)
    assert row["instrumented_commit"] is None
    assert row["last_code_change_date"] is None


def test_build_row_event_type_preserved():
    row = build_provenance_row("Event A", None, present_in_head=None, updated_at=UPDATED)
    assert row["event_type"] == "Event A"


def test_build_row_drops_code_status_and_still_in_code():
    entry = {
        "instrumented": {"commit": "aaaa", "pr": "1", "date": "2025-02-01"},
        "retired": None,
        "last_change": {"commit": "aaaa", "pr": "1", "date": "2025-02-01"},
    }
    row = bf.build_provenance_row("Event A", entry, True, "2026-06-22T00:00:00")
    assert set(row) == set(bf.PROVENANCE_COLUMNS)
    assert "code_status" not in row
    assert "still_in_code" not in row


def test_build_row_removed_event_still_populates_retire_via_internal_status():
    entry = {
        "instrumented": {"commit": "aaaa", "pr": "1", "date": "2025-02-01"},
        "retired": {"commit": "zzzz", "pr": "9", "date": "2026-01-01"},
        "last_change": {"commit": "zzzz", "pr": "9", "date": "2026-01-01"},
    }
    row = bf.build_provenance_row("Event A", entry, False, "2026-06-22T00:00:00")
    assert row["retired_commit"] == "zzzz"
    assert row["retired_date"] == "2026-01-01"


# --------------------------------------------------------------------------- #
# build_git_log_argv / present_at_head / collect_provenance
# --------------------------------------------------------------------------- #


def test_build_git_log_argv_single_pass_with_since():
    argv = build_git_log_argv("/repo", "2024-06-01", ["packages/gp-webapp"])
    assert argv[:4] == ["git", "-C", "/repo", "log"]
    assert "-p" in argv
    assert "--format=%x00%H%x1f%h%x1f%ad%x1f%at%x1f%s" in argv
    assert argv[argv.index("--since") + 1] == "2024-06-01"
    assert argv[argv.index("--") + 1 :] == ["packages/gp-webapp"]


def test_build_git_log_argv_omits_since_when_none():
    argv = build_git_log_argv("/repo", None, ["packages/gp-api"])
    assert "--since" not in argv


def test_build_git_log_argv_walks_the_deploy_ref():
    argv = build_git_log_argv("/repo", "2024-06-01", ["packages/gp-webapp"], ref="origin/develop")
    # the ref must precede the pathspec separator so git treats it as a revision, not a path
    assert "origin/develop" in argv
    assert argv.index("origin/develop") < argv.index("--")


def test_present_at_head_marks_found_literals():
    grep_text = "  a: 'Event A',\n  c: 'Event C',"
    present = present_at_head(["Event A", "Event B", "Event C"], grep_text)
    assert present == {"Event A": True, "Event B": False, "Event C": True}


def test_collect_provenance_one_row_per_event():
    grep_text = "'Event A'\n'Event C'"  # B is gone at HEAD
    rows = collect_provenance(["Event A", "Event B", "Event C"], _STREAM, grep_text, updated_at=UPDATED)
    by_event = {r["event_type"]: r for r in rows}
    assert len(rows) == 3
    # Event A: instrumented set, retired null -> present
    assert by_event["Event A"]["instrumented_commit"] == "aaaa"
    assert by_event["Event A"]["retired_commit"] is None
    # Event B: instrumented set, retired set -> removed
    assert by_event["Event B"]["instrumented_commit"] == "aaaa"
    assert by_event["Event B"]["retired_commit"] == "bbbb"
    # Event C: instrumented set, retired null -> present
    assert by_event["Event C"]["instrumented_commit"] == "bbbb"
    assert by_event["Event C"]["retired_commit"] is None


def test_build_watermark_merge_sql_is_parameterized_and_keys_on_job_name():
    sql = build_watermark_merge_sql("cat.sch.state")
    assert "MERGE INTO cat.sch.state AS t" in sql
    assert "ON t.job_name = s.job_name" in sql
    assert "CAST(? AS BIGINT) AS commit_count" in sql
    assert "CAST(? AS TIMESTAMP) AS last_run_at" in sql
    assert sql.count("?") == 5  # job_name, sha, head_ref, commit_count, last_run_at


# --------------------------------------------------------------------------- #
# resolve_omni_repo
# --------------------------------------------------------------------------- #


def test_resolve_omni_repo_prefers_arg(tmp_path):
    (tmp_path / ".git").mkdir()
    assert resolve_omni_repo(str(tmp_path), {}) == str(tmp_path)


def test_resolve_omni_repo_falls_back_to_env(tmp_path):
    (tmp_path / ".git").mkdir()
    assert resolve_omni_repo(None, {"OMNI_REPO": str(tmp_path)}) == str(tmp_path)


def test_resolve_omni_repo_errors_when_unset():
    import pytest

    with pytest.raises(SystemExit):
        resolve_omni_repo(None, {})


def test_resolve_omni_repo_errors_when_not_a_checkout(tmp_path):
    import pytest

    with pytest.raises(SystemExit):
        resolve_omni_repo(str(tmp_path), {})


# --------------------------------------------------------------------------- #
# run_backfill -- orchestration wiring (git IO stubbed, fake DB cursor)
# --------------------------------------------------------------------------- #


class FakeCursor:
    """Records execute calls; returns the event universe for the taxonomy SELECT."""

    def __init__(self, events):
        self._events = events
        self.executed = []
        self._fetch: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._fetch = [(e,) for e in self._events] if "amplitude_taxonomy_event_type" in sql else []

    def fetchall(self):
        return self._fetch


def test_fetch_event_universe_anchors_on_airbyte_taxonomy_source():
    # The event universe is anchored on the Airbyte-synced Amplitude Govern taxonomy,
    # not the dbt int__ model (repointed 2026-06-22).
    cur = FakeCursor(["Event A", "Event B"])
    events = bf.fetch_event_universe(cur)
    assert events == ["Event A", "Event B"]
    sql = cur.executed[-1][0]
    assert "airbyte_source.amplitude_taxonomy_event_type" in sql
    assert "int__amplitude_event_taxonomy" not in sql


def test_run_backfill_inserts_then_merges_then_watermarks(monkeypatch):
    monkeypatch.setattr(bf, "run_git_log", lambda root, since, paths, ref=None: iter(_STREAM))
    monkeypatch.setattr(
        bf, "git_grep_present_text", lambda root, events, paths, ref="HEAD": "'Event A'\n'Event C'"
    )
    monkeypatch.setattr(bf, "git_head_sha", lambda root, ref="HEAD": "deadbeef")
    monkeypatch.setattr(bf, "git_head_ref", lambda root, ref=None: "origin/develop")
    monkeypatch.setattr(bf, "git_commit_count", lambda root, since, paths, ref="HEAD": 42)

    cur = FakeCursor(["Event A", "Event B", "Event C"])
    rows = bf.run_backfill(cur, "/omni", "2024-06-01", datetime(2026, 6, 17, tzinfo=UTC))

    assert {r["event_type"] for r in rows} == {"Event A", "Event B", "Event C"}
    # rows inserted via a parameterized bulk INSERT (3 events -> 3 tuples in one chunk)
    inserts = [(s, p) for s, p in cur.executed if s.startswith("INSERT INTO")]
    assert len(inserts) == 1
    insert_sql, params = inserts[0]
    assert insert_sql.count("?") == 3 * len(PROVENANCE_COLUMNS)
    assert len(params) == 3 * len(PROVENANCE_COLUMNS)
    # provenance MERGE on event_type, then watermark MERGE on job_name (with 5 bound params)
    merges = [(s, p) for s, p in cur.executed if "MERGE INTO" in s]
    assert any("ON t.event_type = s.event_type" in s for s, _ in merges)
    wm = [(s, p) for s, p in merges if "ON t.job_name = s.job_name" in s]
    assert len(wm) == 1 and wm[0][1] == (bf.JOB_NAME, "deadbeef", "origin/develop", 42, "2026-06-17T00:00:00")
    prov_i = next(i for i, (s, _) in enumerate(cur.executed) if "ON t.event_type = s.event_type" in s)
    wm_i = next(i for i, (s, _) in enumerate(cur.executed) if "ON t.job_name = s.job_name" in s)
    assert wm_i > prov_i


def test_run_backfill_applies_pr_resolver(monkeypatch):
    monkeypatch.setattr(bf, "run_git_log", lambda root, since, paths, ref=None: iter(_STREAM))
    monkeypatch.setattr(
        bf, "git_grep_present_text", lambda root, events, paths, ref="HEAD": "'Event A'\n'Event C'"
    )
    monkeypatch.setattr(bf, "git_head_sha", lambda root, ref="HEAD": "deadbeef")
    monkeypatch.setattr(bf, "git_head_ref", lambda root, ref=None: "origin/develop")
    monkeypatch.setattr(bf, "git_commit_count", lambda root, since, paths, ref="HEAD": 42)

    cur = FakeCursor(["Event A", "Event B", "Event C"])
    # Event A is instrumented at "aaaa" whose subject "WEB-1: add events" has no PR ref;
    # the merge-walk resolver backfills instrumented_pr from the introducing merge commit.
    rows = bf.run_backfill(
        cur,
        "/omni",
        "2024-06-01",
        datetime(2026, 6, 17, tzinfo=UTC),
        pr_resolver=lambda sha: "4321" if sha == "aaaa" else None,
    )
    row_a = next(r for r in rows if r["event_type"] == "Event A")
    assert row_a["instrumented_pr"] == "4321"


# --------------------------------------------------------------------------- #
# Git-native PR resolution -- merge-walk fills *_pr gaps parse_pr_number cannot
# --------------------------------------------------------------------------- #


def test_pick_introducing_merge_takes_oldest_merge_on_ancestry_path():
    # rev-list emits newest-first; the merge that *introduced* a commit is the oldest
    # on the ancestry path to the deploy ref, i.e. the last line.
    rev_list = "newmerge111\noldmerge222\n"
    assert bf._pick_introducing_merge(rev_list) == "oldmerge222"


def test_pick_introducing_merge_none_when_no_merge():
    assert bf._pick_introducing_merge("") is None
    assert bf._pick_introducing_merge("\n") is None


def test_make_merge_walk_resolver_delegates_to_git_merge_pr(monkeypatch):
    monkeypatch.setattr(bf, "git_merge_pr", lambda root, sha, ref: f"{root}:{sha}:{ref}")
    resolver = bf.make_merge_walk_resolver("/omni", "origin/develop")
    assert resolver("abc123") == "/omni:abc123:origin/develop"


def test_git_merge_pr_resolves_pr_from_real_merge_commit(tmp_path):
    # Build a tiny repo: main -> feature -> non-ff merge with omni's merge-commit subject,
    # then confirm git_merge_pr recovers the PR number for the feature commit.
    import subprocess

    repo = str(tmp_path)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@x",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@x",
    }

    def git(*args):
        subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, env=env)

    git("init", "-q", "-b", "main")
    (tmp_path / "f.txt").write_text("base\n")
    git("add", ".")
    git("commit", "-q", "-m", "base")
    git("checkout", "-q", "-b", "feature")
    (tmp_path / "f.txt").write_text("base\nevent\n")
    git("add", ".")
    git("commit", "-q", "-m", "feat: add an event literal")
    feature_sha = subprocess.run(
        ["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    git("checkout", "-q", "main")
    git("merge", "--no-ff", "-m", "Merge pull request #42 from thegoodparty/feature", "feature")

    assert bf.git_merge_pr(repo, feature_sha, "main") == "42"


def test_git_merge_pr_none_for_commit_with_no_merge(tmp_path):
    import subprocess

    repo = str(tmp_path)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@x",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@x",
    }

    def git(*args):
        subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, env=env)

    git("init", "-q", "-b", "main")
    (tmp_path / "f.txt").write_text("base\n")
    git("add", ".")
    git("commit", "-q", "-m", "base committed straight to main")
    head = subprocess.run(
        ["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()

    assert bf.git_merge_pr(repo, head, "main") is None


def _init_repo_one_commit(tmp_path):
    """A minimal repo with a single commit; returns its path."""
    repo = str(tmp_path)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@x",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@x",
    }
    subprocess.run(["git", "-C", repo, "init", "-q", "-b", "main"], check=True, env=env)
    (tmp_path / "f.txt").write_text("base\n")
    subprocess.run(["git", "-C", repo, "add", "."], check=True, capture_output=True, env=env)
    subprocess.run(
        ["git", "-C", repo, "commit", "-q", "-m", "base"], check=True, capture_output=True, env=env
    )
    return repo


def test_run_git_log_raises_on_nonzero_exit(tmp_path):
    # A bad ref makes `git log` exit non-zero; the stream must surface that, not yield nothing.
    repo = _init_repo_one_commit(tmp_path)
    with pytest.raises(subprocess.CalledProcessError):
        list(bf.run_git_log(repo, None, ["f.txt"], ref="no-such-ref-abc123"))


def test_run_git_log_streams_lines_on_success(tmp_path):
    # The happy path still yields the log stream line-by-line.
    repo = _init_repo_one_commit(tmp_path)
    lines = list(bf.run_git_log(repo, None, ["f.txt"]))
    assert any("base" in line for line in lines)


def test_git_fetch_raises_on_failure(tmp_path):
    # No 'origin' remote configured -> `git fetch origin develop` fails; must abort, not continue.
    repo = _init_repo_one_commit(tmp_path)
    with pytest.raises(SystemExit):
        bf.git_fetch(repo, "origin/develop")


def test_resolve_pr_gaps_fills_null_instrumented_pr():
    rows = [
        {"instrumented_commit": "aaaa", "instrumented_pr": None, "retired_commit": None, "retired_pr": None}
    ]
    out, filled = bf.resolve_pr_gaps(rows, lambda sha: "777" if sha == "aaaa" else None)
    assert out[0]["instrumented_pr"] == "777"
    assert filled == 1


def test_resolve_pr_gaps_does_not_overwrite_existing_pr():
    rows = [
        {"instrumented_commit": "aaaa", "instrumented_pr": "111", "retired_commit": None, "retired_pr": None}
    ]
    out, filled = bf.resolve_pr_gaps(rows, lambda sha: "999")
    assert out[0]["instrumented_pr"] == "111"
    assert filled == 0


def test_resolve_pr_gaps_resolves_retired_commit_too():
    rows = [
        {
            "instrumented_commit": "aaaa",
            "instrumented_pr": "111",
            "retired_commit": "bbbb",
            "retired_pr": None,
        }
    ]
    out, filled = bf.resolve_pr_gaps(rows, lambda sha: "888")
    assert out[0]["retired_pr"] == "888"
    assert filled == 1


def test_resolve_pr_gaps_resolves_each_distinct_sha_once():
    calls: list[str] = []

    def resolver(sha):
        calls.append(sha)
        return "5"

    rows = [
        {
            "instrumented_commit": "same",
            "instrumented_pr": None,
            "retired_commit": "same",
            "retired_pr": None,
        },
        {"instrumented_commit": "same", "instrumented_pr": None, "retired_commit": None, "retired_pr": None},
    ]
    bf.resolve_pr_gaps(rows, resolver)
    assert calls == ["same"]  # 3 references, one lookup


def test_resolve_pr_gaps_noop_when_resolver_none():
    rows = [
        {"instrumented_commit": "aaaa", "instrumented_pr": None, "retired_commit": None, "retired_pr": None}
    ]
    out, filled = bf.resolve_pr_gaps(rows, None)
    assert out[0]["instrumented_pr"] is None
    assert filled == 0


def test_resolve_pr_gaps_skips_rows_without_commit():
    rows = [
        {"instrumented_commit": None, "instrumented_pr": None, "retired_commit": None, "retired_pr": None}
    ]
    _, filled = bf.resolve_pr_gaps(rows, lambda sha: "1")
    assert filled == 0


# --------------------------------------------------------------------------- #
# read_watermark / refresh
# --------------------------------------------------------------------------- #


class RefreshCursor:
    """Fake cursor for refresh wiring: serves the event universe, the watermark
    row, and existing provenance rows depending on the SQL it is handed."""

    def __init__(self, events, watermark_row=None, existing_rows=None):
        self._events = events
        self._watermark_row = watermark_row  # 4-tuple or None
        self._existing = list(existing_rows or [])  # tuples in PROVENANCE_COLUMNS order
        self.executed = []
        self._fetch: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "amplitude_taxonomy_event_type" in sql:
            self._fetch = [(e,) for e in self._events]
        elif sql.startswith("SELECT last_processed_sha"):
            self._fetch = [self._watermark_row] if self._watermark_row else []
        elif sql.startswith("SELECT event_type,"):
            self._fetch = list(self._existing)
        else:
            self._fetch = []

    def fetchall(self):
        return self._fetch


def test_read_watermark_returns_row_as_dict():
    wm = ("oldsha", "origin/develop", 42, "2026-06-01T00:00:00")
    cur = RefreshCursor(["Event A"], watermark_row=wm)
    result = bf.read_watermark(cur)
    assert result == {
        "last_processed_sha": "oldsha",
        "head_ref": "origin/develop",
        "commit_count": 42,
        "last_run_at": "2026-06-01T00:00:00",
    }


def test_read_watermark_none_when_no_row():
    cur = RefreshCursor(["Event A"], watermark_row=None)
    assert bf.read_watermark(cur) is None


# --------------------------------------------------------------------------- #
# merge_provenance_entry -- existing row + new-window observation
# --------------------------------------------------------------------------- #

# A window commit that net-removes an event (newer than any watermarked commit).
COMMIT_E = {
    "commit": "eeee",
    "short": "eeee",
    "date": "2026-06-18",
    "ts": "1781827200",
    "subject": "feat: remove (#999)",
    "pr": "999",
}

_EXISTING_B = {
    "event_type": "Event B",
    "event_type_slug": "event_b",
    "instrumented_commit": "aaaa",
    "instrumented_pr": None,
    "instrumented_date": "2025-02-01",
    "retired_commit": None,
    "retired_pr": None,
    "retired_date": None,
    "last_code_change_date": "2025-02-01",
    "updated_at": "2026-01-01T00:00:00",
}


def test_merge_keeps_existing_instrumented_when_window_only_removes():
    new_entry = {"instrumented": None, "retired": COMMIT_E, "last_change": COMMIT_E}
    merged = bf.merge_provenance_entry(_EXISTING_B, new_entry)
    assert merged["instrumented"]["commit"] == "aaaa"
    assert merged["instrumented"]["date"] == "2025-02-01"
    assert merged["retired"]["commit"] == "eeee"
    assert merged["last_change"]["commit"] == "eeee"


def test_merge_new_event_takes_window_instrumented():
    new_entry = {"instrumented": COMMIT_E, "retired": None, "last_change": COMMIT_E}
    merged = bf.merge_provenance_entry(None, new_entry)
    assert merged["instrumented"]["commit"] == "eeee"
    assert merged["retired"] is None


def test_merge_readd_preserves_original_instrumented_and_advances_last_change():
    existing = {
        **_EXISTING_B,
        "retired_commit": "bbbb",
        "retired_pr": "222",
        "retired_date": "2025-03-01",
    }
    new_entry = {"instrumented": COMMIT_E, "retired": None, "last_change": COMMIT_E}
    merged = bf.merge_provenance_entry(existing, new_entry)
    assert merged["instrumented"]["commit"] == "aaaa"  # original instrumentation kept
    assert merged["last_change"]["commit"] == "eeee"  # window is the latest change
    assert merged["retired"]["commit"] == "bbbb"  # carried forward from existing row


# --------------------------------------------------------------------------- #
# run_refresh -- orchestration wiring
# --------------------------------------------------------------------------- #

# A window stream that net-removes only Event B (newer than the watermark).
_REFRESH_STREAM = [
    _header("eeee", "eeee", "2026-06-18", "feat: remove Event B (#999)"),
    "diff --git a/x.ts b/x.ts",
    "--- a/x.ts",
    "+++ b/x.ts",
    "@@ -1,2 +1,1 @@",
    "-  b: 'Event B',",
]

_EXISTING_ROWS = [
    (
        "Event A",
        "event_a",
        "aaaa",
        None,
        "2025-02-01",
        None,
        None,
        None,
        "2025-02-01",
        "2026-01-01T00:00:00",
    ),
    (
        "Event B",
        "event_b",
        "aaaa",
        None,
        "2025-02-01",
        None,
        None,
        None,
        "2025-02-01",
        "2026-01-01T00:00:00",
    ),
]


def test_run_refresh_falls_back_to_full_backfill_when_no_watermark(monkeypatch):
    called = {}

    def fake_backfill(cursor, root, since, now, **kwargs):
        called["since"] = since
        return [{"event_type": "Event A"}]

    monkeypatch.setattr(bf, "run_backfill", fake_backfill)
    cur = RefreshCursor(["Event A", "Event B"], watermark_row=None)
    rows = bf.run_refresh(cur, "/omni", "2024-06-01", datetime(2026, 6, 18, tzinfo=UTC))
    assert called["since"] == "2024-06-01"
    assert rows == [{"event_type": "Event A"}]


def test_run_refresh_walks_bounded_range_from_watermark(monkeypatch):
    captured = {}

    def fake_git_log(root, since, paths, ref=None):
        captured["ref"] = ref
        captured["since"] = since
        return iter(_REFRESH_STREAM)

    monkeypatch.setattr(bf, "run_git_log", fake_git_log)
    monkeypatch.setattr(bf, "git_grep_present_text", lambda root, events, paths, ref="HEAD": "")
    monkeypatch.setattr(bf, "git_head_sha", lambda root, ref="HEAD": "newhead")
    monkeypatch.setattr(bf, "git_head_ref", lambda root, ref=None: "origin/main")
    monkeypatch.setattr(bf, "git_commit_count", lambda root, since, paths, ref="HEAD": 50)

    wm = ("oldsha", "origin/main", 42, "2026-06-01T00:00:00")
    cur = RefreshCursor(["Event A", "Event B"], watermark_row=wm, existing_rows=_EXISTING_ROWS)
    bf.run_refresh(cur, "/omni", "2024-06-01", datetime(2026, 6, 18, tzinfo=UTC), ref="origin/main")
    assert captured["ref"] == "oldsha..origin/main"
    assert captured["since"] is None  # the range supersedes --since


def test_run_refresh_merges_only_affected_events_and_advances_watermark(monkeypatch):
    monkeypatch.setattr(bf, "run_git_log", lambda root, since, paths, ref=None: iter(_REFRESH_STREAM))
    # Event B absent at HEAD -> grep empty -> code_status removed
    monkeypatch.setattr(bf, "git_grep_present_text", lambda root, events, paths, ref="HEAD": "")
    monkeypatch.setattr(bf, "git_head_sha", lambda root, ref="HEAD": "newhead")
    monkeypatch.setattr(bf, "git_head_ref", lambda root, ref=None: "origin/develop")
    monkeypatch.setattr(bf, "git_commit_count", lambda root, since, paths, ref="HEAD": 50)

    wm = ("oldsha", "origin/develop", 42, "2026-06-01T00:00:00")
    cur = RefreshCursor(["Event A", "Event B"], watermark_row=wm, existing_rows=_EXISTING_ROWS)
    rows = bf.run_refresh(cur, "/omni", "2024-06-01", datetime(2026, 6, 18, tzinfo=UTC))

    # only Event B is rewritten, and its existing instrumentation is preserved
    assert [r["event_type"] for r in rows] == ["Event B"]
    assert rows[0]["instrumented_commit"] == "aaaa"
    assert rows[0]["retired_commit"] == "eeee"

    wm_merges = [(s, p) for s, p in cur.executed if "ON t.job_name = s.job_name" in s]
    assert len(wm_merges) == 1
    assert wm_merges[0][1] == (bf.JOB_NAME, "newhead", "origin/develop", 50, "2026-06-18T00:00:00")


def test_run_refresh_advances_watermark_when_nothing_changed(monkeypatch):
    monkeypatch.setattr(bf, "run_git_log", lambda root, since, paths, ref=None: iter([]))
    monkeypatch.setattr(bf, "git_grep_present_text", lambda root, events, paths, ref="HEAD": "")
    monkeypatch.setattr(bf, "git_head_sha", lambda root, ref="HEAD": "newhead")
    monkeypatch.setattr(bf, "git_head_ref", lambda root, ref=None: "origin/develop")
    monkeypatch.setattr(bf, "git_commit_count", lambda root, since, paths, ref="HEAD": 50)

    wm = ("oldsha", "origin/develop", 42, "2026-06-01T00:00:00")
    cur = RefreshCursor(["Event A", "Event B"], watermark_row=wm, existing_rows=_EXISTING_ROWS)
    rows = bf.run_refresh(cur, "/omni", "2024-06-01", datetime(2026, 6, 18, tzinfo=UTC))

    assert rows == []
    assert not [s for s, _ in cur.executed if s.startswith("INSERT INTO")]  # no provenance write
    assert any("ON t.job_name = s.job_name" in s for s, _ in cur.executed)  # watermark still advanced


# --------------------------------------------------------------------------- #
# CSV persistence
# --------------------------------------------------------------------------- #


def _row(event_type, **over):
    base = {c: None for c in bf.PROVENANCE_COLUMNS}
    base["event_type"] = event_type
    base["event_type_slug"] = bf.slugify_event(event_type)
    base["updated_at"] = "2026-06-22T00:00:00"
    base.update(over)
    return base


def test_write_provenance_writes_sorted_header_and_rows(tmp_path):
    csv_path = tmp_path / "prov.csv"
    rows = [_row("Event B"), _row("Event A")]
    bf.write_provenance(rows, str(csv_path))
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == ",".join(bf.PROVENANCE_COLUMNS)
    assert lines[1].startswith("Event A,")
    assert lines[2].startswith("Event B,")


def test_write_provenance_renders_none_as_empty_field(tmp_path):
    csv_path = tmp_path / "prov.csv"
    bf.write_provenance([_row("Event A")], str(csv_path))
    back = bf.read_provenance_rows(str(csv_path))
    assert back["Event A"]["retired_commit"] is None


def test_provenance_csv_round_trips(tmp_path):
    csv_path = tmp_path / "prov.csv"
    rows = [
        _row("Event A", instrumented_commit="aaaa", instrumented_date="2025-02-01"),
        _row("Click Can't See Office", instrumented_commit="bbbb", instrumented_pr="7"),
    ]
    bf.write_provenance(rows, str(csv_path))
    back = bf.read_provenance_rows(str(csv_path))
    assert back["Event A"]["instrumented_commit"] == "aaaa"
    assert back["Click Can't See Office"]["instrumented_pr"] == "7"
    assert set(back) == {"Event A", "Click Can't See Office"}


def test_read_provenance_rows_empty_when_file_absent(tmp_path):
    assert bf.read_provenance_rows(str(tmp_path / "missing.csv")) == {}


# --------------------------------------------------------------------------- #
# parse_args --refresh flag
# --------------------------------------------------------------------------- #


def test_parse_args_refresh_flag_defaults_false_and_sets_true():
    assert bf.parse_args([]).refresh is False
    assert bf.parse_args(["--refresh"]).refresh is True
