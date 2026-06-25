"""Build and maintain the git-provenance dataset for Amplitude events (DATA-2014).

Walks the omni git history over the instrumentation paths and writes one provenance row per
Amplitude ``event_type`` to a CSV committed in this repo
(``analytics/data/amplitude_event_provenance.csv``), plus a sidecar JSON watermark
(``..._state.json``) recording the last processed commit SHA. With no watermark it does a full
backfill; with one it walks ``git log <lastSHA>..origin/develop``, updates the events that
changed, carries the rest forward, onboards any universe events absent from the CSV via a
full-history pickaxe walk, and advances the watermark.

It READS the omni working tree's git history (``--repo`` / ``OMNI_REPO``) and the Amplitude
Govern event universe from Databricks (``airbyte_source.amplitude_taxonomy_event_type``, the one
Databricks read), and WRITES the CSV + JSON into this repo. It never writes back to Amplitude.

Extraction note: ``trackEvent(...)`` in omni is called with *constant references*, so we anchor
on the authoritative event universe and match those literals against added/removed diff lines in
a single ``git log -p`` pass. Deploy-ref anchored (``origin/develop``, fetched first). PR
attribution is pure git via the merge-commit ancestry walk.

Usage::

    # incremental refresh (full backfill on first run, when no state file exists):
    cd analytics && uv run python lib/amplitude_event_provenance_backfill.py \\
        --repo ~/Documents/0_goodparty/0_repos/omni
    # custom output locations (e.g. for a dry run):
    cd analytics && uv run python lib/amplitude_event_provenance_backfill.py \\
        --repo ~/Documents/0_goodparty/0_repos/omni --csv /tmp/prov.csv --state /tmp/state.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Source roots walked for instrumentation, with test files and seed-data CSVs excluded.
# Test files are excluded so a test-only reference to an event (e.g.
# expect(track).toHaveBeenCalledWith('E'), a call-arg context match) does not register
# as an instrumentation change. Seed-data CSV rows (e.g. ``"Page","Page",...`` for the
# city named Page) are line-leading quoted literals and would otherwise be mistaken for
# instrumentation by the widened matcher; event instrumentation never lives in .csv, so
# excluding them is safe and surgical. git log and git grep both honor :(exclude) magic
# pathspecs.
INSTRUMENTATION_PATHS = [
    "packages/gp-webapp",
    "packages/gp-api",
    ":(exclude,glob)packages/**/*.test.*",
    ":(exclude,glob)packages/**/*.spec.*",
    ":(exclude,glob)packages/**/__tests__/**",
    ":(exclude,glob)packages/**/*.csv",
]

# Events came online in Amplitude ~2025-05; the EVENTS map + segment wiring landed
# ~2025-02. Bounding the walk here skips the large pre-2024 scaffold-era diffs while
# keeping an 8-month margin before real instrumentation. None walks full history.
DEFAULT_SINCE = "2024-06-01"

# Deployed default branch we attribute against. Provenance must reflect what shipped, so
# the walk, the HEAD-presence grep, and the merge-walk all target this ref (fetched first),
# not whatever branch the local omni checkout happens to be on.
DEPLOY_REF = "origin/develop"

DATABRICKS_CATALOG = "goodparty_data_catalog"
# Event universe: Amplitude Govern taxonomy synced directly via Airbyte (~434 events, all
# is_active). The one Databricks read this pipeline still makes.
TAXONOMY_TABLE = f"{DATABRICKS_CATALOG}.airbyte_source.amplitude_taxonomy_event_type"

JOB_NAME = "amplitude_event_provenance_backfill"

# Committed outputs. Resolved from this file so paths are cwd-independent:
# analytics/lib/<this file> -> analytics/data/.
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_CSV_PATH = str(_DATA_DIR / "amplitude_event_provenance.csv")
DEFAULT_STATE_PATH = str(_DATA_DIR / "amplitude_event_provenance_state.json")


# --------------------------------------------------------------------------- #
# Pure helpers (self-contained; mirror the DATA-1952 monitor's git correlation)
# --------------------------------------------------------------------------- #


def parse_pr_number(subject: str | None) -> str | None:
    """Pull a PR number from a commit subject, else None.

    Handles both conventions: omni's dominant merge-commit form
    ("Merge pull request #1892 from ...") and the squash suffix ("... (#1234)").
    """
    text = subject or ""
    match = re.search(r"Merge pull request #(\d+)", text) or re.search(r"\(#(\d+)\)", text)
    return match.group(1) if match else None


def classify_code_status(present_in_head: bool | None, has_history: bool) -> str:
    """Map (grep-at-HEAD result, whether the literal has any git history) to a status."""
    if present_in_head is None:
        return "unknown"  # git unavailable / errored
    if present_in_head:
        return "present"  # instrumentation still in the codebase
    if has_history:
        return "removed"  # was there, now gone -> deprecated/renamed in code
    return "not_found_in_code"  # literal never appeared in the instrumentation paths


# Quote-family chars dropped (not separated) when slugging, so "Can't" -> "cant".
_QUOTE_CHARS = "'`’‘“”′″\""


def slugify_event(name: str) -> str:
    """Punctuation-robust key: lowercase, drop quotes/apostrophes, other non-alnum -> '_'.

    Amplitude's stored ``event_type`` and the code literal sometimes differ only by an
    apostrophe (e.g. "Click Can't See Office" vs "Click Cant See Office"); both slug to
    the same value so downstream can join on the slug without punctuation drift. This is
    a convenience join key alongside the verbatim ``event_type`` -- not the walk's match
    key, which stays exact.
    """
    stripped = "".join(c for c in name.lower() if c not in _QUOTE_CHARS)
    return re.sub(r"[^a-z0-9]+", "_", stripped).strip("_")


# --------------------------------------------------------------------------- #
# Event-literal extraction -- anchored on the known event universe
# --------------------------------------------------------------------------- #


# An event literal counts as instrumentation when it is a quoted string that is either
# (a) in a map-value / call-argument position -- immediately preceded by ``:`` or ``(``
# (``Key: 'Event'``, ``track('Event')``) -- or (b) line-leading (only whitespace before the
# opening quote). Case (b) is required because Prettier wraps long declarations so the literal
# sits on its own line (``Key:\n  'Event'``, ``track(\n  userId,\n  'Event',``); the ``:``/``(``
# is then on the previous line, invisible to a per-line match. ``re.MULTILINE`` makes ``^``
# match each line start in the multi-line git-grep dump ``present_at_head`` passes in.
# The bare-token false positives (Playwright ``page``/``Page``, testing-library ``screen``,
# ARIA values, route arrays, comment prose) are never a line-leading quoted literal, so they
# stay excluded. Seed-data CSV rows (``"Page","Page",...``) ARE line-leading quoted literals;
# they are excluded at the path level (see INSTRUMENTATION_PATHS), not here.
_INSTRUMENTATION_PREFIX = r"(?:[:(]\s*|^\s*)"
_QUOTE_CLASS = "['\"`]"  # straight single, double, backtick
_INQUOTE_PAD = r"[ \t]*"  # tolerate stray spaces/tabs inside the quotes from source typos


def compile_event_pattern(events: Iterable[str]) -> re.Pattern[str]:
    """Compile one alternation regex over the known event_type literals.

    Sorted longest-first so a longer event name wins over a shorter one it
    contains (regex alternation is ordered + non-overlapping left-to-right),
    which stops a short literal from being double-counted inside a longer one.

    Each alternate must appear as a quoted string in an instrumentation context:
    either a map-value / call-argument position (immediately preceded by ``:`` or
    ``(``) or a line-leading position (only whitespace before the opening quote).
    The line-leading case is needed because Prettier wraps long declarations so the
    literal sits on its own line with the ``:``/``(`` on the previous line.
    ``re.MULTILINE`` makes ``^`` match each line start in multi-line git-grep dumps.

    The capture group wraps only the alternation (not the padding), so the returned
    name is always the trimmed taxonomy name -- not the typo'd source literal. The
    ``_INQUOTE_PAD`` (``[ \\t]*``) around the capture group tolerates stray leading/
    trailing horizontal whitespace inside the quotes from source typos (e.g.
    ``analyticsHelper.ts``: ``'Pro Upgrade - Committee Check Page: Click Upload '``
    with a trailing space). ``[ \\t]*`` (horizontal only) is used instead of ``\\s*``
    so it never spans newlines and matches across lines in a multi-line git-grep dump.
    """
    ordered = sorted(set(events), key=len, reverse=True)
    alternation = "|".join(re.escape(e) for e in ordered)
    return re.compile(
        rf"{_INSTRUMENTATION_PREFIX}{_QUOTE_CLASS}{_INQUOTE_PAD}({alternation}){_INQUOTE_PAD}{_QUOTE_CLASS}",
        re.MULTILINE,
    )


def find_events(text: str, pattern: re.Pattern[str]) -> set[str]:
    """Set of known event literals appearing in ``text`` (a diff line or grep dump)."""
    return set(pattern.findall(text))


# --------------------------------------------------------------------------- #
# Single-pass history walk
# --------------------------------------------------------------------------- #

# Header line emitted by: git log -p --date=short
#   --format='%x00%H%x1f%h%x1f%ad%x1f%at%x1f%s'
# A NUL marks the start of each commit; the remaining fields are 0x1f-separated.
# %ad (--date=short) is the human-readable date we store; %at (commit epoch) is the
# precise ordering key, so same-day commits break ties by true time, not stream order.
_HEADER_PREFIX = "\x00"
_FIELD_SEP = "\x1f"
_GIT_LOG_FORMAT = "--format=%x00%H%x1f%h%x1f%ad%x1f%at%x1f%s"

Commit = dict[str, str | None]


def _commit_from_header(line: str) -> Commit:
    """Parse a NUL-prefixed header line into a commit dict (pr derived from subject)."""
    full, short, cdate, ts, subject = line[len(_HEADER_PREFIX) :].split(_FIELD_SEP, 4)
    return {
        "commit": full,
        "short": short,
        "date": cdate,
        "ts": ts,
        "subject": subject,
        "pr": parse_pr_number(subject),
    }


def parse_git_log(lines: Iterable[str], pattern: re.Pattern[str]) -> dict[str, dict]:
    """Single pass over a ``git log -p`` stream -> per-event provenance accumulator.

    Returns ``{event_type: {'instrumented', 'retired', 'last_change'}}`` where each
    value is a commit dict or None. Attribution is by COMMIT DATE, not stream order,
    so it is robust to git's newest-first output and to non-monotonic dates:
      - instrumented = earliest-dated commit that net-ADDED the literal
      - retired      = latest-dated commit that net-REMOVED it
      - last_change  = latest-dated commit that net-added OR net-removed it
    Within a commit, a literal on both + and - lines (a move/reindent) nets to zero
    and is ignored, so reformatting never looks like an add or a remove.
    """
    acc: dict[str, dict] = {}
    cur: Commit | None = None
    added: set[str] = set()
    removed: set[str] = set()

    def flush() -> None:
        if cur is None:
            return
        net_added = added - removed
        net_removed = removed - added
        for ev in net_added:
            _record(acc, ev, cur, "instrumented")
        for ev in net_removed:
            _record(acc, ev, cur, "retired")
        for ev in net_added | net_removed:
            _record(acc, ev, cur, "last_change")

    for line in lines:
        if line.startswith(_HEADER_PREFIX):
            flush()
            cur = _commit_from_header(line)
            added, removed = set(), set()
        elif cur is None:
            continue
        elif line.startswith("+") and not line.startswith("+++"):
            added |= find_events(line[1:], pattern)
        elif line.startswith("-") and not line.startswith("---"):
            removed |= find_events(line[1:], pattern)
    flush()
    return acc


def _record(acc: dict[str, dict], event: str, commit: Commit, slot: str) -> None:
    """Keep the earliest commit for 'instrumented', the latest for 'retired'/'last_change'.

    Ordering is by commit epoch ('ts'), so two net-changes on the same calendar day
    are disambiguated by their true time rather than by git's newest-first stream order.
    """
    entry = acc.setdefault(event, {"instrumented": None, "retired": None, "last_change": None})
    current = entry[slot]
    if current is None:
        entry[slot] = commit
    elif slot == "instrumented":
        if int(commit["ts"]) < int(current["ts"]):
            entry[slot] = commit
    elif int(commit["ts"]) > int(current["ts"]):  # 'retired' / 'last_change'
        entry[slot] = commit


# --------------------------------------------------------------------------- #
# Row assembly (schema shared with DATA-2005 staging and DATA-2006 incremental)
# --------------------------------------------------------------------------- #

# Output columns in write order. code_status / still_in_code dropped: both are derivable
# from the date null-pattern (present = instrumented set + retired null; removed = both set;
# not_found_in_code = instrumented null), so consumers derive them at read time.
PROVENANCE_COLUMNS = [
    "event_type",
    "event_type_slug",
    "instrumented_commit",
    "instrumented_pr",
    "instrumented_date",
    "retired_commit",
    "retired_pr",
    "retired_date",
    "last_code_change_date",
    "updated_at",
]


def build_provenance_row(
    event_type: str,
    accum_entry: dict | None,
    present_in_head: bool | None,
    updated_at: str,
) -> dict:
    """Turn one accumulator entry + HEAD-presence into a provenance row dict.

    ``accum_entry`` is None when the literal never net-changed inside the walked
    window (either it does not exist in code, or it predates the ``--since`` bound).
    We never guess provenance: instrumented/retired come only from observed commits.
    ``retired_*`` is emitted only when the event is genuinely gone at HEAD.
    """
    instrumented = accum_entry["instrumented"] if accum_entry else None
    retired = accum_entry["retired"] if accum_entry else None
    last_change = accum_entry["last_change"] if accum_entry else None
    has_history = bool(instrumented or retired or last_change)
    code_status = classify_code_status(present_in_head, has_history)

    row = {
        "event_type": event_type,
        "event_type_slug": slugify_event(event_type),
        "instrumented_commit": instrumented["commit"] if instrumented else None,
        "instrumented_pr": instrumented["pr"] if instrumented else None,
        "instrumented_date": instrumented["date"] if instrumented else None,
        "retired_commit": None,
        "retired_pr": None,
        "retired_date": None,
        "last_code_change_date": last_change["date"] if last_change else None,
        "updated_at": updated_at,
    }
    if code_status == "removed" and retired:
        row["retired_commit"] = retired["commit"]
        row["retired_pr"] = retired["pr"]
        row["retired_date"] = retired["date"]
    return row


def _pseudo_commit(commit: str | None, pr: str | None, date: str | None) -> dict:
    """A minimal commit dict reconstructed from a stored row.

    Carries only the keys ``build_provenance_row`` reads (commit, pr, date). It is never
    fed to ``_record``, so the epoch ``ts`` ordering key is not needed: the windowed
    commits are strictly newer than the watermark, so precedence is positional, not timed.
    """
    return {"commit": commit, "pr": pr, "date": date}


def merge_provenance_entry(
    existing_row: dict | None, new_entry: dict, present_before_window: bool = False
) -> dict:
    """Combine an event's existing table row with its new-window accumulator entry.

    The window only contains commits newer than the watermark, so:
      - instrumented: the existing (older) instrumentation wins; the window's add is used
        only when the row had no instrumentation yet AND the event was absent before the
        window (a genuinely new event). If the event was already present at the watermark
        (``present_before_window``) its true instrumentation predates the window and is not
        visible in it -- the window's net-add is a spurious edit, so instrumentation stays
        null rather than being stamped with a false, too-recent date.
      - retired: the window's removal wins (it is the latest); else the existing retired is
        carried forward. ``build_provenance_row`` still clears retired unless the event is
        absent at HEAD, so a re-add naturally drops a stale retired.
      - last_change: always the window's -- the event net-changed in the window.
    """
    if existing_row and existing_row.get("instrumented_commit"):
        instrumented = _pseudo_commit(
            existing_row["instrumented_commit"],
            existing_row["instrumented_pr"],
            existing_row["instrumented_date"],
        )
    elif present_before_window:
        instrumented = None
    else:
        instrumented = new_entry["instrumented"]

    retired = new_entry["retired"]
    if retired is None and existing_row and existing_row.get("retired_commit"):
        retired = _pseudo_commit(
            existing_row["retired_commit"],
            existing_row["retired_pr"],
            existing_row["retired_date"],
        )

    return {"instrumented": instrumented, "retired": retired, "last_change": new_entry["last_change"]}


def collect_provenance(
    events: Sequence[str],
    git_log_lines: Iterable[str],
    grep_text: str,
    updated_at: str,
) -> list[dict]:
    """Full pure pipeline: walk -> HEAD presence -> one provenance row per event."""
    pattern = compile_event_pattern(events)
    acc = parse_git_log(git_log_lines, pattern)
    present = present_at_head(events, grep_text)
    return [build_provenance_row(e, acc.get(e), present.get(e), updated_at) for e in events]


# --------------------------------------------------------------------------- #
# PR resolution (pure): backfill *_pr gaps the squash-suffix heuristic misses
# --------------------------------------------------------------------------- #

# parse_pr_number reads a PR ref straight off a commit subject -- but the *feature* commits
# that add/remove an event literal carry no ref; the PR number lives only on the merge commit
# that brought them into the deployed branch. So for any commit still missing a PR we walk the
# ancestry path to that introducing merge and parse its subject. Pure git, offline, and aligned
# with omni's merge-commit workflow (commits/{sha}/pulls returns nothing for this repo).

_PR_FIELDS = (("instrumented_commit", "instrumented_pr"), ("retired_commit", "retired_pr"))


def _pick_introducing_merge(rev_list_output: str) -> str | None:
    """From ``git rev-list --ancestry-path --merges <sha>..<ref>`` output, the introducing merge.

    rev-list emits newest-first, so the merge that actually brought the commit into the ref
    is the *oldest* on the ancestry path (later lines are subsequent merges on the mainline).
    """
    shas = rev_list_output.split()
    return shas[-1] if shas else None


def resolve_pr_gaps(
    rows: Sequence[dict], resolver: Callable[[str], str | None] | None
) -> tuple[Sequence[dict], int]:
    """Fill null ``*_pr`` from ``resolver(sha)``; each distinct SHA resolved once.

    Only touches a ``*_pr`` that is null and whose ``*_commit`` is set, so a PR already
    found by parse_pr_number is never overwritten. ``resolver`` is None when offline (no
    token, no gh CLI) -- a no-op, leaving whatever the subject heuristic found. Mutates the
    rows in place and returns ``(rows, n_filled)``.
    """
    if resolver is None:
        return rows, 0
    cache: dict[str, str | None] = {}

    def lookup(sha: str) -> str | None:
        if sha not in cache:
            cache[sha] = resolver(sha)
        return cache[sha]

    filled = 0
    for row in rows:
        for commit_key, pr_key in _PR_FIELDS:
            if row.get(pr_key) is None and row.get(commit_key):
                pr = lookup(row[commit_key])
                if pr is not None:
                    row[pr_key] = pr
                    filled += 1
    return rows, filled


def present_at_head(events: Sequence[str], grep_text: str) -> dict[str, bool]:
    """{event: bool} -- whether each known literal appears in a HEAD ``git grep`` dump."""
    found = find_events(grep_text, compile_event_pattern(events))
    return {e: e in found for e in events}


# --------------------------------------------------------------------------- #
# Git argv builder (pure)
# --------------------------------------------------------------------------- #


def build_git_log_argv(
    root: str, since: str | None, paths: Sequence[str], ref: str | None = None, pickaxe: str | None = None
) -> list[str]:
    """argv for a ``git log -p`` pass over the instrumentation paths.

    ``ref`` (e.g. ``origin/develop``) bounds the walk to a revision; placed before the ``--``
    so git reads it as a rev, not a path. None walks HEAD (current checkout). ``pickaxe`` adds
    ``-S<literal>`` so the walk streams only the commits that changed that literal's occurrence
    count -- far cheaper than the full diff stream, and the count-change semantics match our
    net-add / net-remove attribution exactly. ``-S`` is a fixed string by default, so quotes and
    spaces in an event name are matched literally.
    """
    argv = ["git", "-C", root, "log", "-p", "--date=short", _GIT_LOG_FORMAT]
    if since:
        argv += ["--since", since]
    if pickaxe is not None:
        argv.append(f"-S{pickaxe}")
    if ref:
        argv.append(ref)
    return [*argv, "--", *paths]


# --------------------------------------------------------------------------- #
# Git IO (thin subprocess wrappers; injectable for tests)
# --------------------------------------------------------------------------- #


def run_git_log(
    root: str, since: str | None, paths: Sequence[str], ref: str | None = None, pickaxe: str | None = None
) -> Iterator[str]:
    """Stream a ``git log -p`` pass line-by-line (the diff stream is large).

    A non-zero exit (bad ref, corrupt repo) raises ``CalledProcessError`` once the stream
    is exhausted: otherwise a failed log parses as an empty history and we would MERGE a
    table of all-null provenance rows with no error signal. ``pickaxe`` restricts the walk
    to commits that changed a single literal's count (see ``build_git_log_argv``).
    """
    argv = build_git_log_argv(root, since, paths, ref, pickaxe)
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    try:
        yield from (line.rstrip("\n") for line in proc.stdout)
    finally:
        proc.stdout.close()
        _, stderr = proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, argv, stderr=stderr)


def git_grep_present_text(root: str, events: Sequence[str], paths: Sequence[str], ref: str = "HEAD") -> str:
    """Dump of ``ref`` lines containing any known literal, via one fixed-string git grep.

    ``-F`` treats each event as a literal; ``-h`` drops filenames. git grep exits 1
    when nothing matches (an empty dump), which is not an error here; it exits 2+ on a
    real failure (bad ref, repo corruption, permission denied). We must raise on 2+:
    swallowing it returns an empty dump indistinguishable from "no matches", so every
    event would map to absent at HEAD, be written as ``removed``, and the watermark would
    advance on a fully corrupted CSV with no error signal -- the same silent-corruption
    risk ``run_git_log`` guards against.
    """
    patterns: list[str] = []
    for e in events:
        patterns += ["-e", e]
    argv = ["git", "-C", root, "grep", "-F", "-h", "--no-color", *patterns, ref, "--", *paths]
    proc = subprocess.run(argv, capture_output=True, text=True)
    if proc.returncode not in (0, 1):
        raise subprocess.CalledProcessError(proc.returncode, argv, stderr=proc.stderr)
    return proc.stdout


def git_head_sha(root: str, ref: str = "HEAD") -> str:
    return subprocess.run(
        ["git", "-C", root, "rev-parse", ref], capture_output=True, text=True, check=True
    ).stdout.strip()


def git_head_ref(root: str, ref: str | None = None) -> str:
    """The ref name to record as the watermark's branch: the deploy ref, else current HEAD."""
    if ref:
        return ref
    return subprocess.run(
        ["git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def git_commit_count(root: str, since: str | None, paths: Sequence[str], ref: str = "HEAD") -> int:
    argv = ["git", "-C", root, "rev-list", "--count", ref]
    if since:
        argv += ["--since", since]
    out = subprocess.run([*argv, "--", *paths], capture_output=True, text=True, check=True).stdout
    return int(out.strip() or "0")


def git_fetch(root: str, ref: str) -> None:
    """Update the deploy ref from its remote (``origin/develop`` -> ``git fetch origin develop``).

    Aborts on a non-zero exit (network outage, missing remote, bad credentials) rather than
    silently walking — and watermarking — stale local ``origin/develop`` state.
    """
    remote, _, branch = ref.partition("/")
    argv = ["git", "-C", root, "fetch", "--quiet", remote]
    if branch:
        argv.append(branch)
    result = subprocess.run(argv, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            f"ERROR: git fetch {remote} {branch} failed (exit {result.returncode}).\n"
            f"{result.stderr.strip()}\n"
            "Use --no-fetch to skip the fetch and walk the local ref state."
        )


# --------------------------------------------------------------------------- #
# Git PR resolution IO (merge-walk; no network, matches omni's merge-commit flow)
# --------------------------------------------------------------------------- #


def git_merge_pr(root: str, sha: str, deploy_ref: str) -> str | None:
    """PR number for the merge commit that introduced ``sha`` into ``deploy_ref``, else None.

    Walks the ancestry path from the commit to the deploy ref, takes the introducing merge,
    and parses its subject. None when the commit reached the ref without a merge (e.g. a
    direct push) or is not an ancestor of the ref.
    """
    rev_list = subprocess.run(
        ["git", "-C", root, "rev-list", "--ancestry-path", "--merges", f"{sha}..{deploy_ref}"],
        capture_output=True,
        text=True,
    )
    if rev_list.returncode != 0:
        return None
    merge_sha = _pick_introducing_merge(rev_list.stdout)
    if not merge_sha:
        return None
    subject = subprocess.run(
        ["git", "-C", root, "log", "-1", "--format=%s", merge_sha], capture_output=True, text=True
    ).stdout
    return parse_pr_number(subject)


def make_merge_walk_resolver(root: str, deploy_ref: str) -> Callable[[str], str | None]:
    """A ``sha -> PR`` resolver bound to a checkout + deploy ref, for ``resolve_pr_gaps``."""
    return lambda sha: git_merge_pr(root, sha, deploy_ref)


# --------------------------------------------------------------------------- #
# Databricks IO (cursor is injected -- a DB-API cursor from databricks_conn)
# --------------------------------------------------------------------------- #


def fetch_event_universe(cursor: Any, taxonomy_table: str = TAXONOMY_TABLE) -> list[str]:
    """The authoritative ~434 distinct event_type values to attribute provenance for."""
    cursor.execute(
        f"SELECT DISTINCT event_type FROM {taxonomy_table} WHERE event_type IS NOT NULL ORDER BY event_type"
    )
    return [str(r[0]) for r in cursor.fetchall()]


def write_provenance(rows: Sequence[dict], csv_path: str = DEFAULT_CSV_PATH) -> None:
    """Write the full provenance dataset to a CSV, sorted by event_type.

    Full rewrite (the dataset is ~434 rows): a deterministic column and row order keeps the
    committed file's git diffs minimal. Nulls render as empty fields. The csv module quotes
    event names containing commas or apostrophes correctly. ``lineterminator="\\n"`` forces
    LF (the csv default is CRLF), so the committed file matches the repo's line-ending hook
    and regenerating it produces no spurious diff.
    """
    ordered = sorted(rows, key=lambda r: r["event_type"])
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=PROVENANCE_COLUMNS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in ordered:
            writer.writerow({c: ("" if row.get(c) is None else row[c]) for c in PROVENANCE_COLUMNS})


def read_provenance_rows(csv_path: str = DEFAULT_CSV_PATH) -> dict[str, dict]:
    """Existing provenance rows keyed by event_type; {} when the file does not exist.

    Empty CSV fields parse back to None (the inverse of write_provenance's null rendering).
    """
    path = Path(csv_path)
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            row = {c: (raw.get(c) or None) for c in PROVENANCE_COLUMNS}
            out[row["event_type"]] = row
    return out


def write_watermark(
    state_path: str,
    last_processed_sha: str,
    head_ref: str,
    commit_count: int,
    last_run_at: str,
) -> None:
    """Write the SHA high-water mark to a sidecar JSON so the next run resumes from it."""
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "job_name": JOB_NAME,
        "last_processed_sha": last_processed_sha,
        "head_ref": head_ref,
        "commit_count": commit_count,
        "last_run_at": last_run_at,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_watermark(state_path: str = DEFAULT_STATE_PATH) -> dict | None:
    """Read the SHA high-water mark; None when the file is absent (first run -> backfill)."""
    path = Path(state_path)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "last_processed_sha": data["last_processed_sha"],
        "head_ref": data["head_ref"],
        "commit_count": data["commit_count"],
        "last_run_at": data["last_run_at"],
    }


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def resolve_omni_repo(arg: str | None, env: dict[str, str]) -> str:
    """The omni checkout to walk: --repo, else $OMNI_REPO. Must be an existing dir."""
    root = arg or env.get("OMNI_REPO")
    if not root:
        raise SystemExit("ERROR: pass --repo or set OMNI_REPO to an omni checkout path.")
    root = os.path.expanduser(root)
    if not os.path.isdir(os.path.join(root, ".git")) and not os.path.exists(os.path.join(root, ".git")):
        raise SystemExit(f"ERROR: {root} is not a git checkout (no .git).")
    return root


def run_backfill(
    cursor: Any,
    root: str,
    since: str | None,
    now: datetime,
    csv_path: str = DEFAULT_CSV_PATH,
    state_path: str = DEFAULT_STATE_PATH,
    ref: str = DEPLOY_REF,
    pr_resolver: Callable[[str], str | None] | None = None,
) -> list[dict]:
    """Fetch the universe, walk git once, write the CSV + watermark file. Returns the rows."""
    updated_at = now.replace(tzinfo=None).isoformat(timespec="seconds")
    events = fetch_event_universe(cursor)
    print(f"Event universe: {len(events)} event_types", file=sys.stderr)

    print(
        f"Walking git log -p {ref} {' '.join(INSTRUMENTATION_PATHS)} (since {since or 'all history'}) ...",
        file=sys.stderr,
    )
    lines = run_git_log(root, since, INSTRUMENTATION_PATHS, ref)
    grep_text = git_grep_present_text(root, events, INSTRUMENTATION_PATHS, ref)
    rows = collect_provenance(events, lines, grep_text, updated_at)

    _, filled = resolve_pr_gaps(rows, pr_resolver)
    if pr_resolver is not None:
        print(f"Merge-walk PR backfill: filled {filled} *_pr gaps", file=sys.stderr)

    write_provenance(rows, csv_path)
    write_watermark(
        state_path,
        git_head_sha(root, ref),
        git_head_ref(root, ref),
        git_commit_count(root, since, INSTRUMENTATION_PATHS, ref),
        updated_at,
    )
    return rows


def attribute_events_from_history(
    root: str,
    events: Sequence[str],
    since: str | None,
    ref: str,
    updated_at: str,
    pr_resolver: Callable[[str], str | None] | None = None,
    paths: Sequence[str] = INSTRUMENTATION_PATHS,
) -> list[dict]:
    """Full-history provenance rows for a specific set of events, via per-event pickaxe walks.

    Used to onboard universe events absent from the CSV during a refresh: events new to the
    Govern taxonomy but instrumented before the watermark, which the bounded ``last_sha..ref``
    window never sees. Each event gets a ``git log -S<literal>`` walk that streams only the
    commits changing its count, so onboarding a handful of events does not pay the full
    ``git log -p`` walk. Attribution then reuses the same ``build_provenance_row`` /
    ``present_at_head`` / ``resolve_pr_gaps`` path as the backfill, so an onboarded row matches
    what a full backfill would produce.
    """
    acc: dict[str, dict] = {}
    for event in events:
        lines = run_git_log(root, since, paths, ref, pickaxe=event)
        acc.update(parse_git_log(lines, compile_event_pattern([event])))
    grep_text = git_grep_present_text(root, events, paths, ref)
    present = present_at_head(events, grep_text)
    rows = [build_provenance_row(e, acc.get(e), present.get(e), updated_at) for e in events]
    resolve_pr_gaps(rows, pr_resolver)
    return rows


def run_refresh(
    cursor: Any,
    root: str,
    since: str | None,
    now: datetime,
    csv_path: str = DEFAULT_CSV_PATH,
    state_path: str = DEFAULT_STATE_PATH,
    ref: str = DEPLOY_REF,
    pr_resolver: Callable[[str], str | None] | None = None,
) -> list[dict]:
    """Incremental refresh bounded by the SHA watermark; full backfill when there is none.

    Reads the whole CSV, replaces only the events that changed in the window (giving them a
    fresh updated_at), carries every other row forward verbatim, and rewrites the full sorted
    CSV. Always advances the watermark file so the next run resumes from HEAD.

    New universe events absent from the CSV are onboarded in the same run via
    ``attribute_events_from_history`` (full-history pickaxe attribution), so a routine refresh
    covers both existing-event updates and brand-new events without a manual full backfill.
    Events removed from the universe are left in the CSV (their provenance is kept); a full
    rebuild from scratch is delete-the-state-file-and-re-run.
    """
    updated_at = now.replace(tzinfo=None).isoformat(timespec="seconds")
    watermark = read_watermark(state_path)
    if watermark is None:
        print("No watermark found -> full backfill", file=sys.stderr)
        return run_backfill(
            cursor,
            root,
            since,
            now,
            csv_path=csv_path,
            state_path=state_path,
            ref=ref,
            pr_resolver=pr_resolver,
        )

    last_sha = watermark["last_processed_sha"]
    events = fetch_event_universe(cursor)
    range_ref = f"{last_sha}..{ref}"
    print(f"Refresh: walking git log -p {range_ref} {' '.join(INSTRUMENTATION_PATHS)} ...", file=sys.stderr)

    lines = run_git_log(root, None, INSTRUMENTATION_PATHS, range_ref)
    acc = parse_git_log(lines, compile_event_pattern(events))
    affected = sorted(acc.keys())
    print(f"Affected events in window: {len(affected)}", file=sys.stderr)

    existing = read_provenance_rows(csv_path)
    missing = sorted(set(events) - set(existing))

    # Existing CSV events that changed in the window: cheap windowed update. (Events that are
    # both new to the CSV and changed in-window are excluded here and onboarded below from full
    # history, which is the more accurate attribution.)
    affected_existing = [ev for ev in affected if ev not in set(missing)]
    if affected_existing:
        grep_text = git_grep_present_text(root, affected_existing, INSTRUMENTATION_PATHS, ref)
        present = present_at_head(affected_existing, grep_text)
        # Presence at the watermark distinguishes a genuinely new event (absent before the
        # window -> the window's net-add is its true instrumentation) from one whose
        # instrumentation predates the window (present before it -> the net-add is a
        # spurious edit, so merge_provenance_entry must not stamp a false instrumented date).
        before_text = git_grep_present_text(root, affected_existing, INSTRUMENTATION_PATHS, last_sha)
        present_before = present_at_head(affected_existing, before_text)
        updated_rows: list[dict] = []
        for ev in affected_existing:
            merged = merge_provenance_entry(existing.get(ev), acc[ev], present_before.get(ev, False))
            updated_rows.append(build_provenance_row(ev, merged, present.get(ev), updated_at))
        _, filled = resolve_pr_gaps(updated_rows, pr_resolver)
        if pr_resolver is not None:
            print(f"Merge-walk PR backfill: filled {filled} *_pr gaps", file=sys.stderr)
        for row in updated_rows:
            existing[row["event_type"]] = row

    # New universe events absent from the CSV: instrumented before the watermark, so invisible
    # to the window. Onboard them from full history so a routine refresh needs no full backfill.
    if missing:
        print(
            f"Onboarding {len(missing)} new universe event(s) absent from the CSV "
            f"via full-history attribution ...",
            file=sys.stderr,
        )
        for row in attribute_events_from_history(root, missing, since, ref, updated_at, pr_resolver):
            existing[row["event_type"]] = row

    rows = list(existing.values())
    write_provenance(rows, csv_path)
    write_watermark(
        state_path,
        git_head_sha(root, ref),
        git_head_ref(root, ref),
        git_commit_count(root, None, INSTRUMENTATION_PATHS, ref),
        updated_at,
    )
    return rows


def _summarize(rows: Sequence[dict]) -> str:
    # not_found_in_code counts rows with no instrumentation date: this includes both events
    # never seen in code AND events present in code whose instrumentation predates the --since
    # window (the dropped still_in_code column was the only thing that distinguished them).
    present = removed = not_found = 0
    for r in rows:
        if r.get("instrumented_date") is None:
            not_found += 1
        elif r.get("retired_date") is None:
            present += 1
        else:
            removed += 1
    return f"{len(rows)} rows (present={present}, removed={removed}, not_found_in_code={not_found})"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", default=None, help="Path to an omni checkout (else $OMNI_REPO).")
    p.add_argument(
        "--since",
        default=DEFAULT_SINCE,
        help=f"Lower-bound git history (default {DEFAULT_SINCE}). 'all' walks full history.",
    )
    p.add_argument(
        "--csv", default=DEFAULT_CSV_PATH, help="Output provenance CSV (default analytics/data/...)."
    )
    p.add_argument(
        "--state", default=DEFAULT_STATE_PATH, help="Watermark JSON sidecar (default analytics/data/...)."
    )
    p.add_argument(
        "--ref", default=DEPLOY_REF, help=f"Deploy ref to attribute against (default {DEPLOY_REF})."
    )
    p.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip the git fetch of the deploy ref (use the local checkout's current ref state).",
    )
    p.add_argument(
        "--no-pr-resolve",
        action="store_true",
        help="Skip the merge-walk PR backfill; *_pr stays whatever the commit subject yielded.",
    )
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    root = resolve_omni_repo(args.repo, dict(os.environ))
    since = None if args.since == "all" else args.since
    print(f"Provenance CSV: {args.csv}", file=sys.stderr)

    if not args.no_fetch:
        print(f"Fetching {args.ref} ...", file=sys.stderr)
        git_fetch(root, args.ref)
    pr_resolver = None if args.no_pr_resolve else make_merge_walk_resolver(root, args.ref)

    from databricks_conn import get_connection  # lazy: pure logic imports without pandas/SDK

    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            rows = run_refresh(
                cursor,
                root,
                since,
                datetime.now(UTC),
                csv_path=args.csv,
                state_path=args.state,
                ref=args.ref,
                pr_resolver=pr_resolver,
            )
    finally:
        connection.close()
    print(_summarize(rows), file=sys.stderr)
    print(f"Wrote {args.csv} and watermark {args.state}", file=sys.stderr)


if __name__ == "__main__":
    main()
