"""The evidence link: omni's latched dormant anchors reaching this catalog.

Pure functions and a synthetic state blob. No network — `load_latches` is only
exercised through its degrade paths.
"""

import json

import pytest
from semantic_catalog import evidence
from semantic_catalog.records import MetricRecord, ratified_cell

LEG = "Dashboard - Campaign Plan Viewed"
METRIC = "win_active_candidates_30d"


def _state(**latches):
    return json.dumps({"run_date": "2026-09-08", "latches": latches})


def _latched(metric=METRIC, since="2026-07-31", latched=True):
    return {"metric": metric, "since": since, "reference": 676.5, "consecutive": 5, "latched": latched}


def _rec(name=METRIC, build_approved="2026-08-05"):
    return MetricRecord(
        name=name,
        label=name,
        definition="d",
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=None,
        detail_doc=None,
        retired=None,
        yaml_file="sem.yml",
        kind="metric",
        build_approved=build_approved,
        value_at_signing=377,
    )


def test_a_latched_leg_is_read_onto_its_metric():
    got = evidence.parse_latches(_state(**{LEG: _latched()}))
    assert got[METRIC][0].leg_key == LEG
    assert got[METRIC][0].since == "2026-07-31"


def test_a_broken_but_not_yet_latched_leg_is_ignored():
    # One broken week is recorded but not latched. Acting on it would re-create
    # the false alarms the two-week threshold exists to avoid.
    assert evidence.parse_latches(_state(**{LEG: _latched(latched=False)})) == {}


def test_no_latches_is_empty_not_an_error():
    assert evidence.parse_latches(_state()) == {}
    assert evidence.parse_latches("{}") == {}


def test_a_latch_with_no_metric_is_rejected():
    bad = json.dumps({"latches": {LEG: {"latched": True, "since": "2026-07-31"}}})
    with pytest.raises(ValueError, match="no metric"):
        evidence.parse_latches(bad)


def test_apply_marks_the_build_half_with_the_reason():
    latches = evidence.parse_latches(_state(**{LEG: _latched()}))
    rec = evidence.apply([_rec()], latches)[0]
    assert rec.needs_reverification is not None
    assert LEG in rec.needs_reverification and "2026-07-31" in rec.needs_reverification
    # It says how it clears, because there is no dismissal: the way to silence it
    # is to fix the instrument or change what the metric is anchored on.
    assert "anchored_on" in rec.needs_reverification


def test_apply_says_nothing_about_a_metric_with_no_build_approval():
    # Telling a reader that a PENDING approval needs re-verification says nothing.
    latches = evidence.parse_latches(_state(**{LEG: _latched()}))
    assert evidence.apply([_rec(build_approved=None)], latches)[0].needs_reverification is None


def test_apply_leaves_an_unlatched_metric_alone():
    assert evidence.apply([_rec()], {})[0].needs_reverification is None


def test_the_cell_says_needs_re_verification_rather_than_a_bare_date():
    # The whole point: the catalog must stop reading green while the instrument
    # feeding the number is broken.
    latches = evidence.parse_latches(_state(**{LEG: _latched()}))
    rec = evidence.apply([_rec()], latches)[0]
    assert ratified_cell(rec) == "rule pending · build 2026-08-05 (NEEDS RE-VERIFICATION)"


def test_re_verification_outranks_staleness_in_the_cell():
    import dataclasses

    latches = evidence.parse_latches(_state(**{LEG: _latched()}))
    rec = evidence.apply([dataclasses.replace(_rec(), build_stale=True)], latches)[0]
    assert "NEEDS RE-VERIFICATION" in ratified_cell(rec) and "stale" not in ratified_cell(rec)


def test_several_dormant_legs_report_the_earliest_break():
    state = _state(**{LEG: _latched(since="2026-07-31"), "Other Leg": _latched(since="2026-08-15")})
    rec = evidence.apply([_rec()], evidence.parse_latches(state))[0]
    assert "2026-07-31" in rec.needs_reverification
    assert "events have" in rec.needs_reverification


def test_a_missing_token_disables_the_check_loudly(monkeypatch):
    # A guard that disables itself quietly rebuilds the original bug inside the
    # alarm, so the reason comes back and every surface prints it.
    monkeypatch.delenv(evidence.TOKEN_ENV, raising=False)
    monkeypatch.setenv(evidence.GH_FALLBACK_ENV, "1")
    latches, problems = evidence.load_latches(None)
    assert latches == {}
    assert problems and "NOT being checked" in problems[0]


def test_an_unreachable_repo_is_reported_rather_than_read_as_healthy(monkeypatch):
    def _boom(token):
        raise TimeoutError("gone")

    monkeypatch.setattr(evidence, "_fetch", _boom)
    latches, problems = evidence.load_latches("fake-token")
    assert latches == {} and problems and "could not read" in problems[0]


def test_unparseable_state_is_reported_rather_than_read_as_healthy(monkeypatch):
    monkeypatch.setattr(evidence, "_fetch", lambda token: "{not json")
    latches, problems = evidence.load_latches("fake-token")
    assert latches == {} and problems and "did not parse" in problems[0]


def test_re_verification_is_not_a_change_to_the_definition():
    # Only the after-side is annotated, so if this counted as a field change,
    # every metric with a dormant anchor would read as changed by a merge that
    # did not touch it — and no diff line could explain why.
    from semantic_catalog.slack_diff import changed_metric_names, diff_records

    latches = evidence.parse_latches(_state(**{LEG: _latched()}))
    before = [_rec()]
    after = evidence.apply([_rec()], latches)
    assert changed_metric_names(before, after) == []
    assert diff_records(before, after) == []


def test_a_latches_block_that_is_not_a_mapping_is_rejected():
    # Caught rather than iterated: load_latches turns it into a stated reason,
    # and silently treating a malformed file as "no latches" would read as green.
    with pytest.raises(ValueError, match="not a mapping"):
        evidence.parse_latches(json.dumps({"latches": ["not", "a", "mapping"]}))


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload.encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_the_token_read_returns_the_raw_file(monkeypatch):
    seen = {}

    def urlopen(request, timeout=None):
        seen["url"] = request.full_url
        seen["auth"] = request.get_header("Authorization")
        return _Response('{"latches": {}}')

    monkeypatch.setattr(evidence.urllib.request, "urlopen", urlopen)
    assert evidence._fetch("tok") == '{"latches": {}}'
    assert evidence.STATE_PATH in seen["url"] and evidence.REPO in seen["url"]
    assert seen["auth"] == "Bearer tok"


def test_the_gh_fallback_surfaces_its_own_failure(monkeypatch):
    # The laptop path. A nonzero exit has to become a RuntimeError, because
    # load_latches only turns exceptions into a stated reason; a silent empty
    # string would read as a healthy file with no latches.
    class _Proc:
        returncode = 1
        stdout = ""
        stderr = "gh: not authenticated"

    monkeypatch.setattr(evidence.subprocess, "run", lambda *a, **kw: _Proc())
    with pytest.raises(RuntimeError, match="not authenticated"):
        evidence._fetch_via_gh()


def test_the_gh_fallback_returns_stdout_on_success(monkeypatch):
    class _Proc:
        returncode = 0
        stdout = '{"latches": {}}'
        stderr = ""

    monkeypatch.setattr(evidence.subprocess, "run", lambda *a, **kw: _Proc())
    assert evidence._fetch_via_gh() == '{"latches": {}}'
