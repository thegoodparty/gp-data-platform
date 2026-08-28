"""The Run-B entry point: the cohort pre-write filter, the fail-loud prompt
guards, the A-vs-B manifest compare, and the count-verified rollback. No
live AWS/Databricks/Airflow/dbt-Cloud calls -- every collaborator here is a
double or a monkeypatched module function.
"""

import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from stitch_golden_data.prod_gold_data.backlog_run import (
    _answer_rows,
    _filter_and_write,
    _maybe_install_exclusion,
    _parse_args,
    _require_pinned_prompt,
    _rollback,
    _strict_build_cached_prompt,
    apply_cohort_filter,
    compare_runs,
)
from stitch_golden_data.prod_gold_data.l2_br_matcher import MatchResult

ATTEMPTED_AT = "2026-01-01T00:00:00+00:00"


def _match(br_database_id: int) -> MatchResult:
    return MatchResult(br_database_id, "DE", "House", f"District {br_database_id}", 90)


class TestCohortFilter:
    def test_passes_on_count_equality_and_records_counts(self):
        """Failure this catches: a correct-count predicate still being
        rejected, which would make a legitimate wholesale or cohort
        publication impossible to ship.
        """
        databricks = MagicMock()
        databricks.execute_query.return_value = pd.DataFrame({"br_database_id": [1, 2]})
        results = [_match(1), _match(2), _match(3)]

        filtered, counts = apply_cohort_filter(databricks, "select ...", expected_count=2, results=results)

        assert {r.br_database_id for r in filtered} == {1, 2}
        assert counts == {"approved_count": 2, "results_count": 3, "intersection_count": 2}

    def test_fails_on_count_mismatch(self):
        """Failure this catches: publishing against a predicate whose result
        has drifted from the recorded, owner-ruled count -- the exact
        wholesale-by-accident hole codex found (the write step appended
        results unfiltered and nothing enforced the predicate).
        """
        databricks = MagicMock()
        databricks.execute_query.return_value = pd.DataFrame({"br_database_id": [1, 2]})

        with pytest.raises(RuntimeError, match="recorded count is 3"):
            apply_cohort_filter(databricks, "select ...", expected_count=3, results=[_match(1)])

    def test_filter_runs_before_the_write_excludes_non_approved_offices(self):
        """Failure this catches: the writer receiving the matcher's raw
        output instead of the filtered set -- a quarantined office reaching
        the table because the filter was wired after the write, not before.
        """
        databricks = MagicMock()
        databricks.execute_query.return_value = pd.DataFrame({"br_database_id": [1, 2]})
        writer = MagicMock()
        writer.append_results.return_value = 2
        results = [_match(1), _match(2), _match(3)]  # 3 is NOT in the approved set

        cohort_counts, written = _filter_and_write(
            databricks, writer, results, "select ...", expected_count=2, run_key=datetime(2026, 1, 1, tzinfo=UTC)
        )

        written_results = writer.append_results.call_args.args[0]
        assert {r.br_database_id for r in written_results} == {1, 2}
        assert written == 2
        assert cohort_counts["intersection_count"] == 2


class _FakeMatcherClass:
    """Standalone class-level target for `_maybe_install_exclusion`'s
    patching, so its tests never touch the real `L2BrMatcher` -- a leaked
    patch on the shared class would silently affect every other test in
    this suite that constructs one.
    """

    def load_pending_offices(self, states=None, limit=None):
        return pd.DataFrame({"br_database_id": [1, 2, 3, 4]})


class TestOfficeExclusion:
    def test_listed_ids_are_dropped_and_counts_recorded(self, tmp_path):
        """Failure this catches: a known shape-miss office reaching
        match_office anyway and aborting the whole run pre-write --
        production run() has no per-office catch, so exclusion has to
        happen before the pending list is even read, not inside it.
        """
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("2\n# a note about this one\n\n3  # inline comment\n")

        exclude_ids, info = _maybe_install_exclusion(_FakeMatcherClass, exclude_file)
        df = _FakeMatcherClass().load_pending_offices()

        assert exclude_ids == {2, 3}
        assert sorted(df["br_database_id"]) == [1, 4]
        assert info["id_count"] == 2
        assert info["dropped"] == 2
        assert info["file_sha256"] == hashlib.sha256(exclude_file.read_bytes()).hexdigest()

    def test_no_file_leaves_the_matcher_class_untouched(self):
        """Failure this catches: patching the class even when no exclusion
        was requested -- an empty-set wrapper is observably identical on
        output but is still a global side effect on `L2BrMatcher` that
        every other caller in the process would silently inherit.
        """
        original = _FakeMatcherClass.load_pending_offices

        exclude_ids, info = _maybe_install_exclusion(_FakeMatcherClass, None)

        assert _FakeMatcherClass.load_pending_offices is original
        assert exclude_ids == frozenset()
        assert info == {"file_sha256": None, "id_count": 0, "dropped": 0}


class TestPinnedPromptFailLoud:
    def test_construction_half_raises_when_provenance_is_missing(self):
        """Failure this catches: a production run silently proceeding on the
        in-code fallback because Braintrust was simply unconfigured -- the
        matcher itself only warns in that case, which is fine for ad hoc dev
        use and not for a production write.
        """
        matcher = MagicMock(prompt_provenance=None)
        with pytest.raises(RuntimeError, match="refusing to run on the fallback"):
            _require_pinned_prompt(matcher)

    def test_construction_half_passes_when_provenance_is_present(self):
        _require_pinned_prompt(MagicMock(prompt_provenance={"loaded": True}))

    def test_per_office_half_raises_instead_of_falling_back(self):
        """Failure this catches: a mid-run render failure being swallowed
        into the in-code fallback prompt for just that one office, which
        would silently run part of a batch on a different prompt than the
        rest.
        """
        with patch("stitch_golden_data.prod_gold_data.backlog_run._orig_build_cached_prompt", return_value=""):
            with pytest.raises(RuntimeError, match="failing loud"):
                _strict_build_cached_prompt("p", {}, fallback_prompt="FALLBACK")

    def test_per_office_half_passes_through_a_real_render(self):
        with patch("stitch_golden_data.prod_gold_data.backlog_run._orig_build_cached_prompt", return_value="rendered"):
            assert _strict_build_cached_prompt("p", {}) == "rendered"


class TestAnswerRowsIntegrityGuard:
    def test_pending_and_results_id_mismatch_raises_an_operator_actionable_message(self):
        """Failure this catches: a bare KeyError (a results-lacking office)
        or a silently dropped row that corrupts answers_sha256 and the
        offices/matched/abstained counts (a pending-only office) if the
        worklist changes between this entry point's own read and run()'s
        internal one -- a violated freeze, not a case this module should
        paper over by reconciling the two reads itself.
        """
        # id 3 is pending-only, id 4 is results-only: one fixture, both
        # directions of the set inequality this check must catch.
        pending_df = pd.DataFrame(
            {
                "br_database_id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "state": ["DE", "DE", "DE"],
                "mtfcc": ["G4110", "G4110", "G4110"],
                "geo_id": ["1000000", "1000000", "1000000"],
                "sub_area_name": [None, None, None],
                "sub_area_value": [None, None, None],
                "is_judicial": [False, False, False],
                "has_unknown_boundaries": [False, False, False],
            }
        )
        results = [_match(1), _match(2), _match(4)]

        with pytest.raises(RuntimeError, match="is the freeze in place"):
            _answer_rows(pending_df, results)


def _manifest(pin_version="xact-1", **overrides) -> dict:
    base = {
        "prompt_provenance": {"resolved_version": pin_version},
        "embedding_config": {"model_id": "titan"},
        "llm_config": {"model_id": "haiku"},
        "inputs": {"pending_sha256": "p1", "prior_answers_sha256": "j1"},
        "universe": {"key_sha256": "u1"},
    }
    base.update(overrides)
    return base


def _answer_row(bid, **overrides) -> dict:
    row = {
        "br_database_id": bid,
        "name": "Office",
        "state": "DE",
        "mtfcc": "G4110",
        "geo_id": "1000000",
        "sub_area_name": None,
        "sub_area_value": None,
        "is_judicial": False,
        "has_unknown_boundaries": False,
        "l2_state": "DE",
        "l2_district_type": "House",
        "l2_district_name": "District 5",
        "confidence": 90,
    }
    row.update(overrides)
    return row


class TestManifestCompare:
    def test_produces_the_delta_report_with_correctly_bucketed_row_deltas(self, tmp_path):
        """Failure this catches: the A-vs-B report miscategorizing a row --
        e.g. calling a genuinely new office an output flip, missing that an
        office's BR input changed between the two runs, or letting a
        deliberately excluded office surface as an unexplained missing row
        instead of its own labeled bucket -- any of which would misdirect
        the named approver's review effort.
        """
        prior_dir = tmp_path
        prior_dir.joinpath("dry-run-manifest.json").write_text(json.dumps(_manifest()))
        prior_dir.joinpath("dry-run-answers.json").write_text(
            json.dumps(
                [
                    _answer_row(1),  # unchanged
                    _answer_row(2, name="Old Name"),  # input changes below
                    _answer_row(3, l2_district_name="District 3"),  # output flips below
                    _answer_row(5),  # excluded from B below, not a mystery drop
                ]
            )
        )

        this_manifest = _manifest()
        this_rows = [
            _answer_row(1),
            _answer_row(2, name="New Name"),
            _answer_row(3, l2_district_name="District 9"),
            _answer_row(4),  # B-only: absent from A entirely
        ]

        report = compare_runs(this_manifest, this_rows, prior_dir, excluded_ids=frozenset({5}))

        assert report["row_deltas"] == {
            "b_only": [4],
            "input_changed": [2],
            "output_flipped": [3],
            "excluded": [5],
        }
        assert report["config_equal"] == {"embedding_config": True, "llm_config": True}
        assert report["source_equal"] == {"pending_sha256": True, "prior_answers_sha256": True, "universe_sha256": True}

    def test_hard_fails_only_on_prompt_pin_mismatch(self, tmp_path):
        """Failure this catches: any OTHER difference (config, source hash,
        row deltas) blocking the run, when the runbook makes those review
        input, not a gate -- and, on the other side, a pin mismatch being
        reported as just another row in the delta table instead of stopping
        the compare outright (A and B are then not the same artifact at all).
        """
        prior_dir = tmp_path
        prior_dir.joinpath("dry-run-manifest.json").write_text(json.dumps(_manifest(pin_version="xact-OLD")))
        prior_dir.joinpath("dry-run-answers.json").write_text(json.dumps([_answer_row(1)]))

        # Config AND source hashes differ too -- neither raises.
        this_manifest = _manifest(pin_version="xact-OLD", embedding_config={"model_id": "different"})
        this_manifest["inputs"]["pending_sha256"] = "different"
        report = compare_runs(this_manifest, [_answer_row(1)], prior_dir)
        assert report["config_equal"]["embedding_config"] is False
        assert report["source_equal"]["pending_sha256"] is False

        with pytest.raises(RuntimeError, match="prompt pin mismatch"):
            compare_runs(_manifest(pin_version="xact-NEW"), [_answer_row(1)], prior_dir)


@pytest.fixture
def rollback_env(monkeypatch):
    """Clears, then the test opts in to, the two credential groups -- so a
    developer's ambient shell env can never make a "missing env" case pass
    by accident.
    """
    for name in (
        "DBT_CLOUD_BASE_URL",
        "DBT_CLOUD_ACCOUNT_ID",
        "DBT_CLOUD_API_TOKEN",
        "AIRFLOW_API_BASE_URL",
        "AIRFLOW_API_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)

    def _set_dbt():
        monkeypatch.setenv("DBT_CLOUD_BASE_URL", "https://dbt.example")
        monkeypatch.setenv("DBT_CLOUD_ACCOUNT_ID", "1")
        monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "token")

    def _set_airflow():
        monkeypatch.setenv("AIRFLOW_API_BASE_URL", "https://airflow.example")
        monkeypatch.setenv("AIRFLOW_API_TOKEN", "token")

    return type("Env", (), {"set_dbt": staticmethod(_set_dbt), "set_airflow": staticmethod(_set_airflow)})


@pytest.fixture
def patched_rollback_deps():
    with (
        patch("stitch_golden_data.prod_gold_data.backlog_run.MatchResultWriter") as writer_cls,
        patch("stitch_golden_data.prod_gold_data.backlog_run._trigger_dbt_rebuild_and_wait") as dbt_trigger,
        patch("stitch_golden_data.prod_gold_data.backlog_run._trigger_election_api_sync_and_wait") as sync_trigger,
    ):
        yield {"writer_cls": writer_cls, "writer": writer_cls.return_value, "dbt": dbt_trigger, "sync": sync_trigger}


class TestRollbackCountAssertion:
    @pytest.mark.parametrize(
        ("deleted", "expected", "should_raise"),
        [
            (7, 7, False),
            (5, 7, True),  # a short delete: fewer rows went than the record says the run wrote
            (0, 0, False),  # the zero-row case must NOT be treated as an automatic failure
            (3, 0, True),  # the mirror image: a nonzero delete when the record says zero
        ],
    )
    def test_rollback_count_assertion(self, deleted, expected, should_raise, rollback_env, patched_rollback_deps):
        """Failure this catches: `delete_run`'s only signal against a mistyped
        key or a naive/aware timezone mismatch is a wrong count (this
        connector hardcodes `rowcount = -1`), and a rollback that does not
        check it can silently declare success having deleted the wrong run
        or nothing at all.
        """
        rollback_env.set_dbt()
        patched_rollback_deps["writer"].delete_run.return_value = deleted
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", str(expected)])

        if should_raise:
            with pytest.raises(RuntimeError, match="disagree"):
                _rollback(args)
            patched_rollback_deps["dbt"].assert_not_called()
        else:
            _rollback(args)
            patched_rollback_deps["dbt"].assert_called_once()


class TestRollbackPublishGating:
    def test_sync_is_not_triggered_when_the_run_was_not_published(self, rollback_env, patched_rollback_deps):
        """Failure this catches: rolling back an unpublished run re-swapping
        PostgreSQL anyway -- there is nothing there to undo, and the DAG's
        own schedule already owns that surface until a release happens.
        """
        rollback_env.set_dbt()
        patched_rollback_deps["writer"].delete_run.return_value = 5
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", "5"])

        _rollback(args)

        patched_rollback_deps["dbt"].assert_called_once()
        patched_rollback_deps["sync"].assert_not_called()

    def test_sync_is_triggered_when_published(self, rollback_env, patched_rollback_deps):
        """Failure this catches: a rollback after a real release leaving the
        PostgreSQL surfaces on the bad vintage because the sync was left to
        the next nightly schedule instead of run inside the rollback itself.
        """
        rollback_env.set_dbt()
        rollback_env.set_airflow()
        patched_rollback_deps["writer"].delete_run.return_value = 5
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", "5", "--published"])

        _rollback(args)

        patched_rollback_deps["dbt"].assert_called_once()
        patched_rollback_deps["sync"].assert_called_once()

    def test_missing_env_with_published_fails_before_deleting_anything(self, rollback_env, patched_rollback_deps):
        """Failure this catches: discovering a missing Airflow credential
        only after the rows are already gone, leaving a run that is neither
        rolled back cleanly nor confirmed safe to leave in place.
        """
        rollback_env.set_dbt()  # AIRFLOW_* left unset
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", "5", "--published"])

        with pytest.raises(RuntimeError, match="AIRFLOW_API_BASE_URL"):
            _rollback(args)

        patched_rollback_deps["writer_cls"].assert_not_called()
