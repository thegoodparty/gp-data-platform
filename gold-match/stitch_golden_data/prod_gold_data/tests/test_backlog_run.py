"""The Run-B entry point: the cohort pre-write filter, the fail-loud prompt
guards, the A-vs-B manifest compare, and the count-verified rollback. No
live AWS/Databricks/Airflow/dbt-Cloud calls -- every collaborator here is a
double or a monkeypatched module function.
"""

import hashlib
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from stitch_golden_data.prod_gold_data.backlog_run import (
    _answer_rows,
    _assert_fresh_out_dir,
    _assert_no_later_runs,
    _assert_swap_succeeded,
    _maybe_install_exclusion,
    _parse_args,
    _require_pinned_prompt,
    _rollback,
    _strict_build_cached_prompt,
    _universe_sha256,
    apply_cohort_filter,
    compare_runs,
)
from stitch_golden_data.prod_gold_data.l2_br_matcher import MatchResult

ATTEMPTED_AT = "2026-01-01T00:00:00+00:00"
# The `run` subcommand's other required flags, so a --model-config parse
# test fails on an invalid CHOICE, not a missing argument. argparse only
# converts these to Path/int; nothing needs to exist on disk to parse.
_REQUIRED_RUN_ARGS = ["--cohort-predicate-file", "x.sql", "--cohort-expected-count", "0", "--out-dir", "out"]


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


class TestOfficeExclusion:
    def test_listed_ids_are_dropped_and_counts_recorded(self, tmp_path):
        """Failure this catches: a known shape-miss office reaching
        match_office anyway and aborting the whole run pre-write --
        production run() has no per-office catch, so exclusion has to
        happen before the pending list is even read, not inside it.
        """
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("2\n# a note about this one\n\n3  # inline comment\n")
        matcher = MagicMock()
        matcher.load_pending_offices.return_value = pd.DataFrame({"br_database_id": [1, 2, 3, 4]})

        exclude_ids, captured = _maybe_install_exclusion(matcher, exclude_file)
        df = matcher.load_pending_offices()

        assert exclude_ids == {2, 3}
        assert sorted(df["br_database_id"]) == [1, 4]
        assert captured["id_count"] == 2
        assert captured["dropped"] == 2
        assert captured["file_sha256"] == hashlib.sha256(exclude_file.read_bytes()).hexdigest()
        assert captured["pending_df"] is df

    def test_no_file_applies_no_filtering(self):
        """Failure this catches: the wrap dropping rows (or raising) when no
        exclusion file was given -- it must always be a faithful passthrough
        in that case, differing from the underlying read only when a real
        exclusion file says to. The wrap is still installed (there is no
        second read left to fall back to for the manifest's input echo).
        """
        matcher = MagicMock()
        matcher.load_pending_offices.return_value = pd.DataFrame({"br_database_id": [1, 2, 3]})

        exclude_ids, captured = _maybe_install_exclusion(matcher, None)
        df = matcher.load_pending_offices()

        assert exclude_ids == set()
        assert sorted(df["br_database_id"]) == [1, 2, 3]
        assert captured["file_sha256"] is None
        assert captured["id_count"] == 0
        assert captured["dropped"] == 0
        assert captured["pending_df"] is df


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
    def test_length_mismatch_between_pending_and_results_raises(self):
        """Failure this catches: a silently dropped or extra row corrupting
        answers_sha256 and the offices/matched/abstained counts. With a
        single shared read (the exclusion wrap's capture), a length
        mismatch can only mean run() itself broke its one-result-per-office
        contract -- this must not paper over that by zipping past the
        shorter list.
        """
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
        results = [_match(1), _match(2)]  # one short

        with pytest.raises(RuntimeError, match="expected exactly one result per office"):
            _answer_rows(pending_df, results)

    def test_boolean_fields_survive_stdlib_json_from_a_numpy_bool_cell(self):
        """Failure this catches: is_judicial/has_unknown_boundaries copied
        straight off the pending frame without coercion. An object-dtype
        column carrying a raw numpy.bool_ cell (verified: itertuples()
        passes it through unchanged, unlike a properly astype(bool)'d
        column) crashes stdlib json.dumps -- the run would die writing
        answers.json AFTER the paid matching, not before it.
        """
        pending_df = pd.DataFrame(
            {
                "br_database_id": [1],
                "name": ["Office"],
                "state": ["DE"],
                "mtfcc": ["G4110"],
                "geo_id": ["1000000"],
                "sub_area_name": [None],
                "sub_area_value": [None],
                "is_judicial": pd.array([np.bool_(True)], dtype=object),
                "has_unknown_boundaries": pd.array([np.bool_(False)], dtype=object),
            }
        )

        rows = _answer_rows(pending_df, [_match(1)])

        assert type(rows[0]["is_judicial"]) is bool
        assert type(rows[0]["has_unknown_boundaries"]) is bool
        json.dumps(rows)  # must not raise


class _State:
    def __init__(self, states, district_types, district_names):
        self.states, self.district_types, self.district_names = states, district_types, district_names


class TestUniverseSha256:
    def test_matches_the_two_stage_dry_run_algorithm(self):
        """Failure this catches: a single flatten-everything-then-hash pass
        over every state's district lines together, which produces a
        DIFFERENT digest than the dry-run driver's two-stage (per-state,
        then per-state-hash) algorithm over identical data -- making the
        compare's universe equality always false against Run A.
        """
        matcher = MagicMock()
        matcher._universe_by_state = {
            "DE": _State(["DE"], ["House"], ["District 5"]),
            "CA": _State(["CA"], ["House"], ["District 1"]),
        }

        key_sha256, per_state = _universe_sha256(matcher)

        assert per_state == {
            "DE": "4aaefeb68973e4e37ca90eeda21e427789fb06f0b6ac3368985e5cde7fe737ed",
            "CA": "27c01ed3c7dc881101e9a1da282022d5348855dc5b4c863c8097037cf44c458a",
        }
        assert key_sha256 == "7d5849d18de5ddd1256f98796957c7d3b73d2ba237e3beded562d4e2294b646d"


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
        office's BR input changed, mislabeling an unexplained prior-only
        drop as excluded, or (the mirror image) labeling an id absent from
        BOTH runs as excluded when there was never anything for the
        exclusion to explain -- any of which would misdirect the named
        approver's review effort.
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
                    _answer_row(6),  # prior-only, NOT on the exclusion list: unexplained
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
        # 7 is on the exclusion list but never appeared in EITHER run's
        # answers -- must land in neither bucket.
        report = compare_runs(this_manifest, this_rows, prior_dir, excluded_ids=frozenset({5, 7}))

        assert report["row_deltas"] == {
            "b_only": [4],
            "input_changed": [2],
            "output_flipped": [3],
            "excluded": [5],
            "a_only": [6],
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


class TestModelConfigRestriction:
    def test_only_bedrock_is_an_accepted_choice(self):
        """Failure this catches: --model-config gemini or bedrock-nova
        reaching _build_clients -- gemini's clients lack resolved_config()
        (a crash after the paid matching), and bedrock-nova's embeddings
        never passed the holdout gate, so either is a semantics change the
        STOP rule exists to block, not something argparse should allow
        production to select at all.
        """
        assert _parse_args(["run", *_REQUIRED_RUN_ARGS]).model_config == "bedrock"
        for rejected in ("gemini", "bedrock-nova"):
            with pytest.raises(SystemExit):
                _parse_args(["run", "--model-config", rejected, *_REQUIRED_RUN_ARGS])


class TestOutDirFreshnessGuard:
    @pytest.mark.parametrize("existing_name", ["manifest.json", "answers.json", "run-record.json"])
    def test_refuses_when_any_run_artifact_already_exists(self, existing_name, tmp_path):
        """Failure this catches: a second `run` pointed at an already-used
        out-dir silently overwriting the first run's manifest, answers, or
        record -- the evidence a rollback or audit would need to reconcile
        what actually happened.
        """
        (tmp_path / existing_name).write_text("{}")

        with pytest.raises(RuntimeError, match=existing_name):
            _assert_fresh_out_dir(tmp_path)

    def test_passes_on_a_truly_empty_out_dir(self, tmp_path):
        _assert_fresh_out_dir(tmp_path)


class TestNoLaterRunsGuard:
    def test_raises_when_later_run_keys_exist(self):
        """Failure this catches: rolling back only the target run key while
        a later run's rows still stand, leaving that later run's answers
        un-rolled-back and the results table's newest-row-wins contract
        half-undone -- delete_run's own documented repair is the target and
        every later key together, never the target alone.
        """
        databricks = MagicMock()
        databricks.execute_query.return_value = pd.DataFrame({"n": [3]})

        with pytest.raises(RuntimeError, match="every later key together"):
            _assert_no_later_runs(databricks, datetime.fromisoformat(ATTEMPTED_AT))

    def test_passes_when_no_later_run_keys_exist(self):
        databricks = MagicMock()
        databricks.execute_query.return_value = pd.DataFrame({"n": [0]})

        _assert_no_later_runs(databricks, datetime.fromisoformat(ATTEMPTED_AT))


class TestElectionApiSwapVerification:
    @pytest.mark.parametrize(("task_state", "should_raise"), [("success", False), ("skipped", True)])
    def test_a_skipped_swap_task_is_not_accepted_as_a_completed_sync(self, task_state, should_raise):
        """Failure this catches: the election-api sync DAG completing
        successfully while its swap task was silently skipped --
        cutover_enabled's short-circuit makes that a normal DAG success, not
        a failure, so the DAG-level state alone cannot catch it.
        """
        if should_raise:
            with pytest.raises(RuntimeError, match="election_api_swap_enabled"):
                _assert_swap_succeeded(task_state)
        else:
            _assert_swap_succeeded(task_state)


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
        writer = writer_cls.return_value
        # No later run keys by default -- _assert_no_later_runs reads this
        # before every delete_run call in these tests.
        writer.databricks.execute_query.return_value = pd.DataFrame({"n": [0]})
        yield {"writer_cls": writer_cls, "writer": writer, "dbt": dbt_trigger, "sync": sync_trigger}


class TestRollbackDelegatesCountCheckToDeleteRun:
    @pytest.mark.parametrize(("delete_run_effect", "should_raise"), [(7, False), (RuntimeError("mismatch"), True)])
    def test_expected_count_is_passed_through_and_a_raise_is_propagated(
        self, delete_run_effect, should_raise, rollback_env, patched_rollback_deps
    ):
        """Failure this catches: this module recomputing (or dropping) the
        count check now that it lives in delete_run itself -- rollback must
        pass its recorded expected_count through and trust delete_run's own
        pre-delete verification, never re-deriving or ignoring it, and must
        not rebuild when that verification rejects the count.
        """
        rollback_env.set_dbt()
        if should_raise:
            patched_rollback_deps["writer"].delete_run.side_effect = delete_run_effect
        else:
            patched_rollback_deps["writer"].delete_run.return_value = delete_run_effect
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", "7"])

        if should_raise:
            with pytest.raises(RuntimeError, match="mismatch"):
                _rollback(args)
            patched_rollback_deps["dbt"].assert_not_called()
        else:
            _rollback(args)
            patched_rollback_deps["dbt"].assert_called_once()
        call = patched_rollback_deps["writer"].delete_run.call_args
        assert call.kwargs["expected_count"] == 7


class TestRollbackTeardown:
    def test_close_raising_does_not_prevent_the_rebuild_trigger(self, rollback_env, patched_rollback_deps):
        """Failure this catches: a raise from writer.close() inside the
        finally propagating out and skipping the dbt rebuild trigger
        entirely -- the delete already succeeded by that point, so this
        would leave the rows gone but every consumer unrebuilt until the
        next scheduled build.
        """
        rollback_env.set_dbt()
        patched_rollback_deps["writer"].delete_run.return_value = 5
        patched_rollback_deps["writer"].close.side_effect = RuntimeError("connection already closed")
        args = _parse_args(["rollback", "--run-key", ATTEMPTED_AT, "--expected-count", "5"])

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
