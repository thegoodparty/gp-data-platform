import argparse
import asyncio
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from stitch_golden_data.prod_gold_data.l2_br_matcher import (
    PINNED_PROMPT_VERSION,
    DistrictCandidate,
    L2BrMatcher,
    _district_embedding_text,
    _selection_from_response,
    _StateUniverse,
    _validate_district_universe,
    _validate_pending_offices,
    main,
)


@pytest.fixture
def mock_dependencies():
    with (
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.DatabricksClient") as mock_db,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.Gemini3Client") as mock_llm_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.GeminiEmbeddingClient") as mock_emb_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.init_braintrust") as mock_init_bt,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.cache_prompt") as mock_cache,
    ):
        mock_llm = MagicMock()
        mock_llm_cls.return_value = mock_llm
        mock_emb = MagicMock()
        mock_emb_cls.return_value = mock_emb

        yield {
            "databricks": mock_db,
            "llm_cls": mock_llm_cls,
            "llm": mock_llm,
            "embedding_cls": mock_emb_cls,
            "embedding": mock_emb,
            "init_braintrust": mock_init_bt,
            "cache_prompt": mock_cache,
        }


class TestBraintrustInit:
    def test_init_braintrust_called_on_construction(self, mock_dependencies):
        L2BrMatcher()

        mock_dependencies["init_braintrust"].assert_called_once_with(project="stitch-golden-data")

    def test_cache_prompt_called_on_construction(self, mock_dependencies):
        L2BrMatcher()

        mock_dependencies["cache_prompt"].assert_called_once_with(
            "stitch-golden-data-matcher",
            version=PINNED_PROMPT_VERSION,
        )


class TestPromptBuilding:
    def test_build_cached_prompt_called_with_correct_variables(self, mock_dependencies):
        matcher = L2BrMatcher()

        candidates = [
            DistrictCandidate(
                l2_state="DE", l2_district_type="CITY_COUNCIL", l2_district_name="City Council District 1"
            ),
            DistrictCandidate(
                l2_state="DE", l2_district_type="COUNTY_BOARD", l2_district_name="County Board District 2"
            ),
        ]

        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 90,
            "reasoning": "Best geographic match",
        }

        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt") as mock_build:
            mock_build.return_value = "rendered prompt"

            asyncio.run(matcher._select_candidate("Wilmington City Council", candidates))

            mock_build.assert_called_once()
            call_args = mock_build.call_args
            assert call_args[0][0] == "stitch-golden-data-matcher"

            variables = call_args[0][1]
            assert variables["br_name"] == "Wilmington City Council"
            assert variables["state"] == "DE"
            assert variables["num_districts"] == "2"
            assert "City Council District 1" in variables["districts_text"]
            assert "County Board District 2" in variables["districts_text"]

            assert call_args[1]["fallback_prompt"] is not None
            assert len(call_args[1]["fallback_prompt"]) > 0


class TestTraceNamePassthrough:
    def test_trace_name_passed_to_generate_structured_content(self, mock_dependencies):
        matcher = L2BrMatcher()

        candidates = [
            DistrictCandidate(l2_state="NY", l2_district_type="SCHOOL_BOARD", l2_district_name="School Board"),
        ]

        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 92,
            "reasoning": "Exact match",
        }

        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"):
            asyncio.run(matcher._select_candidate("School Board", candidates))

        call_kwargs = mock_dependencies["llm"].generate_structured_content.call_args[1]
        assert call_kwargs["trace_name"] == "stitch-match-selection"


class TestFrozenPromptReachesTheModel:
    """3: replacing the entire fallback prompt with the single character `x`
    left every existing test green, because the only assertions anywhere in
    this file about the prompt were `is not None` and `len(...) > 0`. The
    PR's central "frozen, and verified rather than asserted" claim had no
    standing enforcement -- the AST diff that verified it at review time was
    a one-time manual check, not a test.
    """

    def test_prompt_carries_a_distinctive_phrase_and_the_rendered_candidates(self, mock_dependencies):
        """Failure this catches: the frozen prompt drifts and every run is
        scored against a different prompt than January's, with nothing
        failing. Deliberately does NOT mock build_cached_prompt: Braintrust
        is disabled for every test (root conftest.py), so the real function
        falls through to rendering the fallback prompt below, exactly as it
        does in production whenever no cached prompt is available -- and
        only then does the assertion see the actual prompt text, not a
        stand-in string.
        """
        matcher = L2BrMatcher()
        candidates = [
            DistrictCandidate(
                l2_state="DE", l2_district_type="CITY_COUNCIL", l2_district_name="City Council District 1"
            ),
        ]
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 90,
            "reasoning": "ok",
            "is_exact_district_match": True,
        }

        asyncio.run(matcher._select_candidate("Wilmington City Council", candidates))

        prompt = mock_dependencies["llm"].generate_structured_content.call_args[1]["prompt"]
        # A stable, distinctive substring from the template -- not the whole
        # thirty lines, which would false-fail on a reflow and teach people
        # to delete the assertion -- plus the rendered candidate list, which
        # proves the menu actually reached the model.
        assert "Analyze the BR position and select the BEST matching candidate." in prompt
        assert "1. City Council District 1 (CITY_COUNCIL)" in prompt


class TestDistrictEmbeddingText:
    def test_district_embedding_text_matches_the_producer(self):
        """Failure this catches: if the embedding text format drifts, every
        cosine similarity score changes and the menu silently differs from
        January's, making the holdout comparison meaningless. The expected
        value is pinned verbatim from the producer's format string --
        vector_store_generator.create_embedding_texts, which stays behind in
        omni with the vector-store pickles. Both sides are frozen text, so
        the literal is a drift guard, not a drifting threshold.
        """
        text = _district_embedding_text("DE", "House", "District 5")

        assert text == "state: DE, district type: House, district name: District 5"


class TestSelectionValidation:
    """C: the response schema declares selected_candidate_number as a bare
    number, and shared/llm_gemini_3.py returns bare json.loads with no
    client-side validation, so a non-integral, boolean, or out-of-range
    value must raise rather than be silently truncated or coerced.
    """

    @pytest.mark.parametrize(
        "response",
        [
            {"selected_candidate_number": 3.9, "selection_confidence": 90},
            {"selected_candidate_number": True, "selection_confidence": 90},
            {"selected_candidate_number": 1, "selection_confidence": "95"},
            {"selected_candidate_number": 1, "selection_confidence": 950},
            {"selected_candidate_number": 1, "selection_confidence": 87.5},
            {"selected_candidate_number": 6, "selection_confidence": 90},
        ],
        ids=[
            "fractional-index",
            "boolean-index",
            "string-confidence",
            "out-of-range-confidence",
            "fractional-confidence",
            "out-of-bounds-index",
        ],
    )
    def test_malformed_selection_or_confidence_raises(self, response):
        """Failure this catches: `int(float(3.9)) == 3` silently records the
        model's 4th choice as its 3rd, `int(float(True)) == 1` lets a
        boolean select candidate 1, and a string/out-of-range/fractional
        confidence writes cleanly into a `confidence bigint` column that
        expects an integer 0-100. The last case is the only one that reaches
        the index BOUNDS branch -- every other index case fails inside
        `_require_integral` first -- and it is reachable because the response
        schema's `maximum` is declared but not enforced client-side.
        """
        with pytest.raises(ValueError):
            _selection_from_response(response, num_candidates=5)


class TestValidateDistrictUniverse:
    """The fail-closed guard on the menu source. Its failure is expensive and
    silent rather than loud: `groupby` drops a null state without an error, and
    a null district type or name survives into the embedding text as the
    literal string "nan" and into `MatchResult.l2_district_name` as a float --
    so the office misses the universe join, rule 3 re-offers it on every dbt
    build, and the LLM cost is re-paid indefinitely.
    """

    @staticmethod
    def _universe() -> pd.DataFrame:
        return pd.DataFrame({"state_postal_code": ["DE"], "district_type": ["House"], "district_name": ["District 5"]})

    @pytest.mark.parametrize("column", ["state_postal_code", "district_type", "district_name"])
    @pytest.mark.parametrize("bad_value", [None, "   "], ids=["null", "blank"])
    def test_a_null_or_blank_column_raises_naming_that_column(self, column, bad_value):
        universe = self._universe()
        universe.loc[0, column] = bad_value

        with pytest.raises(ValueError, match=column):
            _validate_district_universe(universe)

    def test_a_fully_populated_universe_passes(self):
        """Failure this catches: a guard strict enough to reject real data,
        which would fail every run rather than none.
        """
        _validate_district_universe(self._universe())


class TestValidatePendingOffices:
    """The fail-closed guard on the worklist, and the sibling of
    TestValidateDistrictUniverse.

    Its two branches fail differently. A non-canonical state reaches
    `load_district_universe`'s IN-clause unescaped -- and unlike `--states`,
    that value is warehouse data rather than operator input, so one apostrophe
    in it breaks or widens the query. A blank name is worse: it embeds
    cleanly as "race name: " and comes back with a real, arbitrary match,
    which is a link the pending list's own 30-day rule never reopens.
    """

    @staticmethod
    def _pending(state: str = "DE", name: str = "Test Race") -> pd.DataFrame:
        return pd.DataFrame({"br_database_id": [1], "name": [name], "state": [state]})

    @pytest.mark.parametrize(
        "state", ["de", "D", "DEE", "D E", "DE') or 1=1 --"], ids=["lower", "one", "three", "space", "injection"]
    )
    def test_a_non_canonical_state_raises(self, state):
        with pytest.raises(ValueError, match="non-canonical state code"):
            _validate_pending_offices(self._pending(state=state))

    @pytest.mark.parametrize("name", ["", "   ", None], ids=["empty", "whitespace", "null"])
    def test_a_blank_or_null_name_raises(self, name):
        with pytest.raises(ValueError, match="blank or null name"):
            _validate_pending_offices(self._pending(name=name))

    def test_a_valid_worklist_passes(self):
        """Failure this catches: a guard strict enough to reject real rows,
        which would fail every run rather than none.
        """
        _validate_pending_offices(self._pending())


class TestTaskTypeInvariant:
    """K: create_embeddings dispatches on `len(texts) == 1` to decide task
    type. A mock stubbed with a bare `return_value` is insensitive to its
    call arguments, so batching the two query embeddings into one call would
    stay green even though it moves both into RETRIEVAL_DOCUMENT space.
    """

    def test_query_embeddings_are_never_batched_together(self, mock_dependencies):
        """Failure this catches: queries batched into one call move into
        RETRIEVAL_DOCUMENT space, changing every similarity score, and
        nothing raises.
        """
        matcher = L2BrMatcher()
        matcher._universe_by_state["DE"] = _StateUniverse(
            embeddings=np.array([[1.0, 0.0]]), states=["DE"], district_types=["House"], district_names=["District 5"]
        )
        # Size-aware, not a flat return_value: a flat return would make a
        # wrongly-batched call still unpack cleanly (a false negative), so
        # the call-count/call-shape assertions below are what would ever
        # fail rather than an incidental unpacking crash.
        mock_dependencies["embedding"].create_embeddings.side_effect = lambda texts, **kwargs: np.array(
            [[1.0, 0.0]] * len(texts)
        )
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 90,
            "reasoning": "Clean match",
            "is_exact_district_match": True,
        }

        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"):
            asyncio.run(matcher.match_office(br_database_id=1, br_name="Test Race", state="DE"))

        calls = mock_dependencies["embedding"].create_embeddings.call_args_list
        assert len(calls) == 2, f"expected one create_embeddings call per query, got {len(calls)}"
        for call in calls:
            texts = call.args[0]
            assert (
                len(texts) == 1
            ), "a query embedding call must carry exactly one text or it moves into RETRIEVAL_DOCUMENT space"


class TestTerminalOutcomeContract:
    """A run persists two outcomes and no third: the three district fields
    populated (a match) or all three None (an attempt that found nothing).
    There is no status column, so a populated district name is the whole
    signal. A technical error fails the run instead of being delivered as
    a match.
    """

    @staticmethod
    def _seed_single_candidate_universe(
        matcher: L2BrMatcher, office_state: str, candidate_state: str, district_type: str, district_name: str
    ) -> None:
        """One district in one state's universe, pre-embedded so no network
        call is needed to build it.
        """
        matcher._universe_by_state[office_state] = _StateUniverse(
            embeddings=np.array([[1.0, 0.0]]),
            states=[candidate_state],
            district_types=[district_type],
            district_names=[district_name],
        )

    def test_llm_exception_raises_instead_of_returning_a_result(self, mock_dependencies):
        """Failure this catches: a transport failure delivered as a match to
        a district named LLM_ERROR, which joins nothing.
        """
        matcher = L2BrMatcher()
        self._seed_single_candidate_universe(matcher, "DE", "DE", "House", "District 5")
        mock_dependencies["embedding"].create_embeddings.return_value = np.array([[1.0, 0.0]])
        mock_dependencies["llm"].generate_structured_content.side_effect = RuntimeError("Gemini call failed")

        with (
            patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"),
            pytest.raises(RuntimeError, match="Gemini call failed"),
        ):
            asyncio.run(matcher.match_office(br_database_id=1, br_name="Test Race", state="DE"))

    def test_selection_index_zero_yields_abstained_with_null_district_fields(self, mock_dependencies):
        """Failure this catches: a no-match verdict recorded as a match
        because status was decided by string-comparing the district name
        instead of the selection index.
        """
        matcher = L2BrMatcher()
        self._seed_single_candidate_universe(matcher, "DE", "DE", "House", "District 5")
        mock_dependencies["embedding"].create_embeddings.return_value = np.array([[1.0, 0.0]])
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 0,
            "selection_confidence": 15,
            "reasoning": "No good candidate",
            "is_exact_district_match": False,
        }

        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"):
            result = asyncio.run(matcher.match_office(br_database_id=1, br_name="Test Race", state="DE"))

        assert result.l2_state is None
        assert result.l2_district_type is None
        assert result.l2_district_name is None

    def test_valid_selection_yields_matched_with_the_candidates_own_state(self, mock_dependencies):
        """Failure this catches: the state dropped from the district key
        (e.g. read from the office instead of the selected candidate), so a
        same-named district in another state would resolve silently.
        """
        matcher = L2BrMatcher()
        # The office's own state ("DE") deliberately differs from the
        # candidate's stored state ("ZZ") so the assertion below only passes
        # if the result reads the candidate's state, not the office's.
        self._seed_single_candidate_universe(matcher, "DE", "ZZ", "House", "District 5")
        mock_dependencies["embedding"].create_embeddings.return_value = np.array([[1.0, 0.0]])
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 95.0,
            "reasoning": "Clean match",
            "is_exact_district_match": True,
        }

        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"):
            result = asyncio.run(matcher.match_office(br_database_id=1, br_name="Test Race", state="DE"))

        assert result.l2_state == "ZZ"
        assert result.l2_district_type == "House"
        assert result.l2_district_name == "District 5"
        assert result.confidence == 95
        assert isinstance(result.confidence, int)


class TestRunEndToEnd:
    """A: build_universe is called from run()'s own event loop, and
    create_embeddings does `asyncio.run(...)` internally for any multi-text
    input (shared/llm_gemini.py:941) -- calling it directly, not through a
    thread, raises `RuntimeError: asyncio.run() cannot be called from a
    running event loop`. Every state's real universe has at least two rows
    (real districts plus the synthetic 'State' row), so the single-text
    escape never applies. This drives run() end to end with the Databricks
    reads mocked but the universe built through the real code path -- the
    other tests in this file all pre-seed `_universe_by_state` by hand and
    never exercise it.
    """

    embedded_texts: list[list[str]] = []

    @staticmethod
    def _create_embeddings_side_effect(texts, **kwargs):
        if len(texts) != 1:
            # Reproduce the real client's behavior for a multi-text call:
            # asyncio.run(...) internally. This raises if called directly
            # from a running event loop, which is exactly the crash this
            # test exists to catch, without needing the real Gemini client.
            asyncio.run(asyncio.sleep(0))
            TestRunEndToEnd.embedded_texts.append(list(texts))
        return np.array([[1.0, 0.0]] * len(texts))

    def test_run_builds_the_universe_and_reaches_office_matching(self, mock_dependencies):
        TestRunEndToEnd.embedded_texts = []
        # Lower case on BOTH sides, deliberately: with only the universe side
        # lower-cased, this test could pass on an accidental fix to just the
        # groupby key (round 1's mistake) while the pending side's own
        # normalization silently stayed broken (round 1's commit claimed
        # coverage of both sides that this test did not actually have).
        # Geography columns carry pass-through values (no family mapping, not
        # judicial, no sub_area): this test's own concern is the universe
        # build path, not the geography filters, which have their own module.
        pending_df = pd.DataFrame(
            {
                "br_database_id": [1],
                "name": ["Test Race"],
                "state": ["de"],
                "mtfcc": ["Z9999"],
                "geo_id": [None],
                "sub_area_name": [None],
                "sub_area_value": [None],
                "is_judicial": [False],
                "has_unknown_boundaries": [False],
            }
        )
        universe_df = pd.DataFrame(
            {
                "state_postal_code": ["de", "de"],
                "district_type": ["House", "State"],
                "district_name": ["District 5", "Delaware"],
            }
        )
        mock_dependencies["databricks"].return_value.execute_query.side_effect = [pending_df, universe_df]
        mock_dependencies["embedding"].create_embeddings.side_effect = self._create_embeddings_side_effect
        # Realistic (not auto-MagicMock) cost-stat returns: run() reads these
        # for its cost baseline regardless of outcome, and a failure logs a
        # delta against them, so an unconfigured mock would fail on a
        # MagicMock-minus-MagicMock format string rather than on the actual
        # assertion.
        mock_dependencies["embedding"].get_cost_stats.return_value = {"total_cost": 0.0}
        mock_dependencies["llm"].get_usage_stats.return_value = {"total_cost": 0.0}
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 90,
            "reasoning": "Clean match",
            "is_exact_district_match": True,
        }

        matcher = L2BrMatcher()
        with patch("stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt", return_value="prompt"):
            results = asyncio.run(matcher.run())

        assert len(results) == 1
        assert results[0].br_database_id == 1
        # A populated district is what says "matched" now that no status
        # column exists. Not the district's NAME: every fixture embedding
        # here is identical, so which of the two tied candidates lands at
        # slot 1 is the frozen tie-break's business, pinned elsewhere, and
        # asserting it would red this test on an unrelated menu change.
        assert results[0].l2_district_name is not None

        # The district text as it was actually assembled at the call site,
        # not as _district_embedding_text produces it in isolation. Those are
        # different things: the frozen-format test imports the function
        # directly, so it cannot see a caller that reads the three columns in
        # the wrong order. That swap yields a plausible string and a
        # different embedding, silently changing every match -- the failure
        # _embed_state_universe's own comment names, and which nothing
        # detected before this assertion.
        assert TestRunEndToEnd.embedded_texts == [
            [
                "state: DE, district type: House, district name: District 5",
                "state: DE, district type: State, district name: Delaware",
            ]
        ]


class TestUniverseCoverageGuard:
    def test_a_pending_state_missing_from_the_universe_raises_before_embedding(self, mock_dependencies):
        """Failure this catches: the guard deleted, or moved back after the
        embedding loop.

        A state the current L2 delivery does not carry aborts the run and
        persists nothing, so discovering it late means paying the full
        embedding bill for a run that cannot finish. The delivery gap is not
        hypothetical -- prod rebuilds the universe several times an hour, so
        one can land mid-run.

        The fixture is deliberately a PARTIAL universe, not an empty one: with
        an empty universe the groupby yields no groups, nothing is embedded
        either way, and the ordering assertion passes even on a guard placed
        after the loop. Verified by moving the guard back and watching this
        test still pass, which is what forced the fixture to change.
        """
        # TWO pending states with the universe carrying only ONE of them.
        # An empty universe cannot distinguish the two orderings, because the
        # groupby yields no groups and nothing is embedded either way -- so
        # the ordering assertion below would pass on a guard placed after the
        # loop. A partial universe is what makes the two cases differ.
        pending_df = pd.DataFrame(
            {
                "br_database_id": [1, 2],
                "name": ["Test Race", "Other Race"],
                "state": ["DE", "CA"],
                "mtfcc": ["G4110", "G4110"],
                "geo_id": ["1234567", "7654321"],
                "sub_area_name": [None, None],
                "sub_area_value": [None, None],
                "is_judicial": [False, False],
                "has_unknown_boundaries": [False, False],
            }
        )
        partial_universe = pd.DataFrame(
            {
                "state_postal_code": ["DE", "DE"],
                "district_type": ["House", "State"],
                "district_name": ["District 5", "Delaware"],
            }
        )
        mock_dependencies["databricks"].return_value.execute_query.side_effect = [pending_df, partial_universe]
        mock_dependencies["embedding"].create_embeddings.side_effect = lambda texts, **kw: np.array(
            [[1.0, 0.0]] * len(texts)
        )
        mock_dependencies["embedding"].get_cost_stats.return_value = {"total_cost": 0.0}
        mock_dependencies["llm"].get_usage_stats.return_value = {"total_cost": 0.0}

        matcher = L2BrMatcher()
        with pytest.raises(ValueError, match=r"No district universe entry for state\(s\): \['CA'\]"):
            asyncio.run(matcher.run())

        # DE's districts are present and embeddable, so a guard placed after
        # the loop would have paid for them before raising. This is the
        # assertion that actually pins the ordering.
        mock_dependencies["embedding"].create_embeddings.assert_not_called()


class TestMainTeardown:
    def test_a_raising_close_does_not_replace_the_run_s_own_exception(self, mock_dependencies):
        """Failure this catches: the operator losing the real traceback to an
        unrelated cleanup error. The run fails because the session died, and
        `Connection.close()` then raises on that same dead session -- so the
        two failures are correlated rather than independent, and an unguarded
        close in the `finally` supersedes the exception in flight.

        Also pins the ORDER: flush_logs has to run first, or a teardown
        failure takes the Braintrust buffer with it.
        """
        order: list[str] = []

        def _close() -> None:
            order.append("close")
            raise RuntimeError("close also failed")

        mock_dependencies["databricks"].return_value.execute_query.side_effect = RuntimeError("session died")
        mock_dependencies["databricks"].return_value.close.side_effect = _close
        mock_dependencies["embedding"].get_cost_stats.return_value = {"total_cost": 0.0}
        mock_dependencies["llm"].get_usage_stats.return_value = {"total_cost": 0.0}

        with (
            patch("stitch_golden_data.prod_gold_data.l2_br_matcher._parse_args") as mock_args,
            patch(
                "stitch_golden_data.prod_gold_data.l2_br_matcher.flush_logs",
                side_effect=lambda: order.append("flush_logs"),
            ),
        ):
            mock_args.return_value = argparse.Namespace(
                states=None,
                limit=None,
                batch_size=100,
                embedding_batch_size=100,
                enable_school_whole_assertion=False,
                model_config="gemini",
            )
            with pytest.raises(RuntimeError, match="session died"):
                asyncio.run(main())

        # Order, not just survival. Asserting only that the run's exception
        # propagates is tautological with respect to the ordering: the close
        # is swallowed either way, so swapping the two lines leaves such a
        # test green. Verified by swapping them and watching it pass, which
        # is why this assertion exists.
        assert order == ["flush_logs", "close"]


class TestLoadPendingOfficesStatesFilter:
    """An empty states list means "select nothing", not "select everything".
    The two are one character apart: `if states:` treats [] as absent and
    drops the WHERE clause, so a programmatic run(states=[]) -- what a
    sharding loop or the write path can compute -- reads and bills the whole
    backlog instead of returning immediately. argparse's nargs="+" forbids an
    empty list, so no CLI test can reach this.
    """

    def test_empty_states_list_returns_no_rows_and_never_queries(self, mock_dependencies):
        matcher = L2BrMatcher()
        result = matcher.load_pending_offices(states=[])

        assert result.empty
        assert list(result.columns) == [
            "br_database_id",
            "name",
            "state",
            "mtfcc",
            "geo_id",
            "sub_area_name",
            "sub_area_value",
            "is_judicial",
            "has_unknown_boundaries",
        ]
        mock_dependencies["databricks"].return_value.execute_query.assert_not_called()

    def test_states_none_means_no_filter_and_does_query(self, mock_dependencies):
        mock_dependencies["databricks"].return_value.execute_query.return_value = pd.DataFrame(
            {
                "br_database_id": [1],
                "name": ["Race"],
                "state": ["DE"],
                "mtfcc": ["G4110"],
                "geo_id": ["1234567"],
                "sub_area_name": [None],
                "sub_area_value": [None],
                "is_judicial": [False],
                "has_unknown_boundaries": [False],
            }
        )
        matcher = L2BrMatcher()
        result = matcher.load_pending_offices(states=None)

        assert not result.empty
        mock_dependencies["databricks"].return_value.execute_query.assert_called_once()
        assert "where state in" not in mock_dependencies["databricks"].return_value.execute_query.call_args[0][0]

    @pytest.mark.parametrize(
        "bad_value",
        ["DE') or 1=1 --", "DEE", "D", "12", "D E", ""],
        ids=["injection", "three-letters", "one-letter", "digits", "space", "empty"],
    )
    def test_a_malformed_state_raises_before_any_query_is_built(self, mock_dependencies, bad_value):
        """Failure this catches: a non-canonical value spliced unescaped into
        the WHERE clause. This is the only guard on that path --
        `_validate_pending_offices` runs on rows the query has ALREADY
        returned, so it cannot protect the query that produced them. The
        injection case is the vector `_validate_states_filter`'s own docstring
        names, and nothing verified it raised.
        """
        matcher = L2BrMatcher()

        with pytest.raises(ValueError, match="canonical two-letter state code"):
            matcher.load_pending_offices(states=[bad_value])

        mock_dependencies["databricks"].return_value.execute_query.assert_not_called()

    def test_a_named_state_reaches_the_where_clause(self, mock_dependencies):
        mock_dependencies["databricks"].return_value.execute_query.return_value = pd.DataFrame(
            {
                "br_database_id": [1],
                "name": ["Race"],
                "state": ["DE"],
                "mtfcc": ["G4110"],
                "geo_id": ["1234567"],
                "sub_area_name": [None],
                "sub_area_value": [None],
                "is_judicial": [False],
                "has_unknown_boundaries": [False],
            }
        )
        matcher = L2BrMatcher()
        matcher.load_pending_offices(states=["de"])

        query = mock_dependencies["databricks"].return_value.execute_query.call_args[0][0]
        # The normalization must be IN the predicate: this test passes a
        # lowercase 'de' and the warehouse column can hold one too.
        assert "where upper(trim(state)) in ('DE')" in query
