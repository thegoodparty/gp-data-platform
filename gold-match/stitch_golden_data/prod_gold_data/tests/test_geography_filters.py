"""The geography filters: R0/R1/R2, the abstain-before-LLM paths,
the districts_text geography block, and the eligibility restriction
inside `_build_menu`. `test_braintrust_integration.py` stays the
frozen-core suite; this module is additive.
"""

import argparse
import asyncio
import math
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from stitch_golden_data.prod_gold_data.l2_br_matcher import (
    L2BrMatcher,
    _build_geography_block,
    _classify_office_geography,
    _StateUniverse,
    main,
)


@pytest.fixture
def mock_dependencies():
    with (
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.DatabricksClient") as mock_db_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.Gemini3Client") as mock_llm_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.GeminiEmbeddingClient") as mock_emb_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.init_braintrust"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.cache_prompt"),
    ):
        mock_llm = MagicMock()
        mock_llm_cls.return_value = mock_llm
        mock_emb = MagicMock()
        mock_emb_cls.return_value = mock_emb
        yield {"llm": mock_llm, "embedding": mock_emb, "databricks": mock_db_cls}


def _unit_vector(angle_degrees: float) -> list[float]:
    angle = math.radians(angle_degrees)
    return [math.cos(angle), math.sin(angle)]


class TestR0PartyCommittee:
    def test_x0024_abstains_with_no_district_state(self):
        """Failure this catches: a party-committee seat (no public
        electoral district exists) reaching the menu or the LLM instead
        of abstaining immediately.
        """
        verdict = _classify_office_geography(
            mtfcc="X0024",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=["County", "State"],
        )
        assert verdict.abstain is True

    def test_x0024_abstains_even_when_flagged_judicial(self):
        """Failure this catches: R1 (is_judicial) evaluated before R0,
        which would send a party-committee office to a judicial-only
        menu instead of R0's structural abstain -- precedence matters.
        """
        verdict = _classify_office_geography(
            mtfcc="X0024",
            is_judicial=True,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=["Judicial_District", "State"],
        )
        assert verdict.abstain is True


class TestR1Judicial:
    def test_no_judicial_vocabulary_abstains(self):
        """Failure this catches: a judicial office in a state whose L2
        universe carries no Judicial_* type getting a menu of
        exclusively wrong-level candidates instead of abstaining.
        """
        verdict = _classify_office_geography(
            mtfcc="X0010",
            is_judicial=True,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=["County", "City", "State"],
        )
        assert verdict.abstain is True

    def test_sole_supreme_court_vocabulary_abstains(self):
        """Failure this catches: a lower-court judge offered a menu of
        exclusively wrong-level courts because the state's only
        judicial type is Judicial_Supreme_Court_District -- the
        Kentucky case the design names.
        """
        verdict = _classify_office_geography(
            mtfcc="X0014",
            is_judicial=True,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=["Judicial_Supreme_Court_District", "County", "State"],
        )
        assert verdict.abstain is True

    def test_statewide_shaped_office_never_abstains_even_with_no_vocabulary(self):
        """Failure this catches: a statewide judicial office (e.g. a
        state Supreme Court seat) wrongly abstaining in a state whose
        universe carries no Judicial_* type at all -- appellate and
        supreme courts are legitimately statewide.
        """
        types = ["County", "State"]
        verdict = _classify_office_geography(
            mtfcc="G4000",
            is_judicial=True,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=types,
        )
        assert verdict.abstain is False
        assert {types[i] for i in verdict.eligible_indices} == {"State"}

    def test_judicial_with_vocabulary_gets_a_judicial_and_state_only_menu(self):
        """Failure this catches: a judicial office's menu including a
        non-judicial, non-State candidate once the state's universe
        does carry judicial vocabulary.
        """
        types = ["Judicial_District_Court_District", "Judicial_Appellate_District", "County", "City", "State"]
        verdict = _classify_office_geography(
            mtfcc="X0010",
            is_judicial=True,
            has_unknown_boundaries=False,
            geo_id=None,
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=types,
        )
        assert verdict.abstain is False
        eligible_types = {types[i] for i in verdict.eligible_indices}
        assert eligible_types == {"Judicial_District_Court_District", "Judicial_Appellate_District", "State"}


class TestR2Preconditions:
    def test_no_sub_area_passes_through_unrestricted(self):
        """Failure this catches: R2 firing (and denying a family's
        parent types) for a clean at-large office that carries no
        sub_area field at all -- the design restricts R2 to offices
        carrying at least one side of the sub_area pair.
        """
        verdict = _classify_office_geography(
            mtfcc="G4110",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="1234567",
            sub_area_name=None,
            sub_area_value=None,
            state_district_types=["City", "City_Ward"],
        )
        assert verdict.abstain is False
        assert verdict.eligible_indices is None

    def test_unmapped_mtfcc_passes_through(self):
        """Failure this catches: a family being guessed for an mtfcc
        outside the four mapped families, denying types for a
        jurisdiction shape this design was never measured against.
        """
        verdict = _classify_office_geography(
            mtfcc="X0071",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="12345",
            sub_area_name="Subdivision",
            sub_area_value="2",
            state_district_types=["Power_District", "State"],
        )
        assert verdict.abstain is False
        assert verdict.eligible_indices is None


class TestR2DecisionTable:
    """The R2 decision table, first match wins. Each id is one row
    of the table; each catches that row resolving to the wrong verdict.
    """

    @pytest.mark.parametrize(
        ("has_unknown_boundaries", "geo_id", "expected"),
        [
            (True, "1234567", "slice"),
            (False, "12345678", "slice"),
            (False, "1234567", "whole"),
            (False, "123456", "pass-through"),
            (False, "123456A", "pass-through"),
            (False, None, "pass-through"),
        ],
        ids=[
            "flag-true-is-slice-regardless-of-geo-id",
            "flag-false-longer-geo-id-is-slice",
            "flag-false-equal-geo-id-is-whole",
            "shorter-than-parent-is-passthrough",
            "nondigit-parent-prefix-is-passthrough",
            "missing-geo-id-is-passthrough",
        ],
    )
    def test_the_table(self, has_unknown_boundaries, geo_id, expected):
        types = ["City", "City_Ward", "State"]
        verdict = _classify_office_geography(
            mtfcc="G4110",
            is_judicial=False,
            has_unknown_boundaries=has_unknown_boundaries,
            geo_id=geo_id,
            sub_area_name="Ward",
            sub_area_value="3",
            state_district_types=types,
        )
        assert verdict.abstain is False
        if expected == "slice":
            assert {types[i] for i in verdict.eligible_indices} == {"City_Ward", "State"}
        elif expected == "whole":
            assert {types[i] for i in verdict.eligible_indices} == {"City", "State"}
        else:
            assert verdict.eligible_indices is None

    def test_zero_subtype_fail_closed_abstains_instead_of_falling_back(self):
        """Failure this catches: the v1 critical fix regression -- a
        slice-asserted office whose state's universe carries none of
        the family's sub-level types silently falling back to an
        out-of-family or statewide answer instead of abstaining, because
        denying only the family's parent types can never empty a pool
        that still holds out-of-family candidates and the synthetic
        State row.
        """
        verdict = _classify_office_geography(
            mtfcc="G4110",
            is_judicial=False,
            has_unknown_boundaries=True,
            geo_id=None,
            sub_area_name="Ward",
            sub_area_value="3",
            state_district_types=["City", "County", "State"],  # no City_Ward anywhere
        )
        assert verdict.abstain is True


class TestNamedRegressions:
    """The three named regressions, walked through with their
    real BR field values against a small hand-seeded universe. Asserts
    the verdict and its menu consequence (which types are eligible) --
    `_build_menu`'s own pool-restriction mechanism is separately proven
    by TestMenuPathRespectsEligibility below.
    """

    def test_compton_trustee_area_denies_the_parent_usd_and_keeps_the_subdistrict(self):
        """Failure this catches: the documented regression itself -- a
        trustee-area seat (a genuine slice, boundaries known) still able
        to match the too-broad parent Unified_School_District, as
        January did.
        """
        types = ["Unified_School_District", "Unified_School_SubDistrict", "City", "State"]
        verdict = _classify_office_geography(
            mtfcc="X0102",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="06096200000C",
            sub_area_name="Area",
            sub_area_value="C",
            state_district_types=types,
        )
        assert verdict.abstain is False
        eligible_types = {types[i] for i in verdict.eligible_indices}
        assert "Unified_School_District" not in eligible_types
        assert "Unified_School_SubDistrict" in eligible_types
        assert "City" in eligible_types  # out-of-family is never touched

    @pytest.mark.parametrize("gate_enabled", [False, True], ids=["gate-off", "gate-on"])
    def test_clay_county_school_board_seat_is_gated_on_the_school_whole_assertion(self, gate_enabled):
        """Failure this catches: shipping Clay County's fix (denying the
        numbered School_Board_District seats for a countywide-boundary
        office) before the holdout adjudicates it, or -- the opposite
        failure -- the gate never taking effect once flipped.
        """
        types = ["School_Board_District", "Unified_School_District", "State"]
        verdict = _classify_office_geography(
            mtfcc="G5420",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="1200300",
            sub_area_name="District",
            sub_area_value="4",
            state_district_types=types,
            school_whole_assertion_enabled=gate_enabled,
        )
        assert verdict.abstain is False
        if not gate_enabled:
            # The off arm must be steering-free: no whole-jurisdiction
            # sentence reaches the prompt, or "off" is not today's behavior
            # and the holdout's before/after comparison is contaminated.
            assert verdict.verdict_sentence is None
        if gate_enabled:
            eligible_types = {types[i] for i in verdict.eligible_indices}
            assert "School_Board_District" not in eligible_types
        else:
            assert verdict.eligible_indices is None

    @pytest.mark.parametrize("gate_enabled", [False, True], ids=["gate-off", "gate-on"])
    def test_bridgewater_value_only_sub_area_never_touches_the_out_of_family_township(self, gate_enabled):
        """Failure this catches: (a) a value-only sub_area (no name) read
        as "no sub_area" and passed through unclassified, and (b) the
        out-of-family Township answer -- the correct match here -- being
        denied in either gate state.
        """
        types = ["School_Subdistrict", "Township", "State"]
        verdict = _classify_office_geography(
            mtfcc="G5420",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="3402280",
            sub_area_name=None,
            sub_area_value="Bridgewater District",
            state_district_types=types,
            school_whole_assertion_enabled=gate_enabled,
        )
        assert verdict.abstain is False
        eligible_types = (
            {types[i] for i in verdict.eligible_indices} if verdict.eligible_indices is not None else set(types)
        )
        assert "Township" in eligible_types
        if gate_enabled:
            # Without this, a gate-on regression to an unrestricted menu
            # passes: Township survives correct and no-op implementations
            # alike, so the denial itself must be asserted.
            assert "School_Subdistrict" not in eligible_types


class TestGeographyBlock:
    def test_block_is_present_and_carries_the_territory_class_and_sentence(self):
        """Failure this catches: the block silently omitted from the
        prompt, leaving the LLM with no geography context at all.
        """
        block = _build_geography_block("G5420", "District", "4", False, "a verdict sentence")
        assert "unified school district" in block
        assert "District: 4" in block
        assert "a verdict sentence" in block

    def test_no_verdict_sentence_when_r2_never_fired(self):
        """Failure this catches: a verdict sentence appearing even when
        R2 never fired (pass-through), which would tell the LLM a
        classification happened when none did.
        """
        block = _build_geography_block("G4110", None, None, False, None)
        assert "none recorded" in block
        assert block.count("\n") == 3  # exactly the three always-present lines

    def test_a_newline_in_the_sub_area_value_is_collapsed(self):
        """Failure this catches: vendor text carrying an embedded newline
        breaking the block's one-line-per-field structure in the prompt.
        """
        block = _build_geography_block("G5420", None, "Bridgewater District\nSuite 400", False, None)
        sub_area_line = next(line for line in block.splitlines() if line.startswith("- Sub-area:"))
        assert "\n" not in sub_area_line
        assert sub_area_line == "- Sub-area: Bridgewater District Suite 400"

    def test_an_overlong_sub_area_value_is_capped(self):
        """Failure this catches: unbounded vendor text inflating the
        prompt with no limit.
        """
        block = _build_geography_block("G5420", None, "A" * 500, False, None)
        sub_area_line = next(line for line in block.splitlines() if line.startswith("- Sub-area:"))
        assert len(sub_area_line) < 250


class TestGeographyBlockReachesThePrompt:
    def test_match_office_appends_the_geography_block_to_the_actual_prompt(self, mock_dependencies):
        """Failure this catches: `match_office` building a geography block
        that is never actually wired into the LLM prompt -- a value
        computed but not passed to `_select_candidate`, or passed but not
        used. `_build_geography_block`'s own unit tests call it directly
        and cannot see a break in that wiring; this goes through the real
        call chain, with Braintrust disabled (conftest) so the real
        fallback prompt renders.
        """
        matcher = L2BrMatcher()
        matcher._universe_by_state["FL"] = _StateUniverse(
            embeddings=np.array([[1.0, 0.0]]),
            states=["FL"],
            district_types=["School_Board_District"],
            district_names=["Clay County School Board"],
        )
        embedded_texts: list[str] = []
        mock_dependencies["embedding"].create_embeddings.side_effect = lambda texts, **kw: (
            embedded_texts.append(texts[0]) or np.array([[1.0, 0.0]])
        )
        mock_dependencies["llm"].generate_structured_content.return_value = {
            "selected_candidate_number": 1,
            "selection_confidence": 90,
            "reasoning": "ok",
            "is_exact_district_match": True,
        }

        asyncio.run(
            matcher.match_office(
                br_database_id=1,
                br_name="Clay County School Board - District 4",
                state="FL",
                mtfcc="G5420",
                sub_area_name="District",
                sub_area_value="4",
                geo_id="1200300",
            )
        )

        prompt = mock_dependencies["llm"].generate_structured_content.call_args[1]["prompt"]
        assert "Office geography" in prompt
        assert "District: 4" in prompt
        # Folded from a standalone test: geography context must never leak
        # into the race-query embedding text, and only the caller boundary
        # can see that. The first embedded text is the race query verbatim.
        assert embedded_texts[0] == "race name: Clay County School Board - District 4"


class TestMenuPathRespectsEligibility:
    """The caller-level lesson from the core PR's own review: eligibility
    must precede both the top-13 ranking and the slot-11 insertion, not
    trim their output after the fact.
    """

    def test_thirteen_denied_candidates_are_replaced_by_the_eligible_rank_14_candidate(self, mock_dependencies):
        """Failure this catches: eligibility applied AFTER the top-13 cut
        (a post-hoc filter) instead of before it -- that would return an
        empty menu here, since all 13 candidates the raw race-similarity
        ranking would have chosen are denied and the eligible one never
        gets considered.
        """
        angles = list(range(13)) + [20]  # indices 0-12 outrank index 13 on raw similarity
        types = ["Denied"] * 13 + ["Eligible"]
        embeddings = np.array([_unit_vector(a) for a in angles])
        embedded_texts: list[str] = []
        mock_dependencies["embedding"].create_embeddings.side_effect = lambda texts, **kw: (
            embedded_texts.append(texts[0]) or np.array([[1.0, 0.0]])
        )

        matcher = L2BrMatcher()
        matcher._universe_by_state["DE"] = _StateUniverse(
            embeddings=embeddings, states=["DE"] * 14, district_types=types, district_names=types
        )

        menu = asyncio.run(matcher._build_menu("Test Race", "DE", eligible_indices=frozenset({13})))

        assert [c.l2_district_type for c in menu] == ["Eligible"]

    def test_slot_11_insertion_only_considers_eligible_candidates(self, mock_dependencies):
        """Failure this catches: the slot-11 "state" query insertion
        ranking over the FULL universe regardless of eligibility, which
        would let a denied candidate occupy the 11th slot even though
        every other menu position already respects the restriction.
        Proven differentially on one fixture: unrestricted, the denied
        candidate (the single best "state"-query match) is inserted;
        restricted, it never appears and an eligible candidate is
        inserted in its place instead.
        """
        filler_angles = list(range(14))  # indices 0-13: race ranks 1-14, state ranks 14-1 (ascending)
        types = [f"Filler_{i}" for i in range(14)] + ["Denied_State_Best"]
        angles = filler_angles + [89]  # index 14: worst race match, best state match overall
        embeddings = np.array([_unit_vector(a) for a in angles])

        def _query_embedding(texts, **kwargs):
            text = texts[0]
            return np.array([[0.0, 1.0]]) if text == "state" else np.array([[1.0, 0.0]])

        mock_dependencies["embedding"].create_embeddings.side_effect = _query_embedding

        def _seeded_matcher() -> L2BrMatcher:
            matcher = L2BrMatcher()
            matcher._universe_by_state["DE"] = _StateUniverse(
                embeddings=embeddings, states=["DE"] * 15, district_types=types, district_names=types
            )
            return matcher

        unrestricted_menu = asyncio.run(_seeded_matcher()._build_menu("Test Race", "DE", eligible_indices=None))
        assert unrestricted_menu[10].l2_district_type == "Denied_State_Best"

        restricted_menu = asyncio.run(
            _seeded_matcher()._build_menu("Test Race", "DE", eligible_indices=frozenset(range(14)))
        )
        assert restricted_menu[10].l2_district_type == "Filler_13"
        assert "Denied_State_Best" not in [c.l2_district_type for c in restricted_menu]


class TestLoadPendingOfficesNaNNormalization:
    def test_an_all_null_nullable_column_is_coerced_to_none_and_false(self, mock_dependencies):
        """Failure this catches: a small batch whose nullable column comes
        back entirely NULL is inferred float64 by pandas, and truthy NaN
        then subscript-crashes the geo_id format check, classifies a
        no-sub_area office as carrying one, and renders the literal string
        "nan" into a prompt. Coercion at the one load site is what makes
        every downstream bool()/subscript safe by construction.
        """
        raw = pd.DataFrame(
            {
                "br_database_id": [1],
                "name": ["Test Race"],
                "state": ["DE"],
                "mtfcc": ["G4110"],
                "geo_id": [float("nan")],
                "sub_area_name": [float("nan")],
                "sub_area_value": ["NULL"],
                "is_judicial": [float("nan")],
                "has_unknown_boundaries": [float("nan")],
            }
        )
        mock_dependencies["databricks"].return_value.execute_query.return_value = raw

        out = L2BrMatcher().load_pending_offices()

        row = out.iloc[0]
        assert row["geo_id"] is None
        assert row["sub_area_name"] is None
        # A literal "null"/"NULL" string is BR's documented sentinel for
        # this source family; it must normalize to absent, not truthy text.
        assert row["sub_area_value"] is None
        assert not row["is_judicial"] and out["is_judicial"].dtype == bool
        assert not row["has_unknown_boundaries"] and out["has_unknown_boundaries"].dtype == bool


class TestMatchOfficeAbstainsBeforeAnyCall:
    @pytest.mark.parametrize(
        ("mtfcc", "is_judicial", "sub_area_value", "types"),
        [
            ("X0024", False, None, ["County", "State"]),
            ("X0014", True, None, ["County", "State"]),
            ("G4110", False, "3", ["City", "State"]),
        ],
        ids=["party-committee", "judicial-no-vocabulary", "zero-subtype-slice"],
    )
    def test_an_abstaining_verdict_never_reaches_embeddings_or_the_llm(
        self, mock_dependencies, mtfcc, is_judicial, sub_area_value, types
    ):
        """Failure this catches: `match_office`'s early return moved or
        deleted, so an abstaining office still pays for query embeddings
        and an LLM call -- every classifier-level test stays green because
        none of them goes through the caller. Covers all three abstain
        families and pins confidence=None (no model judgment happened).
        """
        matcher = L2BrMatcher()
        matcher._universe_by_state["DE"] = _StateUniverse(
            embeddings=np.array([[1.0, 0.0]] * len(types)),
            states=["DE"] * len(types),
            district_types=types,
            district_names=[f"Name {i}" for i in range(len(types))],
        )

        result = asyncio.run(
            matcher.match_office(
                br_database_id=7,
                br_name="Test Race",
                state="DE",
                mtfcc=mtfcc,
                is_judicial=is_judicial,
                has_unknown_boundaries=True,
                geo_id="1234567",
                sub_area_name=None,
                sub_area_value=sub_area_value,
            )
        )

        assert (result.l2_state, result.l2_district_type, result.l2_district_name) == (None, None, None)
        assert result.confidence is None
        mock_dependencies["embedding"].create_embeddings.assert_not_called()
        mock_dependencies["llm"].generate_structured_content.assert_not_called()


class TestEmptyRestrictedMenuRaises:
    def test_a_restriction_that_empties_the_menu_raises_instead_of_abstaining(self, mock_dependencies):
        """Failure this catches: cross-repo drift (the universe losing its
        synthetic State row) turning a fully-restricted office into a
        0-candidate LLM prompt whose forced 0-selection is recorded as a
        model abstention, closing the office for 30 days over an
        infrastructure failure.
        """
        matcher = L2BrMatcher()
        matcher._universe_by_state["DE"] = _StateUniverse(
            embeddings=np.array([[1.0, 0.0]]), states=["DE"], district_types=["County"], district_names=["KENT"]
        )
        mock_dependencies["embedding"].create_embeddings.side_effect = lambda texts, **kw: np.array(
            [[1.0, 0.0]] * len(texts)
        )

        with pytest.raises(ValueError, match="left no eligible candidates"):
            asyncio.run(matcher._build_menu("Test Race", "DE", eligible_indices=frozenset()))

        mock_dependencies["llm"].generate_structured_content.assert_not_called()


class TestCountySubdivisionParentLength:
    def test_county_subdivision_uses_its_own_parent_length(self):
        """Failure this catches: the county_subdivision family (the only
        10-digit parent id) sharing another family's length -- a 10-digit
        geo_id must read as whole for G4040 while it reads as slice for the
        7-digit families, and nothing else in the suite can see that
        distinction.
        """
        types = ["Township", "Town_Ward", "State"]
        whole = _classify_office_geography(
            mtfcc="G4040",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="2004539175",
            sub_area_name="District",
            sub_area_value="2",
            state_district_types=types,
        )
        assert whole.eligible_indices is not None
        assert "Town_Ward" not in {types[i] for i in whole.eligible_indices}

        sliced = _classify_office_geography(
            mtfcc="G4040",
            is_judicial=False,
            has_unknown_boundaries=False,
            geo_id="200453917501",
            sub_area_name="District",
            sub_area_value="2",
            state_district_types=types,
        )
        assert sliced.eligible_indices is not None
        assert "Township" not in {types[i] for i in sliced.eligible_indices}


class TestMainForwardsTheSchoolGateFlag:
    def test_the_parsed_flag_reaches_run(self, mock_dependencies):
        """Failure this catches: --enable-school-whole-assertion parsed but
        never forwarded from main() to run(), leaving the holdout's enabled
        arm silently identical to the default arm.
        """
        captured: dict = {}

        async def _fake_run(self, **kwargs):
            captured.update(kwargs)
            return []

        with (
            patch("stitch_golden_data.prod_gold_data.l2_br_matcher._parse_args") as mock_args,
            patch.object(L2BrMatcher, "run", _fake_run),
            patch.object(L2BrMatcher, "print_summary", lambda self, results: None),
            patch("stitch_golden_data.prod_gold_data.l2_br_matcher.flush_logs", lambda: None),
        ):
            mock_args.return_value = argparse.Namespace(
                states=None,
                limit=None,
                batch_size=100,
                embedding_batch_size=100,
                enable_school_whole_assertion=True,
            )
            asyncio.run(main())

        assert captured["school_whole_assertion_enabled"] is True
