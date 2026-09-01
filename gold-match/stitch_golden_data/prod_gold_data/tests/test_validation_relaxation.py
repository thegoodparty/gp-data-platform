"""The unconsumed-field relaxation: responses are judged against a copy of the
selection schema that tolerates is_exact_district_match being absent or
malformed, while the request schema stays frozen."""

import asyncio
import inspect
from unittest.mock import MagicMock, patch

import jsonschema
import pytest

from stitch_golden_data.prod_gold_data.l2_br_matcher import (
    UNCONSUMED_RESPONSE_FIELD,
    DistrictCandidate,
    L2BrMatcher,
    relax_validation_schema,
)

SAMPLE_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_candidate_number": {"type": "number", "minimum": 0, "maximum": 2},
        "selection_confidence": {"type": "number", "minimum": 0, "maximum": 100},
        "reasoning": {"type": "string"},
        "is_exact_district_match": {"type": "boolean"},
    },
    "required": [
        "selected_candidate_number",
        "selection_confidence",
        "reasoning",
        "is_exact_district_match",
    ],
}


@pytest.fixture
def mock_dependencies():
    with (
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.DatabricksClient"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.Gemini3Client") as mock_llm_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.GeminiEmbeddingClient"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.init_braintrust"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.cache_prompt"),
    ):
        mock_llm = MagicMock()
        mock_llm_cls.return_value = mock_llm
        yield {"llm": mock_llm}


def test_relaxation_tolerates_only_the_unconsumed_field():
    """An omitted or markup-mangled is_exact_district_match aborted whole runs
    (the model flubs exactly this field, deterministically per office, and the
    field feeds nothing); a missing CONSUMED field must still fail."""
    relaxed = relax_validation_schema(SAMPLE_SCHEMA)

    omission = {"selected_candidate_number": 1, "selection_confidence": 75, "reasoning": "r"}
    mangled = omission | {"is_exact_district_match": "true</is_exact_district_match>\n</invoke>"}
    jsonschema.validate(omission, relaxed)
    jsonschema.validate(mangled, relaxed)

    missing_consumed_field = {"selected_candidate_number": 1, "reasoning": "r"}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_consumed_field, relaxed)

    # The input schema is untouched: the request is built from it.
    assert UNCONSUMED_RESPONSE_FIELD in SAMPLE_SCHEMA["required"]
    assert SAMPLE_SCHEMA["properties"][UNCONSUMED_RESPONSE_FIELD] == {"type": "boolean"}


def test_select_candidate_judges_responses_against_the_relaxed_schema(mock_dependencies):
    """Without this wiring the client validates strictly and a flubbed
    unconsumed field still fails the office after a wasted re-ask."""
    with patch(
        "stitch_golden_data.prod_gold_data.l2_br_matcher.build_cached_prompt",
        return_value="prompt",
    ):
        matcher = L2BrMatcher()
        candidates = [
            DistrictCandidate(l2_state="WI", l2_district_type="City_Ward", l2_district_name="WAUWATOSA WARD 4"),
            DistrictCandidate(l2_state="WI", l2_district_type="City", l2_district_name="WAUWATOSA CITY"),
        ]
        asyncio.run(matcher._select_candidate("Wauwatosa City Council - District 4", candidates, ""))

    call_kwargs = mock_dependencies["llm"].generate_structured_content.call_args.kwargs
    assert UNCONSUMED_RESPONSE_FIELD in call_kwargs["response_schema"]["required"]
    assert call_kwargs["validation_schema"] == relax_validation_schema(call_kwargs["response_schema"])


def test_gemini_client_tolerates_the_validation_schema_kwarg():
    """--model-config gemini constructs the dormant incumbent stack; without
    keyword tolerance the first selection call dies with a TypeError."""
    from shared.llm_gemini_3 import Gemini3Client

    params = inspect.signature(Gemini3Client.generate_structured_content).parameters
    assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
