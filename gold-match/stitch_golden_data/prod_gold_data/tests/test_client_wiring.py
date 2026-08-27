"""Injection and CLI wiring: the Bedrock stack is the CLI default, the
Gemini defaults stay byte-preserved and dormant, and the prompt cache pins
the registry version."""

from unittest.mock import MagicMock, patch

import pytest

from stitch_golden_data.prod_gold_data.l2_br_matcher import (
    L2BrMatcher,
    PINNED_PROMPT_VERSION,
    _build_clients,
    _parse_args,
)


@pytest.fixture
def patched_deps():
    with (
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.DatabricksClient"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.Gemini3Client") as gem_llm,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.GeminiEmbeddingClient") as gem_emb,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.init_braintrust"),
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.cache_prompt") as cache,
    ):
        yield {"llm_cls": gem_llm, "emb_cls": gem_emb, "cache_prompt": cache}


def test_injected_clients_bypass_gemini_construction(patched_deps):
    emb, llm = MagicMock(name="emb"), MagicMock(name="llm")
    matcher = L2BrMatcher(embedding_client=emb, llm=llm)
    assert matcher.embedding_client is emb
    assert matcher.llm is llm
    patched_deps["llm_cls"].assert_not_called()
    patched_deps["emb_cls"].assert_not_called()


def test_default_construction_is_byte_preserved(patched_deps):
    L2BrMatcher()
    patched_deps["emb_cls"].assert_called_once_with(max_retries=11, base_delay=1.0)
    kwargs = patched_deps["llm_cls"].call_args.kwargs
    assert kwargs["max_connections"] == 1200
    assert kwargs["max_keepalive_connections"] == 300
    assert kwargs["max_retries"] == 11
    assert kwargs["default_temperature"] == 0.0


def test_prompt_cache_pins_version(patched_deps):
    L2BrMatcher()
    patched_deps["cache_prompt"].assert_called_once_with(
        "stitch-golden-data-matcher", version=PINNED_PROMPT_VERSION
    )


def test_cli_default_is_bedrock():
    assert _parse_args([]).model_config == "bedrock"
    assert _parse_args(["--model-config", "gemini"]).model_config == "gemini"
    assert _parse_args(["--model-config", "bedrock-nova"]).model_config == "bedrock-nova"


def test_build_clients_configs():
    with (
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.BedrockEmbeddingClient") as emb_cls,
        patch("stitch_golden_data.prod_gold_data.l2_br_matcher.BedrockStructuredContentClient") as llm_cls,
    ):
        _build_clients("bedrock")
        emb_cls.assert_called_once_with(model="titan")
        llm_cls.assert_called_once_with()

        emb_cls.reset_mock()
        llm_cls.reset_mock()
        _build_clients("bedrock-nova")
        emb_cls.assert_called_once_with(model="nova", requests_per_minute=2000, tokens_per_minute=None)
        llm_cls.assert_called_once_with()

    assert _build_clients("gemini") == (None, None)
