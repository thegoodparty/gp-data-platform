# tests/test_audit.py
"""Tests for audit false-negative 3-state classification."""

import pytest

from scripts.audit_false_negatives import _canonicalize_pair_key, _classify_pair


def test_canonicalize_pair_key():
    """Pair keys are sorted so id_l < id_r."""
    assert _canonicalize_pair_key("b", "a") == ("a", "b")
    assert _canonicalize_pair_key("a", "b") == ("a", "b")
    assert _canonicalize_pair_key("x", "x") == ("x", "x")


def test_classify_generated_and_kept():
    """Pair in pairwise_predictions -> generated_and_kept."""
    pairwise_keys = {("a", "b")}
    filtered_keys = set()
    assert (
        _classify_pair("a", "b", pairwise_keys, filtered_keys) == "generated_and_kept"
    )
    assert (
        _classify_pair("b", "a", pairwise_keys, filtered_keys) == "generated_and_kept"
    )


def test_classify_generated_but_filtered():
    """Pair in filtered_pairs but not pairwise -> generated_but_filtered."""
    pairwise_keys = set()
    filtered_keys = {("a", "b")}
    assert (
        _classify_pair("a", "b", pairwise_keys, filtered_keys)
        == "generated_but_filtered"
    )


def test_classify_never_generated():
    """Pair in neither file -> never_generated."""
    pairwise_keys = set()
    filtered_keys = set()
    assert _classify_pair("a", "b", pairwise_keys, filtered_keys) == "never_generated"


def test_classify_legacy_fallback_no_filtered_file():
    """When filtered_pairs.csv is missing, only 2 states are available."""
    pairwise_keys = {("a", "b")}
    assert _classify_pair("a", "b", pairwise_keys, None) == "generated_and_kept"
    assert _classify_pair("c", "d", pairwise_keys, None) == "never_generated"
