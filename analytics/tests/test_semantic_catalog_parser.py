from pathlib import Path

import pytest
from semantic_catalog.parser import parse_semantic_file, parse_semantic_tree

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "semantic_catalog"
FIXTURE = FIXTURE_DIR / "sem_fixture__users_demo.yml"


def _by_name(records):
    return {r.name: r for r in records}


def test_parses_metric_with_governance_meta():
    recs = _by_name(parse_semantic_file(FIXTURE))
    m = recs["activated_users"]
    assert m.kind == "metric"
    assert m.label == "Activated Users"
    assert m.definition.startswith("Count of users who have sent")
    assert m.metric_type == "simple"
    assert m.source == "ref('users_demo_base')"
    assert m.owner == "semantic-layer-business"
    assert m.detail_doc == "engagement.md"
    assert m.retired is None
    assert m.filter is not None
    assert "is_activated" in m.dimensions


def test_file_parse_carries_no_ratification():
    # A file parse yields the DEFINITION only. Sign-offs live in the sidecar and
    # are joined by parse_semantic_tree (DATA-2249).
    for rec in parse_semantic_file(FIXTURE):
        assert rec.ratified is None
        assert rec.ratified_stale is False


def test_pending_metric_has_no_owner_or_detail_doc():
    m = _by_name(parse_semantic_file(FIXTURE))["demo_users"]
    assert m.owner is None
    assert m.detail_doc is None


def test_parses_exposure_as_record():
    e = _by_name(parse_semantic_file(FIXTURE))["demo_external_retention"]
    assert e.kind == "exposure"
    assert e.source == "https://example.amplitude.com/chart/abc123"
    assert e.definition == "Share of activated users returning in week one."


def test_ratified_left_in_config_meta_is_a_hard_error(tmp_path):
    # The old home for the date. Failing loudly stops the habit coming back and
    # stops a metric reading pending while its yml claims otherwise.
    path = tmp_path / "sem_stray__legacy.yml"
    path.write_text(
        "metrics:\n"
        "  - name: legacy\n"
        "    description: A metric still carrying its old date.\n"
        "    config:\n"
        "      meta:\n"
        "        ratified: '2026-07-24'\n"
    )
    with pytest.raises(ValueError, match="config.meta.ratified"):
        parse_semantic_file(path)

    # ...but parsing HISTORY reads it, because back then that WAS the
    # ratification. Rejecting it would crash the publish job on the very merge
    # that introduces the sidecar, since its base commit predates it.
    legacy = parse_semantic_file(path, legacy_ratified=True)
    assert legacy[0].ratified == "2026-07-24"


def test_tree_joins_sidecar_sign_offs(tmp_path):
    from semantic_catalog import ratifications

    activated = _by_name(parse_semantic_file(FIXTURE))["activated_users"]
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text(
        f"activated_users:\n"
        f"  ratified: 2026-07-24\n"
        f"  definition_sha: '{ratifications.definition_sha(activated)}'\n"
    )
    recs = _by_name(parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar))
    assert recs["activated_users"].ratified == "2026-07-24"
    assert recs["activated_users"].ratified_stale is False
    # Unlisted metrics stay pending.
    assert recs["demo_users"].ratified is None


def test_tree_flags_a_sign_off_whose_definition_moved(tmp_path):
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text("activated_users:\n  ratified: 2026-07-24\n  definition_sha: '0000000'\n")
    recs = _by_name(parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar))
    assert recs["activated_users"].ratified_stale is True


def test_tree_rejects_a_sidecar_key_matching_no_metric(tmp_path):
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text("typoed_name:\n  ratified: 2026-07-24\n  definition_sha: '0000000'\n")
    with pytest.raises(ValueError, match="typoed_name"):
        parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar)
