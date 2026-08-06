from pathlib import Path

from semantic_catalog.parser import parse_semantic_file

FIXTURE = Path(__file__).parent / "fixtures" / "semantic_catalog" / "sem_fixture__users_demo.yml"


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
    assert m.ratified == "2026-07-24"
    assert m.detail_doc == "engagement.md"
    assert m.retired is None
    assert m.filter is not None
    assert "is_activated" in m.dimensions


def test_pending_metric_has_no_ratified():
    recs = _by_name(parse_semantic_file(FIXTURE))
    m = recs["demo_users"]
    assert m.owner is None
    assert m.ratified is None
    assert m.detail_doc is None


def test_parses_exposure_as_record():
    recs = _by_name(parse_semantic_file(FIXTURE))
    e = recs["demo_external_retention"]
    assert e.kind == "exposure"
    assert e.source == "https://example.amplitude.com/chart/abc123"
    assert e.definition == "Share of activated users returning in week one."
    assert e.ratified == "2026-07-20"
