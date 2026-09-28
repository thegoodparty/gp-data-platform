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
        assert rec.rule_approved is None and rec.build_approved is None
        assert rec.rule_stale is False and rec.build_stale is False


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
    # Back then one date certified the whole definition, so it reads onto BOTH
    # halves, marked legacy and stale: nothing under the two-seal scheme has
    # been signed for it.
    legacy = parse_semantic_file(path, legacy_ratified=True)[0]
    assert legacy.rule_approved == legacy.build_approved == "2026-07-24"
    assert legacy.seal_scheme == "legacy"


def test_tree_joins_sidecar_sign_offs(tmp_path):
    from semantic_catalog import ratifications

    activated = _by_name(parse_semantic_file(FIXTURE))["activated_users"]
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text(
        f"activated_users:\n"
        f"  business:\n    approved: 2026-07-24\n"
        f"    rule_sha: '{ratifications.rule_sha(activated)}'\n"
        f"  data:\n    approved: 2026-08-01\n"
        f"    build_sha: '{ratifications.build_sha(activated)}'\n"
        f"    value_at_signing: 1260\n"
    )
    recs = _by_name(parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar))
    assert recs["activated_users"].rule_approved == "2026-07-24"
    assert recs["activated_users"].build_approved == "2026-08-01"
    assert recs["activated_users"].rule_stale is False
    assert recs["activated_users"].build_stale is False
    # Unlisted metrics stay pending.
    assert recs["demo_users"].rule_approved is None


def test_tree_flags_a_sign_off_whose_definition_moved(tmp_path):
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text("activated_users:\n  business:\n    approved: 2026-07-24\n    rule_sha: '0000000'\n")
    recs = _by_name(parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar))
    assert recs["activated_users"].rule_stale is True


def test_tree_rejects_a_sidecar_key_matching_no_metric(tmp_path):
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text("typoed_name:\n  business:\n    approved: 2026-07-24\n    rule_sha: '0000000'\n")
    with pytest.raises(ValueError, match="typoed_name"):
        parse_semantic_tree([FIXTURE_DIR], ratifications_path=sidecar)


def test_a_rule_that_names_a_declared_event_fails_the_parse(tmp_path):
    # The layers only stay separable if the rule stays out of the
    # implementation. A rule naming an event means the business group is being
    # asked to rule on instrumentation, and the two halves can no longer go
    # stale for different reasons.
    path = tmp_path / "sem_stray__leaky.yml"
    path.write_text(
        "metrics:\n"
        "  - name: leaky\n"
        "    description: A metric whose rule reaches into layer 2.\n"
        "    config:\n"
        "      meta:\n"
        "        business_rule:\n"
        "          counts: A user counts on Voter Outreach - Campaign Completed.\n"
        "        anchored_on:\n"
        "          - event: Voter Outreach - Campaign Completed\n"
    )
    with pytest.raises(ValueError, match="Voter Outreach - Campaign Completed"):
        parse_semantic_file(path)


def test_the_parser_carries_both_sealed_layers(tmp_path):
    path = tmp_path / "sem_stray__layered.yml"
    path.write_text(
        "metrics:\n"
        "  - name: layered\n"
        "    description: prose\n"
        "    type_params:\n"
        "      measure: user_count\n"
        "    config:\n"
        "      meta:\n"
        "        business_rule:\n"
        "          counts: A user counts once they have come back.\n"
        "        anchored_on:\n"
        "          - event: Viewed\n"
        "            path: /dashboard\n"
    )
    rec = parse_semantic_file(path)[0]
    assert rec.business_rule.startswith("counts: A user counts once they have come back.")
    assert rec.anchored_on == "event=Viewed path=/dashboard"
    assert rec.measure == "user_count"
