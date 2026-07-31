from semantic_catalog import sigma_tasks
from semantic_catalog.records import MetricRecord


def _rec(name, ratified=None, retired=None, definition="def"):
    return MetricRecord(
        name=name,
        label=name.replace("_", " ").title(),
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner="semantic-layer-business",
        ratified=ratified,
        detail_doc="engagement.md",
        retired=retired,
        yaml_file="sem_analytics__users_serve.yml",
        kind="metric",
    )


def test_newly_ratified_fires_on_pending_to_dated_transition():
    before = [_rec("m", ratified=None)]
    after = [_rec("m", ratified="2026-07-28")]
    got = [r.name for r in sigma_tasks.newly_ratified(before, after)]
    assert got == ["m"]


def test_newly_ratified_ignores_already_ratified_unchanged():
    before = [_rec("m", ratified="2026-07-28")]
    after = [_rec("m", ratified="2026-07-28")]
    assert sigma_tasks.newly_ratified(before, after) == []


def test_newly_ratified_fires_again_on_new_ratified_date():
    before = [_rec("m", ratified="2026-07-28")]
    after = [_rec("m", ratified="2026-08-15")]
    assert [r.name for r in sigma_tasks.newly_ratified(before, after)] == ["m"]


def test_newly_ratified_treats_brand_new_ratified_metric_as_new():
    before = []
    after = [_rec("m", ratified="2026-07-28")]
    assert [r.name for r in sigma_tasks.newly_ratified(before, after)] == ["m"]


def test_newly_ratified_excludes_pending_and_retired():
    before = []
    after = [_rec("still_pending", ratified=None), _rec("dead", ratified="2026-07-28", retired="2026-07-30")]
    assert sigma_tasks.newly_ratified(before, after) == []


def test_build_key_is_name_at_ratified_date():
    assert (
        sigma_tasks.build_key(_rec("active_serve_users", ratified="2026-07-28"))
        == "active_serve_users@2026-07-28"
    )
