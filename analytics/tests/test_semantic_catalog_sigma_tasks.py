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


def test_task_payload_shape():
    p = sigma_tasks.task_payload(
        _rec("active_serve_users", ratified="2026-07-28", definition="count of active serve users")
    )
    assert p.build_key == "active_serve_users@2026-07-28"
    assert "active_serve_users" in p.name
    assert p.name.startswith("Build in Sigma:")
    assert "count of active serve users" in p.markdown_description
    assert "2026-07-28" in p.markdown_description
    assert "active_serve_users@2026-07-28" in p.markdown_description
    # Org copy rule: no em dashes, no emoji in generated copy.
    assert "—" not in p.name and "—" not in p.markdown_description


class _FakeClient:
    def __init__(self, existing_keys=()):
        self.existing = set(existing_keys)
        self.created = []

    def find_task_by_build_key(self, list_id, field_id, build_key):
        return "existing-id" if build_key in self.existing else None

    def create_task(self, list_id, payload, field_id):
        self.created.append(payload.build_key)
        return "new-id"


def test_sync_creates_task_for_newly_ratified():
    client = _FakeClient()
    result = sigma_tasks.sync(
        client, "901", "field1", [_rec("m", ratified=None)], [_rec("m", ratified="2026-07-28")]
    )
    assert client.created == ["m@2026-07-28"]
    assert result.created == ("m",)
    assert result.skipped == ()


def test_sync_is_idempotent_when_task_already_exists():
    # Same transition seen again on a workflow re-run: ClickUp already has the task.
    client = _FakeClient(existing_keys={"m@2026-07-28"})
    result = sigma_tasks.sync(
        client, "901", "field1", [_rec("m", ratified=None)], [_rec("m", ratified="2026-07-28")]
    )
    assert client.created == []
    assert result.created == ()
    assert result.skipped == ("m",)


def test_sync_noop_when_nothing_newly_ratified():
    client = _FakeClient()
    result = sigma_tasks.sync(
        client, "901", "field1", [_rec("m", ratified="2026-07-28")], [_rec("m", ratified="2026-07-28")]
    )
    assert client.created == []
    assert result == sigma_tasks.SyncResult(created=(), skipped=())
