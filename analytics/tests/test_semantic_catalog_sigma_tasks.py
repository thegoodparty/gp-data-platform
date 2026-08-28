from semantic_catalog import sigma_tasks
from semantic_catalog.records import MetricRecord


def _rec(name, ratified=None, retired=None, definition="def", stale=False):
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
        ratified_stale=stale,
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


def test_newly_ratified_ignores_a_stale_sign_off():
    # A date whose fingerprint no longer matches certifies a definition that has
    # since changed. Firing here would tell someone to build a definition nobody
    # approved, and the task text renders the date with no stale marker, so the
    # catalog's warning would not travel with it.
    before = [_rec("m", ratified="2026-07-28")]
    after = [_rec("m", ratified="2026-08-15", stale=True)]
    assert sigma_tasks.newly_ratified(before, after) == []


def test_newly_ratified_fires_when_a_stale_fingerprint_is_corrected():
    # The date does not move here; only the fingerprint is repaired. The task
    # the stale entry never got has to arrive now, or it never would.
    before = [_rec("m", ratified="2026-08-15", stale=True)]
    after = [_rec("m", ratified="2026-08-15")]
    assert [r.name for r in sigma_tasks.newly_ratified(before, after)] == ["m"]


def test_newly_ratified_excludes_pending_and_retired():
    before = []
    after = [_rec("still_pending", ratified=None), _rec("dead", ratified="2026-07-28", retired="2026-07-30")]
    assert sigma_tasks.newly_ratified(before, after) == []


def test_build_key_is_name_at_definition_fingerprint():
    from semantic_catalog import ratifications

    rec = _rec("activated_serve_users", ratified="2026-07-28")
    assert sigma_tasks.build_key(rec) == f"activated_serve_users@{ratifications.definition_sha(rec)}"


def test_build_key_is_stable_when_only_the_date_is_corrected():
    # The whole point: re-dating a metric must not mint a rival build task for
    # work already done. Three of those landed on an assignee after DATA-2249.
    early = _rec("win_users", ratified="2026-08-03")
    corrected = _rec("win_users", ratified="2026-08-04")
    assert sigma_tasks.build_key(early) == sigma_tasks.build_key(corrected)


def test_build_key_changes_when_the_definition_changes():
    # A real redefinition IS new build work, so it must still get a task.
    old = _rec("win_users", ratified="2026-08-04", definition="old meaning")
    new = _rec("win_users", ratified="2026-08-04", definition="new meaning")
    assert sigma_tasks.build_key(old) != sigma_tasks.build_key(new)


def test_task_payload_shape():
    rec = _rec("active_serve_users", ratified="2026-07-28", definition="count of active serve users")
    p = sigma_tasks.task_payload(rec)
    expected_key = sigma_tasks.build_key(rec)
    assert p.build_key == expected_key
    assert "active_serve_users" in p.name
    assert p.name.startswith("Build in Sigma:")
    assert "count of active serve users" in p.markdown_description
    assert "2026-07-28" in p.markdown_description
    assert expected_key in p.markdown_description
    # Org copy rule: no em dashes, no emoji in generated copy.
    assert "—" not in p.name and "—" not in p.markdown_description


def test_task_payload_defaults_to_no_assignees():
    p = sigma_tasks.task_payload(_rec("m", ratified="2026-07-28"))
    assert p.assignee_ids == ()


def test_task_payload_carries_assignee_ids():
    p = sigma_tasks.task_payload(_rec("m", ratified="2026-07-28"), assignee_ids=(111975138,))
    assert p.assignee_ids == (111975138,)


def test_task_url_builds_clickup_web_link():
    assert sigma_tasks.task_url("abc123") == "https://app.clickup.com/t/abc123"


class _FakeClient:
    def __init__(self, existing_keys=()):
        self.existing = set(existing_keys)
        self.created = []  # records each TaskPayload passed to create_task

    def find_task_by_build_key(self, list_id, field_id, build_key):
        return "existing-id" if build_key in self.existing else None

    def create_task(self, list_id, payload, field_id):
        self.created.append(payload)
        # Deterministic id derived from the build key so url assertions are stable.
        return f"id-{payload.build_key}"


def test_sync_creates_task_for_newly_ratified():
    client = _FakeClient()
    after = [_rec("m", ratified="2026-07-28")]
    result = sigma_tasks.sync(client, "901", "field1", [_rec("m", ratified=None)], after)
    expected_key = sigma_tasks.build_key(after[0])
    assert [p.build_key for p in client.created] == [expected_key]
    assert len(result.created) == 1
    task = result.created[0]
    assert task.metric_name == "m"
    assert task.task_id == f"id-{expected_key}"
    assert task.url == f"https://app.clickup.com/t/id-{expected_key}"
    assert result.skipped == ()


def test_sync_passes_assignee_ids_through_to_the_payload():
    client = _FakeClient()
    sigma_tasks.sync(
        client,
        "901",
        "field1",
        [_rec("m", ratified=None)],
        [_rec("m", ratified="2026-07-28")],
        assignee_ids=(111975138,),
    )
    assert client.created[0].assignee_ids == (111975138,)


def test_sync_is_idempotent_when_task_already_exists():
    # Same transition seen again on a workflow re-run: ClickUp already has the task.
    after = [_rec("m", ratified="2026-07-28")]
    client = _FakeClient(existing_keys={sigma_tasks.build_key(after[0])})
    result = sigma_tasks.sync(client, "901", "field1", [_rec("m", ratified=None)], after)
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
