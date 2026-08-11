import json
from pathlib import Path

from semantic_catalog import cli
from semantic_catalog.parser import parse_semantic_tree

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_sem_roots_exist():
    assert cli.SEM_ROOTS, "no semantic-model roots configured"
    for root in cli.SEM_ROOTS:
        assert root.is_dir(), f"missing {root}"


def test_real_repo_parses_without_error():
    records = parse_semantic_tree(cli.SEM_ROOTS)
    names = {r.name for r in records}
    # The four metrics that exist today must all parse.
    assert {"win_users", "activated_serve_users", "goodparty_win_rate", "goodparty_cumulative_wins"} <= names


def test_write_then_check_is_idempotent(tmp_path):
    # Copy a target file into a tmp location, write, then check == clean.
    target = tmp_path / "canonical_metrics.md"
    target.write_text("# Canonical metrics\n\nintro\n")
    records = parse_semantic_tree(cli.SEM_ROOTS)
    cli.write_region(target, records)
    assert cli.region_is_current(target, records) is True


def _base_worktree(tmp_path, sidecar_body):
    """A minimal base tree: the fixture sem file plus its own ratification sidecar."""
    import shutil

    models = tmp_path / "dbt" / "project" / "models"
    models.mkdir(parents=True)
    fixture = Path(__file__).parent / "fixtures" / "semantic_catalog" / "sem_fixture__users_demo.yml"
    shutil.copy(fixture, models / fixture.name)
    sidecar = tmp_path / cli.RATIFICATIONS_RELPATH
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(sidecar_body)
    return tmp_path


def test_before_side_reads_the_base_trees_own_sidecar(tmp_path):
    # The load-bearing wiring: the sidecar lives outside the dbt tree, so the
    # before side has to resolve it inside the base worktree. Reading the
    # CURRENT sidecar instead would make every ratification compare equal to
    # itself, erasing the pending-to-dated edge from the Slack summary and
    # firing no Sigma build task.
    base = _base_worktree(tmp_path, "activated_users:\n  ratified: 2026-01-01\n  definition_sha: '0000000'\n")
    before, _ = cli._before_after(base)
    assert {r.name: r.ratified for r in before}["activated_users"] == "2026-01-01"


def test_before_side_is_all_pending_when_the_base_predates_the_sidecar(tmp_path):
    # Base commits older than DATA-2249 have no sidecar at all; that has to load
    # as "nothing ratified yet" rather than raise.
    base = _base_worktree(tmp_path, "")
    (base / cli.RATIFICATIONS_RELPATH).unlink()
    before, _ = cli._before_after(base)
    assert before and all(r.ratified is None for r in before)


def test_check_reports_a_bad_sidecar_as_a_config_error(tmp_path, monkeypatch, capsys):
    bad = tmp_path / "ratifications.yml"
    bad.write_text("no_such_metric:\n  ratified: 2026-08-05\n  definition_sha: '0000000'\n")
    monkeypatch.setattr(cli.ratifications, "DEFAULT_PATH", bad)
    rc = cli.main(["--check"])
    assert rc == 1
    assert "config error" in capsys.readouterr().err


def test_fingerprints_prints_quoted_hashes(capsys):
    # Quoted because the sidecar demands quotes: an all-digit hash left bare
    # would be read back as an integer.
    rc = cli.main(["--fingerprints"])
    assert rc == 0
    lines = capsys.readouterr().out.splitlines()
    assert any(line.startswith("win_users: '") and line.endswith("'") for line in lines)


def test_emit_slack_writes_rendered_message(tmp_path):
    out = tmp_path / "slack.txt"
    rc = cli.main(
        [
            "--emit-slack",
            str(out),
            "--pr-url",
            "http://pr/1",
            "--coverage",
            '{"data": true, "business": false}',
        ]
    )
    assert rc == 0
    text = out.read_text()
    assert "http://pr/1" in text
    assert ":warning: review coverage: data ✓ · business ✗" in text


def test_region_is_current_false_on_half_marked_file(tmp_path):
    # One marker present (corrupt/merge-conflict): --check should treat the
    # file as stale (return False), not crash with an uncaught ValueError.
    from semantic_catalog.md_catalog import BEGIN_MARK

    target = tmp_path / "canonical_metrics.md"
    target.write_text(f"# Canonical metrics\n\n{BEGIN_MARK}\nrows but no end marker\n")
    records = parse_semantic_tree(cli.SEM_ROOTS)
    assert cli.region_is_current(target, records) is False


def test_records_by_target_routes_each_metric_to_its_skill():
    # Per-skill projection: each product's cheat sheet gets only its own
    # metrics. Civics outcome metrics route to Win (outcomes.md lives there).
    records = parse_semantic_tree(cli.SEM_ROOTS)
    grouped = cli.records_by_target(records)
    win_names = {r.name for r in grouped[cli.MD_TARGET_BY_SKILL["win-analytics-knowledge"]]}
    serve_names = {r.name for r in grouped[cli.MD_TARGET_BY_SKILL["serve-analytics-knowledge"]]}

    assert win_names == {
        "win_users",
        "win_activated_users",
        "win_active_candidates_30d",
        "goodparty_win_rate",
        "goodparty_cumulative_wins",
    }
    assert serve_names == {"activated_serve_users"}
    assert "activated_serve_users" not in win_names


def test_records_by_target_raises_on_unmapped_sem_file():
    import pytest
    from semantic_catalog.records import MetricRecord

    stray = MetricRecord(
        name="stray",
        label="Stray",
        definition="x",
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=None,
        ratified=None,
        detail_doc=None,
        retired=None,
        yaml_file="sem_unmapped__thing.yml",
        kind="metric",
    )
    with pytest.raises(ValueError):
        cli.records_by_target([stray])


def test_sync_sigma_tasks_skips_cleanly_without_token(capsys, monkeypatch):
    monkeypatch.delenv("CLICKUP_TASK_TOKEN", raising=False)
    rc = cli.main(["--sync-sigma-tasks"])
    assert rc == 0
    assert "skipping" in capsys.readouterr().out.lower()


def test_sync_sigma_tasks_invokes_sync_when_token_present(capsys, monkeypatch):
    monkeypatch.setenv("CLICKUP_TASK_TOKEN", "tok")
    seen = {}

    def fake_sync(client, list_id, field_id, before, after, assignee_ids=()):
        seen["list_id"] = list_id
        seen["field_id"] = field_id
        seen["assignee_ids"] = assignee_ids
        from semantic_catalog.sigma_tasks import CreatedTask, SyncResult

        return SyncResult(
            created=(CreatedTask(metric_name="m", task_id="id-1", url="https://app.clickup.com/t/id-1"),),
            skipped=(),
        )

    monkeypatch.setattr(cli.sigma_tasks, "sync", fake_sync)
    rc = cli.main(["--sync-sigma-tasks"])
    assert rc == 0
    # list_id comes from the committed config, not a hardcoded literal in cli.py
    assert seen["list_id"] == "901326391561"
    # default assignee (Audrey) is read from the committed config, not hardcoded in cli.py
    assert seen["assignee_ids"] == (111975138,)
    assert "created 1" in capsys.readouterr().out.lower()


def test_sync_sigma_tasks_emits_created_json(tmp_path, monkeypatch):
    monkeypatch.setenv("CLICKUP_TASK_TOKEN", "tok")

    def fake_sync(client, list_id, field_id, before, after, assignee_ids=()):
        from semantic_catalog.sigma_tasks import CreatedTask, SyncResult

        return SyncResult(
            created=(
                CreatedTask(
                    metric_name="win_users",
                    task_id="abc123",
                    url="https://app.clickup.com/t/abc123",
                ),
            ),
            skipped=(),
        )

    monkeypatch.setattr(cli.sigma_tasks, "sync", fake_sync)
    out = tmp_path / "created_tasks.json"
    rc = cli.main(["--sync-sigma-tasks", "--emit-created", str(out)])
    assert rc == 0
    import json

    payload = json.loads(out.read_text())
    assert payload == [
        {"metric": "win_users", "task_id": "abc123", "url": "https://app.clickup.com/t/abc123"}
    ]


def test_reply_created_skips_cleanly_without_token(capsys, monkeypatch):
    monkeypatch.delenv("SLACK_APP_BOT_TOKEN", raising=False)
    monkeypatch.setenv("SLACK_TS", "1699.1")
    monkeypatch.setenv("SLACK_CHANNEL_ID", "C999")
    rc = cli.main(["--reply-created", "/does/not/exist.json"])
    assert rc == 0
    assert "skipping" in capsys.readouterr().out.lower()


def test_reply_created_invokes_reply_in_thread(tmp_path, monkeypatch):
    monkeypatch.setenv("SLACK_APP_BOT_TOKEN", "tok")
    monkeypatch.setenv("SLACK_TS", "1699.1")
    monkeypatch.setenv("SLACK_CHANNEL_ID", "C999")
    seen = {}

    def fake_reply(token, channel, thread_ts, tasks):
        seen.update(token=token, channel=channel, thread_ts=thread_ts, tasks=tasks)

    monkeypatch.setattr(cli.slack_reply, "reply_in_thread", fake_reply)
    import json

    p = tmp_path / "created.json"
    p.write_text(json.dumps([{"metric": "win_users", "task_id": "a", "url": "https://app.clickup.com/t/a"}]))
    rc = cli.main(["--reply-created", str(p)])
    assert rc == 0
    assert seen["token"] == "tok"
    assert seen["channel"] == "C999"
    assert seen["thread_ts"] == "1699.1"
    assert seen["tasks"][0]["metric"] == "win_users"


def _reviews_file(tmp_path, reviews):
    p = tmp_path / "reviews.json"
    p.write_text(json.dumps(reviews))
    return p


def _isolated_sidecar(tmp_path, monkeypatch, body=""):
    """Point the generator at a throwaway sidecar so tests never touch the repo's."""
    sidecar = tmp_path / "ratifications.yml"
    sidecar.write_text(body)
    monkeypatch.setattr(cli.ratifications, "DEFAULT_PATH", sidecar)
    return sidecar


def test_record_writes_nothing_when_a_review_group_is_missing(tmp_path, monkeypatch, capsys):
    sidecar = _isolated_sidecar(tmp_path, monkeypatch)
    reviews = _reviews_file(
        tmp_path,
        [{"login": "amanda847", "state": "APPROVED", "submitted_at": "2026-08-07T10:00:00Z"}],
    )
    out = tmp_path / "recorded.json"
    rc = cli.main(
        [
            "--record-ratifications",
            "--reviews",
            str(reviews),
            "--data-members",
            "danpelota",
            "--business-members",
            "amanda847",
            "--pr-number",
            "800",
            "--emit-recorded",
            str(out),
        ]
    )
    assert rc == 0
    assert sidecar.read_text() == "", "an uncovered merge must record nothing"
    assert json.loads(out.read_text())["metrics"] == []
    assert "coverage incomplete" in capsys.readouterr().out.lower()


def test_record_writes_nothing_without_a_base_tree(tmp_path, monkeypatch, capsys):
    # Both groups approved, so coverage is complete, but with no base tree there
    # is no way to tell which definition moved. Recording anyway would sign off
    # every pending metric, including bystanders nobody reviewed.
    sidecar = _isolated_sidecar(tmp_path, monkeypatch)
    reviews = _reviews_file(
        tmp_path,
        [
            {"login": "amanda847", "state": "APPROVED", "submitted_at": "2026-08-07T10:00:00Z"},
            {"login": "danpelota", "state": "APPROVED", "submitted_at": "2026-08-07T11:00:00Z"},
        ],
    )
    out = tmp_path / "recorded.json"
    rc = cli.main(
        [
            "--record-ratifications",
            "--reviews",
            str(reviews),
            "--data-members",
            "danpelota",
            "--business-members",
            "amanda847",
            "--pr-number",
            "800",
            "--emit-recorded",
            str(out),
        ]
    )
    assert rc == 0
    assert sidecar.read_text() == "", "no base tree must record nothing"
    assert json.loads(out.read_text())["metrics"] == []
    assert "no base tree" in capsys.readouterr().out.lower()


def test_record_writes_nothing_when_a_base_dir_is_missing_on_disk(tmp_path, monkeypatch, capsys):
    # A --base-dir that does not exist parses as an empty tree, which is the same
    # bystander hazard as omitting it entirely.
    sidecar = _isolated_sidecar(tmp_path, monkeypatch)
    reviews = _reviews_file(
        tmp_path,
        [
            {"login": "amanda847", "state": "APPROVED", "submitted_at": "2026-08-07T10:00:00Z"},
            {"login": "danpelota", "state": "APPROVED", "submitted_at": "2026-08-07T11:00:00Z"},
        ],
    )
    rc = cli.main(
        [
            "--record-ratifications",
            "--reviews",
            str(reviews),
            "--data-members",
            "danpelota",
            "--business-members",
            "amanda847",
            "--pr-number",
            "800",
            "--base-dir",
            str(tmp_path / "does-not-exist"),
        ]
    )
    assert rc == 0
    assert sidecar.read_text() == ""
    assert "no base tree" in capsys.readouterr().out.lower()


def test_record_writes_nothing_when_the_merge_changed_no_definition(tmp_path, monkeypatch, capsys):
    # Both groups approved, but every metric is already ratified and untouched,
    # so the merge earned nothing.
    _isolated_sidecar(tmp_path, monkeypatch)
    reviews = _reviews_file(
        tmp_path,
        [
            {"login": "amanda847", "state": "APPROVED", "submitted_at": "2026-08-07T10:00:00Z"},
            {"login": "danpelota", "state": "APPROVED", "submitted_at": "2026-08-07T11:00:00Z"},
        ],
    )
    out = tmp_path / "recorded.json"
    # No --base-dir, so `before` is empty and every current metric looks new;
    # guard against that by pointing base-dir at the real tree (identical to HEAD).
    rc = cli.main(
        [
            "--record-ratifications",
            "--reviews",
            str(reviews),
            "--data-members",
            "danpelota",
            "--business-members",
            "amanda847",
            "--pr-number",
            "800",
            "--base-dir",
            str(REPO_ROOT),
            "--emit-recorded",
            str(out),
        ]
    )
    assert rc == 0
    assert json.loads(out.read_text())["metrics"] == []


def test_record_emits_the_pr_body_only_when_something_was_earned(tmp_path, monkeypatch):
    _isolated_sidecar(tmp_path, monkeypatch)
    reviews = _reviews_file(
        tmp_path, [{"login": "amanda847", "state": "APPROVED", "submitted_at": "2026-08-07T10:00:00Z"}]
    )
    body = tmp_path / "body.md"
    cli.main(
        [
            "--record-ratifications",
            "--reviews",
            str(reviews),
            "--data-members",
            "danpelota",
            "--business-members",
            "amanda847",
            "--pr-number",
            "800",
            "--emit-recorded",
            str(tmp_path / "recorded.json"),
            "--emit-pr-body",
            str(body),
        ]
    )
    assert not body.exists(), "no PR body when there is no PR to open"
