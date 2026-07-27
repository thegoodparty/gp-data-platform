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
    assert {"win_users", "active_serve_users", "goodparty_win_rate", "goodparty_cumulative_wins"} <= names


def test_write_then_check_is_idempotent(tmp_path):
    # Copy a target file into a tmp location, write, then check == clean.
    target = tmp_path / "canonical_metrics.md"
    target.write_text("# Canonical metrics\n\nintro\n")
    records = parse_semantic_tree(cli.SEM_ROOTS)
    cli.write_region(target, records)
    assert cli.region_is_current(target, records) is True


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
    assert "incomplete" in text.lower()
    assert "business" in text.lower()


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

    assert win_names == {"win_users", "goodparty_win_rate", "goodparty_cumulative_wins"}
    assert serve_names == {"active_serve_users"}
    assert "active_serve_users" not in win_names


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
