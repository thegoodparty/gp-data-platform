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
