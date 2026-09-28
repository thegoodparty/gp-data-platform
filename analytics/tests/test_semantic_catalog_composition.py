import json

from semantic_catalog.composition import evaluate, main


def test_both_groups_covered():
    cov = evaluate({"alice", "charlie"}, {"alice", "bob"}, {"charlie"})
    assert cov == {"data": True, "business": True}


def test_missing_business():
    cov = evaluate({"alice"}, {"alice", "bob"}, {"charlie"})
    assert cov == {"data": True, "business": False}


def test_case_insensitive_logins():
    cov = evaluate({"Alice"}, {"alice"}, {"charlie"})
    assert cov["data"] is True


def test_bot_approval_never_counts_even_as_a_team_member():
    # delegate-reviewer[bot] approves nearly every governed PR. Excluding bots
    # explicitly, rather than relying on them never being added to a team,
    # keeps the two-group gate from becoming satisfiable by automation alone.
    cov = evaluate({"delegate-reviewer[bot]"}, {"delegate-reviewer[bot]", "alice"}, {"charlie"})
    assert cov == {"data": False, "business": False}


def test_human_approval_alongside_a_bot_still_counts():
    cov = evaluate({"delegate-reviewer[bot]", "alice"}, {"alice"}, {"charlie"})
    assert cov["data"] is True


def test_main_prints_coverage_json(capsys, monkeypatch):
    monkeypatch.setenv("APPROVERS", "Alice,charlie")
    monkeypatch.setenv("DATA", "alice,bob")
    monkeypatch.setenv("BIZ", "charlie")

    exit_code = main()

    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == {"data": True, "business": True}


def test_main_empty_env_vars_treated_as_no_members(capsys, monkeypatch):
    monkeypatch.delenv("APPROVERS", raising=False)
    monkeypatch.delenv("DATA", raising=False)
    monkeypatch.delenv("BIZ", raising=False)

    exit_code = main()

    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == {"data": False, "business": False}
