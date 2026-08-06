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
