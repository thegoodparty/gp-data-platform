from pathlib import Path

import yaml
from semantic_catalog.anchors import parse_anchors

SEM_DOC = {
    "metrics": [
        {
            "name": "win_active_candidates_30d",
            "config": {
                "meta": {
                    "anchored_on": [
                        {"event": "Viewed", "path": "/dashboard"},
                        {"event": "Dashboard - Campaign Plan Viewed", "era": "historical"},
                    ]
                }
            },
        },
        {"name": "win_users", "config": {"meta": {"owner": "semantic-layer-data"}}},
    ]
}


def test_parse_anchors_returns_only_metrics_that_declare_one():
    anchors = parse_anchors(SEM_DOC)
    assert list(anchors) == ["win_active_candidates_30d"]


def test_parse_anchors_normalises_missing_path_and_era_to_none():
    legs = parse_anchors(SEM_DOC)["win_active_candidates_30d"]
    assert legs == [
        {"event": "Viewed", "path": "/dashboard", "era": None},
        {"event": "Dashboard - Campaign Plan Viewed", "path": None, "era": "historical"},
    ]


def test_parse_anchors_rejects_a_leg_without_an_event():
    doc = {"metrics": [{"name": "m", "config": {"meta": {"anchored_on": [{"path": "/x"}]}}}]}
    try:
        parse_anchors(doc)
    except ValueError as exc:
        assert "event" in str(exc)
    else:
        raise AssertionError("expected ValueError")


MODELS = Path(__file__).resolve().parents[2] / "dbt/project/models/marts/analytics"


def test_real_sem_files_declare_anchors_for_the_okr_metrics():
    declared = {}
    for name in ("sem_analytics__users_win.yml", "sem_analytics__users_serve.yml"):
        declared.update(parse_anchors(yaml.safe_load((MODELS / name).read_text())))
    assert set(declared) == {
        "win_active_candidates_30d",
        "win_activated_users",
        "activated_serve_users",
    }


def test_dashboard_anchor_keeps_the_path_leg_live_and_the_dead_names_historical():
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_active_candidates_30d"]
    live = [leg for leg in legs if leg["era"] != "historical"]
    assert {"event": "Viewed", "path": "/dashboard", "era": None} in live
    assert len(live) == 2  # the path leg plus the current named event
