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


def test_parse_anchors_normalises_missing_qualifiers():
    legs = parse_anchors(SEM_DOC)["win_active_candidates_30d"]
    assert legs == [
        {"event": "Viewed", "path": "/dashboard", "era": None, "excluding": {}},
        {
            "event": "Dashboard - Campaign Plan Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
        },
    ]


def test_parse_anchors_carries_a_property_exclusion():
    # A consumer that cannot see the exclusion reports the leg as wider than the
    # metric counts, which is how a self-report leg hid inside an OKR.
    doc = {
        "metrics": [
            {
                "name": "m",
                "config": {"meta": {"anchored_on": [{"event": "E", "excluding": {"method": "manual"}}]}},
            }
        ]
    }
    assert parse_anchors(doc)["m"] == [
        {"event": "E", "path": None, "era": None, "excluding": {"method": "manual"}}
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
    # Pinned by exact string, historical legs included: dropping a dead name silently
    # rewrites the metric's history, so changing this list has to be deliberate.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_active_candidates_30d"]
    assert legs == [
        {"event": "Viewed", "path": "/dashboard", "era": None, "excluding": {}},
        {
            "event": "Dashboard - Candidate Dashboard Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
        },
        {
            "event": "Dashboard - Campaign Plan Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
        },
        {
            "event": "Campaign Plan - Campaign Tracker Viewed",
            "path": None,
            "era": None,
            "excluding": {},
        },
    ]
    live = [leg for leg in legs if leg["era"] != "historical"]
    assert len(live) == 2  # the path leg plus the current named event


def test_activated_metrics_declare_the_exact_event_string():
    # Pinned by exact string for the same reason the dashboard anchor is: the outreach
    # surface has been rebuilt twice and each rebuild retired the leg the number was
    # computed from, so widening or narrowing this list has to be deliberate.
    win_doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    serve_doc = yaml.safe_load((MODELS / "sem_analytics__users_serve.yml").read_text())
    win_legs = parse_anchors(win_doc)["win_activated_users"]
    serve_legs = parse_anchors(serve_doc)["activated_serve_users"]
    assert [leg["event"] for leg in win_legs] == [
        "Voter Outreach - Campaign Completed",
        "Robocall - Scheduled",
        "Outreach - Phone Banking: Complete",
    ]
    assert [leg["event"] for leg in serve_legs] == ["Serve Onboarding - SMS Poll Sent"]


def test_win_activation_excludes_self_reported_outreach():
    # Self-report shares the Campaign Completed name and, once the in-product leg
    # stopped firing, was carrying the OKR on its own. The exclusion is the whole
    # reason the metric means "a send this product made".
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_activated_users"]
    by_event = {leg["event"]: leg for leg in legs}
    assert by_event["Voter Outreach - Campaign Completed"]["excluding"] == {"method": "manual"}
    # No other leg carries an exclusion, so a stray one fails loudly here.
    assert [leg["event"] for leg in legs if leg["excluding"]] == ["Voter Outreach - Campaign Completed"]


def test_win_activation_declares_no_preparation_event():
    # Building an audience, creating a call list and downloading a call sheet are
    # preparation to reach voters, not outreach. They are the tempting proxies for
    # the three channels that still have no send terminal.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    events = {leg["event"] for leg in parse_anchors(doc)["win_activated_users"]}
    assert events.isdisjoint(
        {
            "Voter Data - List Created",
            "Voter Outreach - Phone Banking Call List Created",
            "Voter Outreach - Phone Banking Call Sheet Downloaded",
            "Door Knocking - List Created",
            # Per-call, not per-campaign: the same predicate feeds campaigns_sent.
            "Outreach - Phone Banking: Call Logged",
        }
    )
