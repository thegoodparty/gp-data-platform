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
        {
            "event": "Viewed",
            "path": "/dashboard",
            "era": None,
            "excluding": {},
            "paywalled": False,
            "unit": None,
        },
        {
            "event": "Dashboard - Campaign Plan Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
            "paywalled": False,
            "unit": None,
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
        {
            "event": "E",
            "path": None,
            "era": None,
            "excluding": {"method": "manual"},
            "paywalled": False,
            "unit": None,
        }
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
        "win_product_output_users",
        "activated_serve_users",
    }


def test_dashboard_anchor_keeps_the_path_leg_live_and_the_dead_names_historical():
    # Pinned by exact string, historical legs included: dropping a dead name silently
    # rewrites the metric's history, so changing this list has to be deliberate.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_active_candidates_30d"]
    assert legs == [
        {
            "event": "Viewed",
            "path": "/dashboard",
            "era": None,
            "excluding": {},
            "paywalled": False,
            "unit": None,
        },
        {
            "event": "Dashboard - Candidate Dashboard Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
            "paywalled": False,
            "unit": None,
        },
        {
            "event": "Dashboard - Campaign Plan Viewed",
            "path": None,
            "era": "historical",
            "excluding": {},
            "paywalled": False,
            "unit": None,
        },
        {
            "event": "Campaign Plan - Campaign Tracker Viewed",
            "path": None,
            "era": None,
            "excluding": {},
            "paywalled": False,
            "unit": None,
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
        "Outreach - Campaign Completed",
        "Voter Outreach - Campaign Scheduled",
        "Outreach - Phone Banking: Complete",
        "Door Knocking - Door Logged",
        "Outreach - Door Knocking Door Logged",
        "Outreach - Phone Banking: Call Logged",
        "Outreach - Phone Banking Call Logged",
    ]
    assert [leg["event"] for leg in serve_legs] == [
        "Serve Onboarding - SMS Poll Sent",
        "Outreach - Campaign Completed",
        "Outreach - Door Knocking Door Logged",
        "Outreach - Phone Banking Call Logged",
    ]


def test_win_activation_excludes_self_reported_outreach():
    # Self-report shares the Campaign Completed name and, once the in-product leg
    # stopped firing, was carrying the OKR on its own. The exclusion is the whole
    # reason the metric means "a send this product made".
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_activated_users"]
    by_event = {leg["event"]: leg for leg in legs}
    assert by_event["Voter Outreach - Campaign Completed"]["excluding"] == {"method": ["manual", "unknown"]}
    assert by_event["Outreach - Campaign Completed"]["excluding"]["method"] == ["manual", "unknown"]
    # Only the two names of the completion event carry self-report, so a method
    # exclusion anywhere else is a stray and fails loudly here.
    assert [leg["event"] for leg in legs if "method" in leg["excluding"]] == [
        "Voter Outreach - Campaign Completed",
        "Outreach - Campaign Completed",
    ]


def test_serve_activation_excludes_win_and_self_report():
    # The outreach events are shared with Win, so a Serve leg that forgot the product
    # exclusion would count an official's own campaign outreach as Serve activation.
    # The onboarding poll carries no product and needs none: only Serve fires it.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_serve.yml").read_text())
    for leg in parse_anchors(doc)["activated_serve_users"]:
        if leg["event"] == "Serve Onboarding - SMS Poll Sent":
            continue
        assert leg["excluding"].get("product") == "win", leg["event"]
    by_event = {leg["event"]: leg for leg in parse_anchors(doc)["activated_serve_users"]}
    # Both spellings of self-report: 'unknown' is the legacy one.
    assert by_event["Outreach - Campaign Completed"]["excluding"]["method"] == ["manual", "unknown"]


def test_win_activation_excludes_serve_on_every_leg_that_can_say_so():
    # Serve outreach does not count toward Win. Only events named since 2026-09-29
    # carry `product`, so every leg that is not historical must exclude it, except
    # the send terminal, whose emitters are all keyed on a Win campaign.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    for metric in ("win_activated_users", "win_product_output_users"):
        for leg in parse_anchors(doc)[metric]:
            if leg["era"] == "historical" or leg["event"] in (
                "Voter Outreach - Campaign Scheduled",
                "Candidate Website - Published",
                "Voter Data - List Exported",
            ):
                continue
            assert leg["excluding"].get("product") == "serve", f"{metric}: {leg['event']}"


def test_win_activation_marks_every_per_person_leg_as_contact():
    # A door or a call is one person reached. Unmarked, every call would count as
    # a campaign sent.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    contact = {leg["event"] for leg in parse_anchors(doc)["win_activated_users"] if leg["unit"] == "contact"}
    assert contact == {
        "Door Knocking - Door Logged",
        "Outreach - Door Knocking Door Logged",
        "Outreach - Phone Banking: Call Logged",
        "Outreach - Phone Banking Call Logged",
    }


def test_win_activation_declares_no_preparation_event():
    # Building an audience, creating a call list or a campaign, and downloading a
    # call sheet are preparation to reach voters, not outreach. Nobody has been
    # contacted yet, so none of them activates a user.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    events = {leg["event"] for leg in parse_anchors(doc)["win_activated_users"]}
    assert events.isdisjoint(
        {
            "Voter Data - List Created",
            "Voter Outreach - Phone Banking Call List Created",
            "Outreach - Phone Banking Call List Created",
            "Voter Outreach - Phone Banking Call Sheet Downloaded",
            "Outreach - Phone Banking Call Sheet Downloaded",
            "Door Knocking - List Created",
            "Outreach - Door Knocking List Created",
            "Outreach - Campaign Created",
            # Fires at robocall draft-create on an unpaid row, so it counted a
            # candidate who built a draft and never paid. The shared send
            # terminal replaced it at the pay commit.
            "Robocall - Scheduled",
        }
    )


def test_product_output_declares_the_exact_event_string():
    # Pinned for the same reason the outreach anchor is, plus one of its own: this
    # is the broader of two things both once called activation, and the point of
    # separating them is that neither list can move by accident.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_product_output_users"]
    assert [leg["event"] for leg in legs] == [
        "Candidate Website - Published",
        "Voter Outreach - Campaign Completed",
        "Outreach - Campaign Completed",
        "Voter Data - List Exported",
        "Voter Outreach - Phone Banking Call Sheet Downloaded",
        "Outreach - Phone Banking Call Sheet Downloaded",
        "Voter Outreach - Campaign Scheduled",
        "Door Knocking - Door Logged",
        "Outreach - Door Knocking Door Logged",
        "Outreach - Phone Banking: Call Logged",
        "Outreach - Phone Banking Call Logged",
    ]


def test_product_output_follows_the_okr_on_self_report_and_the_robocall_draft():
    # Where the outreach OKR has settled a question, product output follows it
    # rather than diverging. Self-report produced no output here, and the robocall
    # draft fires on an unpaid row, so nothing left the product in either case.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_product_output_users"]
    by_event = {leg["event"]: leg for leg in legs}
    assert by_event["Voter Outreach - Campaign Completed"]["excluding"] == {"method": ["manual", "unknown"]}
    assert "Robocall - Scheduled" not in by_event


def test_self_report_exclusion_is_one_set_across_win_and_serve():
    # Self-report is excluded in two sem files, and the Win legs once caught only
    # one of its two spellings. A new spelling has to land on every leg at once.
    excluded: dict[str, list[str]] = {}
    for name in ("sem_analytics__users_win.yml", "sem_analytics__users_serve.yml"):
        doc = yaml.safe_load((MODELS / name).read_text())
        for metric, legs in parse_anchors(doc).items():
            for leg in legs:
                method = leg["excluding"].get("method")
                if method is not None:
                    values = [method] if isinstance(method, str) else method
                    excluded[f"{metric}: {leg['event']}"] = sorted(values)
    assert excluded
    mismatched = {leg: values for leg, values in excluded.items() if values != ["manual", "unknown"]}
    assert not mismatched, f"method exclusions differ from ['manual', 'unknown']: {mismatched}"


def test_product_output_leaves_one_free_leg_for_use_as_a_label():
    # A label drawn from the paywalled legs teaches a model who paid and lets it
    # report that as who engaged. If every leg were marked paywalled the free
    # variant would read zero for everyone, so the floor is asserted, not assumed.
    doc = yaml.safe_load((MODELS / "sem_analytics__users_win.yml").read_text())
    legs = parse_anchors(doc)["win_product_output_users"]
    free = [leg["event"] for leg in legs if not leg["paywalled"]]
    assert free == ["Candidate Website - Published"]
