import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd

_MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "projects" / "win_topline_reporting" / "topline_report.py"
)
_spec = importlib.util.spec_from_file_location("topline_report", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
topline_report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(topline_report)

build_working_flags = topline_report.build_working_flags
build_report = topline_report.build_report

# Lands in the november_2025 bucket with a run date past the election, so
# d_eval == the election date and every signal below is comfortably <= it.
_ELECTION_DATE = date(2025, 11, 1)
_ASOF = date(2025, 11, 5)


def _stub_df(sub_confirmed=None, sub_canceled=None, has_stripe=False, general_election_date=_ELECTION_DATE):
    return pd.DataFrame(
        [
            {
                "user_created_date": date(2025, 6, 1),
                "general_election_date": general_election_date,
                "election_date": None,
                "has_stripe": has_stripe,
                "stripe_pro_at_d": False,
                "amp_upgrade_date": None,
                "hs_upgrade_date": None,
                "sub_confirmed_le_d": sub_confirmed,
                "sub_canceled_le_d": sub_canceled,
                "first_campaign_sent_date": None,
            }
        ]
    )


def test_fallback_confirm_after_cancel_requalifies():
    df = _stub_df(sub_confirmed=date(2025, 10, 1), sub_canceled=date(2025, 9, 1))
    out = build_working_flags(df, asof=_ASOF)
    assert out.loc[0, "pro_at_fallback"]
    assert not out.loc[0, "cancel_gated"]


def test_fallback_cancel_after_confirm_gates():
    df = _stub_df(sub_confirmed=date(2025, 9, 1), sub_canceled=date(2025, 10, 1))
    out = build_working_flags(df, asof=_ASOF)
    assert not out.loc[0, "pro_at_fallback"]
    assert out.loc[0, "cancel_gated"]


def test_cancel_only_no_pro():
    df = _stub_df(sub_confirmed=None, sub_canceled=date(2025, 10, 1))
    out = build_working_flags(df, asof=_ASOF)
    assert not out.loc[0, "pro_at_fallback"]


def test_dateless_user_never_qualifies_even_with_signal():
    df = _stub_df(sub_confirmed=date(2025, 10, 1), general_election_date=None)
    out = build_working_flags(df, asof=_ASOF)
    assert not out.loc[0, "pro_at_fallback"]


def test_same_day_confirm_and_cancel_not_gated():
    """The gate is `> sig`, not `>= sig`: a same-day cancel does not void a
    same-day confirm. Flipping the comparison would silently regress this."""
    same_day = date(2025, 10, 1)
    df = _stub_df(sub_confirmed=same_day, sub_canceled=same_day)
    out = build_working_flags(df, asof=_ASOF)
    assert out.loc[0, "pro_at_fallback"]
    assert not out.loc[0, "cancel_gated"]


def test_stripe_user_never_uses_fallback():
    df = _stub_df(sub_confirmed=date(2025, 10, 1), has_stripe=True)
    out = build_working_flags(df, asof=_ASOF)
    assert not out.loc[0, "pro_at_fallback"]


def _report_stub_df(rows):
    base = {
        "user_created_date": date(2025, 6, 1),
        "general_election_date": _ELECTION_DATE,
        "election_date": None,
        "has_stripe": False,
        "stripe_pro_at_d": False,
        "amp_upgrade_date": None,
        "hs_upgrade_date": None,
        "sub_confirmed_le_d": None,
        "sub_canceled_le_d": None,
        "first_campaign_sent_date": None,
        "is_pro": False,
        "is_activated": False,
        "n_candidacies": 1,
    }
    return pd.DataFrame([{**base, **overrides, "user_id": i} for i, overrides in enumerate(rows)])


def test_delta_table_added_and_removed_counts():
    """One user unaffected by the 2026-07-24 amendment, one newly qualifies
    via the Confirmed-only channel, one is newly gated off by a later Cancel
    that the prior (legacy-channel, no-gate) logic never looked at."""
    df = _report_stub_df(
        [
            {"amp_upgrade_date": date(2025, 8, 1)},
            {"sub_confirmed_le_d": date(2025, 9, 1)},
            {"amp_upgrade_date": date(2025, 8, 1), "sub_canceled_le_d": date(2025, 9, 1)},
        ]
    )
    flagged = build_working_flags(df, asof=_ASOF)
    _, _, _, delta, _ = build_report(flagged, asof=_ASOF)

    row = delta.loc["november_2025"]
    assert row["pro_at_election_prev_logic"] == 2
    assert row["pro_at_election_new_logic"] == 2
    assert row["delta"] == 0
    assert row["added_by_confirmed_evt"] == 1
    assert row["removed_by_cancel_gate"] == 1
