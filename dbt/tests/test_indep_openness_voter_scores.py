"""Unit tests for the pure helpers of the per-voter openness-to-an-independent model.

They import the helpers straight from the dbt Python model file, which is why
`import mlflow` stays inside the functions that load models. The cut-point notebook
embeds the same helpers, so these also guard that cuts and bands are built alike.
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from dbt.project.models.intermediate.l2.int__indep_openness_voter_scores import (
    ARM_0OR1,
    ARM_2PLUS,
    FEATURES,
    FLAG_CUT,
    MIN_PRECINCT_CASTERS,
    PRECINCT_FEATURES,
    RACE_FEATURES,
    SCENARIOS,
    SEED_INCUMBENT,
    SEED_OPEN,
    _voter_feature_exprs,
    booster_digest,
    build_features_sql,
    cuts_by_state,
    flag_column,
    flagged_frame,
    flags,
    missing_l2_columns,
    output_schema,
    primary_ballot_columns,
    race_block,
    raw_frame,
    raw_score_column,
    raw_scores,
    scenario_arm,
    scoring_fingerprint,
)

_SEED = Path(__file__).resolve().parents[1] / "project" / "seeds" / "indep_openness_n_ref.csv"
_CYCLE = dict(
    election_date="2026-11-03",
    reg_cutoff="2026-11-15",
    primary_end_year=2026,
    caster_column="General_2024",
)


# ── features ─────────────────────────────────────────────────────────────────
def test_feature_groups_partition_the_45():
    voter = list(_voter_feature_exprs("2026-11-03", "2026-11-15", 2026))
    assert len(FEATURES) == len(set(FEATURES)) == 45
    assert len(RACE_FEATURES) == 11 and len(PRECINCT_FEATURES) == 5 and len(voter) == 29
    assert sorted(RACE_FEATURES + PRECINCT_FEATURES + voter) == sorted(FEATURES)


def test_features_sql_emits_every_non_race_feature():
    sql = build_features_sql("_l2", "_to_score", **_CYCLE)
    for name in set(FEATURES) - set(RACE_FEATURES):
        assert f"as {name}" in sql, name


def test_features_sql_precinct_fallback_and_casters():
    sql = build_features_sql("_l2", "_to_score", **_CYCLE)
    assert f"precinct_eco.n_casters >= {MIN_PRECINCT_CASTERS}" in sql
    assert "else statewide_eco." in sql
    # Aggregates read the full state; only the scored relation shrinks.
    assert "from _l2\n            where General_2024" in sql
    assert "from _to_score" in sql


def test_features_sql_nh_precinct_key_uses_towns_and_wards():
    sql = build_features_sql("_l2", "_to_score", **_CYCLE)
    assert "when state_postal_code = 'NH'" in sql
    assert "City_Ward" in sql and "Town_Ward" in sql
    assert "Residence_Addresses_City" not in sql


def test_features_sql_uses_cycle_dates():
    sql = build_features_sql("_l2", "_to_score", "2028-11-07", "2028-11-18", 2028, "General_2026")
    assert "'2028-11-07'" in sql and "'2028-11-18'" in sql
    assert "PRI_BLT_2028" in sql and "where General_2026" in sql


def test_primary_columns_and_missing_check():
    assert primary_ballot_columns(2026)[0] == "PRI_BLT_2010"
    assert primary_ballot_columns(2026)[-1] == "PRI_BLT_2026"
    present = [*primary_ballot_columns(2026), "General_2024"]
    assert missing_l2_columns(present, 2026, "General_2024") == []
    assert missing_l2_columns(present[1:], 2026, "General_2024") == ["PRI_BLT_2010"]


# ── scenarios ────────────────────────────────────────────────────────────────
def test_ten_scenarios_route_by_major_party_field():
    assert len(SCENARIOS) == 10
    expect_0or1 = {"donly_inc", "donly_open", "ronly_inc", "ronly_open"}
    for name in SCENARIOS:
        assert scenario_arm(name) == (ARM_0OR1 if name in expect_0or1 else ARM_2PLUS), name


def test_seeds_follow_seat_status():
    for name, s in SCENARIOS.items():
        assert s["seed"] == (SEED_OPEN if name.endswith("_open") else SEED_INCUMBENT), name


def test_race_block_values():
    rb = race_block("single_inc", 7161.0)
    total = 0.0505 * 7161
    assert rb["n_voters"] == 7161.0
    assert math.isclose(float(rb["log_total_raised"]), math.log(total + 1))
    assert math.isclose(float(rb["log_raised_per_voter"]), math.log((total + 1) / 7161))
    assert (rb["pct_incumbents_running"], rb["is_ind_incumbent"], rb["n_ind"]) == (1.0, 0.0, 1.0)
    multi = race_block("donly_multi", 7161.0)
    assert (multi["n_democrats"], multi["n_republicans"], multi["magnitude"]) == (3.0, 0.0, 3.0)
    assert set(rb) == set(RACE_FEATURES)


# ── flags ────────────────────────────────────────────────────────────────────
def test_flag_is_top_40_percent_and_ties_count():
    assert FLAG_CUT == "c60"
    got = flags([0.1, 0.59, 0.6, 0.79, 0.95], [0.2, 0.4, 0.6, 0.8])
    assert got.dtype == bool
    assert list(got) == [False, False, True, True, True]


class _FakeBooster:
    """Predicts one input column, so tests can see which features reached the model."""

    def __init__(self, feat_names, column, sign=1.0):
        self.idx = feat_names.index(column)
        self.sign = sign

    def predict(self, matrix):
        return self.sign * matrix[:, self.idx]


def _features_frame(states):
    pdf = pd.DataFrame({f: 0.5 for f in FEATURES if f not in RACE_FEATURES}, index=range(len(states)))
    pdf["LALVOTERID"] = [f"V{i}" for i in range(len(states))]
    pdf["state"] = states
    pdf["mean_age"] = np.arange(len(states), dtype=float)
    return pdf


def test_raw_scores_inject_state_n_ref_and_route():
    boosters = {
        ARM_2PLUS: _FakeBooster(FEATURES, "n_voters"),
        ARM_0OR1: _FakeBooster(FEATURES, "n_voters", sign=-1.0),
    }
    pdf = _features_frame(["WV", "CA", "WV"])
    scores = raw_scores(pdf, boosters, FEATURES, {"WV": 7161.0, "CA": 209509.0})
    assert list(scores["single_inc"]) == [7161.0, 209509.0, 7161.0]
    assert list(scores["donly_open"]) == [-7161.0, -209509.0, -7161.0]


def test_raw_scores_fail_on_unknown_state():
    boosters = {a: _FakeBooster(FEATURES, "n_voters") for a in (ARM_2PLUS, ARM_0OR1)}
    with pytest.raises(ValueError, match="No n_ref"):
        raw_scores(_features_frame(["WV", "PR"]), boosters, FEATURES, {"WV": 7161.0})


def test_flagged_frame_cuts_within_each_state_and_keeps_raw():
    boosters = {a: _FakeBooster(FEATURES, "mean_age") for a in (ARM_2PLUS, ARM_0OR1)}
    pdf = _features_frame(["WV", "WV", "CA", "CA"])  # mean_age 0, 1, 2, 3
    cuts = {
        "WV": {s: [0.2, 0.4, 0.6, 0.8] for s in SCENARIOS},  # c60 0.6: 0 -> False, 1 -> True
        "CA": {s: [10.0, 11.0, 12.0, 13.0] for s in SCENARIOS},  # c60 12: both False
    }
    out = flagged_frame(pdf, boosters, FEATURES, {"WV": 7161.0, "CA": 209509.0}, cuts)
    assert list(out.columns) == (
        ["LALVOTERID", "state"]
        + [raw_score_column(s) for s in SCENARIOS]
        + [flag_column(s) for s in SCENARIOS]
    )
    assert list(out[flag_column("multi_open")]) == [False, True, False, False]
    assert list(out[raw_score_column("multi_open")]) == [0.0, 1.0, 2.0, 3.0]
    assert out[flag_column("ronly_inc")].dtype == bool


# ── fingerprint ──────────────────────────────────────────────────────────────
_DIGESTS = {ARM_2PLUS: "aaaa", ARM_0OR1: "bbbb"}
_NREF = {"WV": 7161.0, "NH": 3762.0}


def _fp(digests=_DIGESTS, n_ref=_NREF, sql=None):
    return scoring_fingerprint(digests, n_ref, sql or build_features_sql("_l2", "_to_score", **_CYCLE))


def test_fingerprint_is_stable_and_ignores_whitespace():
    sql = build_features_sql("_l2", "_to_score", **_CYCLE)
    assert _fp() == _fp(sql=sql.replace("\n", "\n   "))
    assert _fp(n_ref={"NH": 3762, "WV": 7161}) == _fp()


@pytest.mark.parametrize(
    "change",
    [
        dict(digests={ARM_2PLUS: "cccc", ARM_0OR1: "bbbb"}),
        dict(n_ref={"WV": 7162.0, "NH": 3762.0}),
        dict(sql=build_features_sql("_l2", "_to_score", **{**_CYCLE, "election_date": "2026-11-04"})),
        dict(sql=build_features_sql("_l2", "_to_score", **{**_CYCLE, "caster_column": "General_2022"})),
    ],
)
def test_fingerprint_moves_with_anything_that_moves_a_score(change):
    assert _fp(**change) != _fp()


def test_booster_digest_is_content_based():
    assert booster_digest("tree text") == booster_digest("tree text")
    assert booster_digest("tree text") != booster_digest("tree text ")


# ── cut-point lookup ─────────────────────────────────────────────────────────
def _cut_rows(states=("WV", "NH"), fingerprint="fp1", **overrides):
    rows = []
    for st in states:
        for sc in SCENARIOS:
            row = dict(
                scoring_fingerprint=fingerprint, state=st, scenario=sc, c20=0.1, c40=0.2, c60=0.3, c80=0.4
            )
            row.update(overrides)
            rows.append(row)
    return pd.DataFrame(rows)


def test_cuts_by_state_valid():
    cuts, missing = cuts_by_state(_cut_rows(), ["NH", "WV"], "fp1")
    assert missing == []
    assert cuts["WV"]["single_inc"] == [0.1, 0.2, 0.3, 0.4]
    assert set(cuts) == {"NH", "WV"}


def test_cuts_by_state_reads_only_its_fingerprint():
    rows = pd.concat([_cut_rows(), _cut_rows(fingerprint="fp0", c20=0.0)])
    cuts, missing = cuts_by_state(rows, ["WV"], "fp1")
    assert missing == [] and cuts["WV"]["multi_inc"][0] == 0.1


def test_cuts_by_state_reports_missing_instead_of_raising():
    rows = pd.concat([_cut_rows(states=("WV",)), _cut_rows(states=("NH",)).iloc[:3]])
    cuts, missing = cuts_by_state(rows, ["NH", "WV", "VT"], "fp1")
    assert missing == ["NH", "VT"] and set(cuts) == {"WV"}
    _, missing = cuts_by_state(_cut_rows(), ["WV"], "new-fingerprint")
    assert missing == ["WV"]


@pytest.mark.parametrize(
    "rows,match",
    [
        (pd.concat([_cut_rows(), _cut_rows().iloc[:1]]), "Duplicate"),
        (_cut_rows(c40=0.05), "ascending"),
    ],
)
def test_cuts_by_state_rejects_corrupt_cuts(rows, match):
    with pytest.raises(ValueError, match=match):
        cuts_by_state(rows, ["NH", "WV"], "fp1")


def test_output_schema_lists_flags_and_raw_scores():
    schema = output_schema()
    for s in SCENARIOS:
        assert f"{flag_column(s)} boolean" in schema
        assert f"{raw_score_column(s)} double" in schema
    assert "scoring_fingerprint string" in schema


def test_raw_frame_columns():
    boosters = {a: _FakeBooster(FEATURES, "mean_age") for a in (ARM_2PLUS, ARM_0OR1)}
    out = raw_frame(_features_frame(["WV", "CA"]), boosters, FEATURES, {"WV": 7161.0, "CA": 209509.0})
    assert list(out.columns) == ["LALVOTERID", "state"] + [raw_score_column(s) for s in SCENARIOS]
    assert list(out[raw_score_column("single_open")]) == [0.0, 1.0]


# ── seed ─────────────────────────────────────────────────────────────────────
def test_n_ref_seed_covers_states_and_floor():
    seed = pd.read_csv(_SEED)
    assert len(seed) == 51 and seed["state"].is_unique
    assert seed["n_ref"].min() == 3762
    assert set(seed.loc[seed["n_ref"] == 3762, "state"]) == {"NH", "VT", "WY"}
