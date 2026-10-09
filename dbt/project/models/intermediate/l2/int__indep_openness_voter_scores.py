"""Per-voter openness-to-an-independent flags from the state-leg independent model.

Scores every L2 voter as a "district of 1" under 10 fixed race scenarios with the
two ballot-caster (voted) state-leg models. For each scenario it keeps the raw score
and a true/false flag: true when the voter is in the top 40% of their state on that
score. One row per LALVOTERID; m_people_api__voter left-joins only the flags onto the
voter row, the same way turnout's int__voter_turnout_lgbm_voter_scores reaches the
product. The raw scores stay in the warehouse: their level is not meaningful on its
own, only their rank within the state.

Features (the 45 the models consume):
  - 11 race features are fixed by the scenario and the state's reference size n_ref
    (seed indep_openness_n_ref), so every voter in a state shares them.
  - 29 are the voter's own L2 values: the row-level analog of the district
    aggregates (AVG(x) -> x, a ratio -> the voter's own indicator).
  - 5 ecological features (hhi_ethnicity, partisan_imbalance, pct_reg_*) cannot be
    taken from one voter, so they come from the voter's precinct, aggregated over its
    ballot-casters. Precincts under 40 casters, or voters with no precinct key, take
    the statewide caster value instead.

Cut points are made during model development, not here. A research notebook scores
the whole voter file with these same helpers and writes each state's c20/c40/c60/c80
per scenario, keyed by a scoring fingerprint: a hash of both boosters' contents, the
n_ref seed, the scenarios and the feature SQL. This model computes the same
fingerprint from what it is about to score with and reads only cuts that match, so
flags are always cut on the same scores the cuts came from. The flag uses c60; the
other cuts are kept so analysis can rebuild finer bands.

The models follow an alias (@production by default). When the fingerprint changes
(a promoted model, a new seed or feature date), the next run rescores the whole voter
file; otherwise it scores only new LALVOTERIDs, so a voter's flag does not move
between changes. If no cuts exist yet for the new fingerprint, the run leaves the
table as it is and logs a warning rather than failing the monthly chain.

Scoring follows the turnout scorer's mapInPandas pattern, with one difference: these
boosters are a few MB, so the closure carries their text and no Volume staging is
needed. Workers cache the parsed boosters on their content digest.

Helpers are pure so the dbt tests and the cut-point notebook can use this file
without Spark or MLflow; those imports stay inside the functions that need them.
"""

import hashlib
import json
import math
import re

import numpy as np
import pandas as pd

# The 45 model features, as trained (tmp_voted45_features.json in the research).
FEATURES = [
    "n_democrats",
    "n_republicans",
    "n_ind",
    "magnitude",
    "pct_incumbents_running",
    "is_ind_incumbent",
    "ind_writein",
    "balloting_system_code",
    "log_total_raised",
    "log_raised_per_voter",
    "n_voters",
    "pct_reg_dem",
    "pct_reg_rep",
    "pct_reg_ind",
    "mean_age",
    "pct_age_34_66_at_election",
    "pct_age_18_33_at_election",
    "pct_income_under_50k",
    "mean_edu_years",
    "mean_persons_in_hh",
    "pct_moved_from",
    "mean_lor_corrected",
    "mean_reg_duration_yrs",
    "pct_recently_registered",
    "hhi_ethnicity",
    "area_pct_spanish_speaking",
    "pct_has_religion",
    "generations_in_hh",
    "pct_ind_or_mixed_hh",
    "mean_voting_perf_general",
    "mean_voting_perf_minor",
    "mean_hs_dropoff_only_top",
    "mean_hs_party_indep",
    "mean_hs_ticket_splitter_yes",
    "mean_hs_ideology_general_moderate",
    "partisan_imbalance",
    "mean_hs_third_party_support",
    "pct_weak_partisan_50",
    "und_x_general",
    "mean_hs_gig_worker_unlikely",
    "mean_hs_rfkjr_approval",
    "mean_hs_trump_approval",
    "pct_party_switcher",
    "pct_O",
    "pct_DR",
]

RACE_FEATURES = [
    "n_democrats",
    "n_republicans",
    "n_ind",
    "magnitude",
    "pct_incumbents_running",
    "is_ind_incumbent",
    "ind_writein",
    "balloting_system_code",
    "log_total_raised",
    "log_raised_per_voter",
    "n_voters",
]

PRECINCT_FEATURES = [
    "hhi_ethnicity",
    "partisan_imbalance",
    "pct_reg_dem",
    "pct_reg_rep",
    "pct_reg_ind",
]

# Below this many ballot-casters a precinct's aggregate is noise; fall back to statewide.
MIN_PRECINCT_CASTERS = 40

# Fundraising seed per registered voter. Seat status enters the model only through it.
SEED_INCUMBENT = 0.0505
SEED_OPEN = 0.1143

ARM_2PLUS = "2plus"
ARM_0OR1 = "0or1"
MODEL_NAMES = {
    ARM_2PLUS: "stateleg_ind_2plusmajorparty_voted",
    ARM_0OR1: "stateleg_ind_voted",
}

# Multi-seat races are scored at magnitude 3 whatever their seat count. The lone-party
# multi-seat scenarios are rare enough that they only take the incumbent seed.
SCENARIOS = {
    "single_inc": dict(n_democrats=1, n_republicans=1, magnitude=1, seed=SEED_INCUMBENT),
    "single_open": dict(n_democrats=1, n_republicans=1, magnitude=1, seed=SEED_OPEN),
    "multi_inc": dict(n_democrats=3, n_republicans=3, magnitude=3, seed=SEED_INCUMBENT),
    "multi_open": dict(n_democrats=3, n_republicans=3, magnitude=3, seed=SEED_OPEN),
    "donly_inc": dict(n_democrats=1, n_republicans=0, magnitude=1, seed=SEED_INCUMBENT),
    "donly_open": dict(n_democrats=1, n_republicans=0, magnitude=1, seed=SEED_OPEN),
    "ronly_inc": dict(n_democrats=0, n_republicans=1, magnitude=1, seed=SEED_INCUMBENT),
    "ronly_open": dict(n_democrats=0, n_republicans=1, magnitude=1, seed=SEED_OPEN),
    "donly_multi": dict(n_democrats=3, n_republicans=0, magnitude=3, seed=SEED_INCUMBENT),
    "ronly_multi": dict(n_democrats=0, n_republicans=3, magnitude=3, seed=SEED_INCUMBENT),
}

CUT_COLUMNS = ["c20", "c40", "c60", "c80"]
# The flag marks the top 40% of the state: at or above the 60th percentile.
FLAG_CUT = "c60"


def flag_column(scenario):
    return f"indep_openness_{scenario}"


def raw_score_column(scenario):
    return f"raw_{scenario}"


def scenario_arm(scenario):
    """The two specialists rank voters very differently on identical inputs, so route
    strictly on the scenario's major-party field size, as in training."""
    s = SCENARIOS[scenario]
    return ARM_2PLUS if s["n_democrats"] + s["n_republicans"] >= 2 else ARM_0OR1


def race_block(scenario, n_ref):
    """The 11 race features for a scenario. n_ref may be a scalar or an array (one
    value per voter, from the voter's state)."""
    s = SCENARIOS[scenario]
    n_ref = np.asarray(n_ref, dtype=float)
    total_raised = s["seed"] * n_ref
    return {
        "n_democrats": float(s["n_democrats"]),
        "n_republicans": float(s["n_republicans"]),
        "n_ind": 1.0,
        "magnitude": float(s["magnitude"]),
        "pct_incumbents_running": 1.0,
        "is_ind_incumbent": 0.0,
        "ind_writein": 0.0,
        "balloting_system_code": 0.0,
        "log_total_raised": np.log(total_raised + 1.0),
        "log_raised_per_voter": np.log((total_raised + 1.0) / n_ref),
        "n_voters": n_ref,
    }


def flags(scores, cuts):
    """cuts: [c20, c40, c60, c80]. A score equal to the cutoff is flagged."""
    return np.asarray(scores, dtype=float) >= cuts[CUT_COLUMNS.index(FLAG_CUT)]


# ── feature SQL ──────────────────────────────────────────────────────────────

# NH has no Precinct values at all; its voting units are towns and city wards, which L2
# splits across two complementary column pairs. Never substitute an address field.
PRECINCT_KEY_SQL = """
    case
        when state_postal_code = 'NH'
        then concat(
            County,
            '|',
            coalesce(
                nullif(concat(coalesce(City, ''), coalesce(City_Ward, '')), ''),
                nullif(concat(coalesce(Town_District, ''), coalesce(Town_Ward, '')), '')
            )
        )
        else concat(County, '|', Precinct)
    end
"""

_ETHNIC_GROUPS = [
    "European",
    "Hispanic and Portuguese",
    "Likely African-American",
    "East and South Asian",
]


def _ethnic_onehot_sql(group):
    if group is None:
        listed = ", ".join(f"'{g}'" for g in _ETHNIC_GROUPS)
        return (
            f"case when EthnicGroups_EthnicGroup1Desc not in ({listed}) "
            f"or EthnicGroups_EthnicGroup1Desc is null then 1.0 else 0.0 end"
        )
    return f"case when EthnicGroups_EthnicGroup1Desc = '{group}' then 1.0 else 0.0 end"


def _precinct_feature_aggs_sql():
    """The 5 ecological features as aggregates, identical to the district training SQL."""
    hhi = " + ".join(f"pow(avg({_ethnic_onehot_sql(g)}), 2)" for g in [*_ETHNIC_GROUPS, None])
    return {
        "hhi_ethnicity": hhi,
        "partisan_imbalance": (
            "abs(avg(hs_ideology_partisanship_partisanship_overall_party_dem_strong)"
            " - avg(hs_ideology_partisanship_partisanship_overall_party_gop_strong))"
        ),
        "pct_reg_dem": "avg(case when Parties_Description = 'Democratic' then 1.0 else 0.0 end)",
        "pct_reg_rep": "avg(case when Parties_Description = 'Republican' then 1.0 else 0.0 end)",
        "pct_reg_ind": (
            "avg(case when Parties_Description not in ('Democratic', 'Republican')"
            " or Parties_Description is null then 1.0 else 0.0 end)"
        ),
    }


def primary_ballot_columns(primary_end_year):
    return [f"PRI_BLT_{y}" for y in range(2010, int(primary_end_year) + 1)]


def required_l2_columns(primary_end_year, caster_column):
    return set(primary_ballot_columns(primary_end_year)) | {caster_column}


def missing_l2_columns(l2_columns, primary_end_year, caster_column):
    """Fail rather than drop a missing primary year: pct_O / pct_DR would silently
    change meaning."""
    return sorted(required_l2_columns(primary_end_year, caster_column) - set(l2_columns))


def _voter_feature_exprs(election_date, reg_cutoff, primary_end_year):
    """The 29 per-voter features: row-level forms of the district aggregates."""
    pri = primary_ballot_columns(primary_end_year)
    has_o = " or ".join(f"{c} = 'O'" for c in pri)
    has_d = " or ".join(f"{c} = 'D'" for c in pri)
    has_r = " or ".join(f"{c} = 'R'" for c in pri)
    age = f"datediff('{election_date}', bd_parsed) / 365.25"
    reg_ok = f"rd_parsed is not null and rd_parsed < to_date('{reg_cutoff}')"
    reg_years = f"datediff(to_date('{reg_cutoff}'), rd_parsed) / 365.25"
    lor = "ConsumerData_Length_Of_Residence_Code"
    return {
        "mean_age": f"case when bd_parsed is not null then {age} end",
        "pct_age_34_66_at_election": (
            f"case when bd_parsed is null then null when {age} between 34 and 66 then 1.0 else 0.0 end"
        ),
        "pct_age_18_33_at_election": (
            f"case when bd_parsed is null then null when {age} between 18 and 33 then 1.0 else 0.0 end"
        ),
        "pct_income_under_50k": (
            "case when try_cast(replace(ConsumerData_Estimated_Income_Amount, '$', '') as double)"
            " < 50000 then 1.0 else 0.0 end"
        ),
        "mean_edu_years": "try_cast(ConsumerData_AreaMedianEducationYears as double)",
        "mean_persons_in_hh": "try_cast(ConsumerData_Number_Of_Persons_in_HH as double)",
        "pct_moved_from": "case when Voters_MovedFrom_Date is not null then 1.0 else 0.0 end",
        "mean_lor_corrected": lor,
        "mean_reg_duration_yrs": f"case when {reg_ok} then {reg_years} end",
        "pct_recently_registered": (
            f"case when not ({reg_ok}) then null when {reg_years} <= 10 then 1.0 else 0.0 end"
        ),
        "area_pct_spanish_speaking": (
            "try_cast(replace(ConsumerData_AreaPcntHHSpanishSpeaking, '%', '') as double) / 100.0"
        ),
        "pct_has_religion": (
            "case when ConsumerData_Religion_Code is not null and ConsumerData_Religion_Code != ''"
            " then 1.0 else 0.0 end"
        ),
        "generations_in_hh": "try_cast(ConsumerData_Generations_In_HH as double)",
        "pct_ind_or_mixed_hh": (
            "case when Residence_HHParties_Description is not null"
            " and (Residence_HHParties_Description like '%Independent%'"
            " or Residence_HHParties_Description like '%&%') then 1.0 else 0.0 end"
        ),
        "mean_voting_perf_general": "Voters_VotingPerformanceEvenYearGeneral",
        "mean_voting_perf_minor": "Voters_VotingPerformanceMinorElection",
        "mean_hs_dropoff_only_top": "hs_dropoff_fill_only_top",
        "mean_hs_party_indep": "hs_ideology_partisanship_partisanship_overall_party_indep",
        "mean_hs_ticket_splitter_yes": "hs_ticket_splitting_yes",
        "mean_hs_ideology_general_moderate": "hs_ideology_general_moderate",
        "mean_hs_third_party_support": "hs_partisanship_moderate_third_party_support",
        "pct_weak_partisan_50": (
            "case when hs_trump_vs_harris_favor_trump is not null"
            " and hs_trump_vs_harris_favor_harris is not null"
            " and hs_trump_vs_harris_favor_trump < 50 and hs_trump_vs_harris_favor_harris < 50"
            " then 1.0 else 0.0 end"
        ),
        "und_x_general": (
            "case when hf_trump_vs_harris is null"
            " then Voters_VotingPerformanceEvenYearGeneral / 100.0 else 0.0 end"
        ),
        "mean_hs_gig_worker_unlikely": "hs_gig_worker_unlikely",
        "mean_hs_rfkjr_approval": "hs_rfkjr_approval",
        "mean_hs_trump_approval": "hs_trump_approval",
        "pct_party_switcher": (
            "case when Voters_MovedFrom_Party_Description <> Parties_Description"
            " or VoterParties_Change_Changed_Party is not null then 1.0 else 0.0 end"
        ),
        "pct_O": f"case when ({has_o}) then 1.0 else 0.0 end",
        "pct_DR": f"case when ({has_d}) and ({has_r}) then 1.0 else 0.0 end",
    }


def _parsed_date_sql(column_name):
    return (
        f"coalesce("
        f"try_to_date(cast({column_name} as string), 'yyyy-MM-dd'), "
        f"try_to_date(cast({column_name} as string), 'MM/dd/yyyy'), "
        f"try_to_date(cast({column_name} as string), 'M/d/yyyy'))"
    )


def build_features_sql(
    aggregate_relation,
    score_relation,
    election_date,
    reg_cutoff,
    primary_end_year,
    caster_column,
):
    """One row per voter in score_relation with the 34 non-race features.

    aggregate_relation feeds the precinct and statewide caster aggregates and must
    hold every voter in the scored states; score_relation is the subset to score
    (the new LALVOTERIDs on an incremental run, a sample in the cut-point script).
    """
    voter_exprs = _voter_feature_exprs(election_date, reg_cutoff, primary_end_year)
    prec_aggs = _precinct_feature_aggs_sql()
    agg_select = ",\n            ".join(f"{expr} as {name}" for name, expr in prec_aggs.items())
    voter_select = ",\n            ".join(
        f"cast({expr} as double) as {name}" for name, expr in voter_exprs.items()
    )
    passthrough = ",\n        ".join(f"voters.{name}" for name in voter_exprs)
    eco_select = ",\n        ".join(
        f"cast(case when precinct_eco.n_casters >= {MIN_PRECINCT_CASTERS}"
        f" and precinct_eco.{name} is not null then precinct_eco.{name}"
        f" else statewide_eco.{name} end as double) as {name}"
        for name in PRECINCT_FEATURES
    )
    return f"""
    with
        casters as (
            select
                state_postal_code,
                {PRECINCT_KEY_SQL} as precinct_key,
                Parties_Description,
                EthnicGroups_EthnicGroup1Desc,
                hs_ideology_partisanship_partisanship_overall_party_dem_strong,
                hs_ideology_partisanship_partisanship_overall_party_gop_strong
            from {aggregate_relation}
            where {caster_column}
        ),
        precinct_eco as (
            select
                state_postal_code,
                precinct_key,
                count(*) as n_casters,
            {agg_select}
            from casters
            where precinct_key is not null
            group by 1, 2
        ),
        statewide_eco as (
            select
                state_postal_code,
            {agg_select}
            from casters
            group by 1
        ),
        parsed as (
            select
                *,
                {_parsed_date_sql("Voters_BirthDate")} as bd_parsed,
                {_parsed_date_sql("Voters_CalculatedRegDate")} as rd_parsed,
                {PRECINCT_KEY_SQL} as precinct_key
            from {score_relation}
        ),
        voters as (
            select
                LALVOTERID,
                state_postal_code,
                precinct_key,
            {voter_select}
            from parsed
        )
    select
        voters.LALVOTERID,
        voters.state_postal_code as state,
        {passthrough},
        {eco_select}
    from voters
    left join
        precinct_eco
        on voters.state_postal_code = precinct_eco.state_postal_code
        and voters.precinct_key = precinct_eco.precinct_key
    left join statewide_eco on voters.state_postal_code = statewide_eco.state_postal_code
    """


# ── scoring ──────────────────────────────────────────────────────────────────


def raw_scores(pdf, boosters, feat_names, n_ref_by_state):
    """Continuous score per scenario for each row of a features frame.

    boosters: {arm: object with .predict(ndarray)}; feat_names: the booster's order.
    No calibration: only ranks are used.
    """
    n_ref = pdf["state"].map(n_ref_by_state).to_numpy(dtype=float)
    if np.isnan(n_ref).any():
        missing = sorted(set(pdf.loc[np.isnan(n_ref), "state"]))
        raise ValueError(f"No n_ref for states {missing}")
    x = pd.DataFrame(
        {f: pd.to_numeric(pdf[f], errors="coerce") for f in feat_names if f not in RACE_FEATURES},
        index=pdf.index,
    )
    out = {}
    for scenario in SCENARIOS:
        for name, value in race_block(scenario, n_ref).items():
            x[name] = value
        matrix = x[feat_names].to_numpy(dtype=float, na_value=np.nan)
        out[scenario] = np.asarray(boosters[scenario_arm(scenario)].predict(matrix), dtype=float)
    return out


def raw_frame(pdf, boosters, feat_names, n_ref_by_state):
    scores = raw_scores(pdf, boosters, feat_names, n_ref_by_state)
    out = pdf[["LALVOTERID", "state"]].copy()
    for scenario, score in scores.items():
        out[raw_score_column(scenario)] = score
    return out


def flagged_frame(pdf, boosters, feat_names, n_ref_by_state, cuts):
    """Raw scores plus flags. cuts: {state: {scenario: [c20, c40, c60, c80]}}."""
    scores = raw_scores(pdf, boosters, feat_names, n_ref_by_state)
    out = pdf[["LALVOTERID", "state"]].copy()
    for scenario, score in scores.items():
        out[raw_score_column(scenario)] = score
    rows_by_state = pdf.groupby("state").indices
    for scenario, score in scores.items():
        flagged = np.zeros(len(pdf), dtype=bool)
        for state, idx in rows_by_state.items():
            flagged[idx] = flags(score[idx], cuts[state][scenario])
        out[flag_column(scenario)] = flagged
    return out


def booster_digest(model_text):
    return hashlib.sha256(model_text.encode("utf-8")).hexdigest()[:16]


def scoring_fingerprint(booster_digests, n_ref_by_state, features_sql):
    """Identifies everything that moves a raw score. Boosters enter by content, not
    registry version, so copying a model from sandbox to model_predictions (which
    renumbers it) is not a change. Whitespace in the SQL is ignored."""
    payload = {
        "boosters": {arm: booster_digests[arm] for arm in sorted(booster_digests)},
        "n_ref": {st: int(v) for st, v in sorted(n_ref_by_state.items())},
        "scenarios": SCENARIOS,
        "features_sql": " ".join(features_sql.split()),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def cuts_by_state(cut_rows, states, fingerprint):
    """Return ({state: {scenario: [c20..c80]}}, states with no complete cut set).

    Missing cuts are expected right after a model change, so they are reported, not
    raised. Duplicate or out-of-order cuts mean the table is corrupt, so they raise.
    """
    rows = cut_rows[(cut_rows["scoring_fingerprint"] == fingerprint) & cut_rows["state"].isin(states)]
    dupes = rows[rows.duplicated(["state", "scenario"], keep=False)]
    if not dupes.empty:
        raise ValueError(
            f"Duplicate cut rows for fingerprint {fingerprint}: "
            f"{dupes[['state', 'scenario']].values.tolist()[:20]}"
        )
    result = {}
    for r in rows.itertuples(index=False):
        cuts = [float(getattr(r, c)) for c in CUT_COLUMNS]
        if any(math.isnan(c) for c in cuts) or cuts != sorted(cuts):
            raise ValueError(f"Cut points for {r.state}/{r.scenario} are not ascending: {cuts}")
        result.setdefault(r.state, {})[r.scenario] = cuts
    incomplete = sorted(st for st in states if set(result.get(st, {})) != set(SCENARIOS))
    return {st: result[st] for st in states if st not in incomplete}, incomplete


def _parse_state_allowlist(raw):
    """DEV-ONLY: score a subset of states without a full national run."""
    if raw is None:
        return None
    parts = [p for p in re.split(r"[,\s]+", str(raw).strip().upper()) if p]
    return set(parts) or None


def make_scorer(model_texts, feat_names, n_ref_by_state, cuts=None):
    """mapInPandas closure. model_texts: {arm: booster model string}. With cuts it
    yields raw scores and flags, without them raw scores only (the cut-point notebook's mode)."""
    cache_key = tuple(sorted((arm, booster_digest(t)) for arm, t in model_texts.items()))

    def _score_partition(iterator):
        import builtins

        import lightgbm as lgb

        cache = getattr(builtins, "_GP_INDEP_OPENNESS_BOOSTER_CACHE", None)
        if cache is None:
            cache = {}
            builtins._GP_INDEP_OPENNESS_BOOSTER_CACHE = cache
        if cache_key not in cache:
            cache.clear()
            cache[cache_key] = {arm: lgb.Booster(model_str=t) for arm, t in model_texts.items()}
        boosters = cache[cache_key]

        for pdf in iterator:
            if len(pdf) == 0:
                continue
            if cuts is None:
                yield raw_frame(pdf, boosters, feat_names, n_ref_by_state)
            else:
                yield flagged_frame(pdf, boosters, feat_names, n_ref_by_state, cuts)

    return _score_partition


def load_boosters(catalog, models_schema, alias="production", versions=None):
    """Resolve and load both models. versions ({arm: n}) overrides the alias, for
    cutting a candidate model before it is promoted. Returns (boosters, versions,
    feat_names). The alias is resolved to a number once, so a promotion mid-run
    cannot swap a booster underneath a scoring pass."""
    import mlflow
    import mlflow.lightgbm

    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()
    boosters, resolved = {}, {}
    for arm, name in MODEL_NAMES.items():
        full_name = f"{catalog}.{models_schema}.{name}"
        if versions and versions.get(arm):
            resolved[arm] = str(versions[arm])
        else:
            resolved[arm] = str(client.get_model_version_by_alias(full_name, alias).version)
        model = mlflow.lightgbm.load_model(f"models:/{full_name}/{resolved[arm]}")
        boosters[arm] = model.booster_ if hasattr(model, "booster_") else model
        print(f"loaded {full_name} version {resolved[arm]}")
    feat_names = list(boosters[ARM_2PLUS].feature_name())
    if feat_names != list(boosters[ARM_0OR1].feature_name()):
        raise ValueError("The two state-leg models disagree on feature order")
    if set(feat_names) != set(FEATURES):
        raise ValueError(
            f"Model features differ from the expected 45: {sorted(set(feat_names) ^ set(FEATURES))}"
        )
    return boosters, resolved, feat_names


def scored_schema():
    """What the scorer yields with cuts: keys, raw scores, flags."""
    raw = ", ".join(f"{raw_score_column(s)} double" for s in SCENARIOS)
    flag = ", ".join(f"{flag_column(s)} boolean" for s in SCENARIOS)
    return f"LALVOTERID string, state string, {raw}, {flag}"


def output_schema():
    return (
        f"{scored_schema()}, model_version_2plus string, model_version_0or1 string, "
        f"n_ref int, scoring_fingerprint string, scored_at timestamp"
    )


def model(dbt, session):
    dbt.config(
        # skops-format models need scikit-learn and skops on the driver to load. The
        # models were trained on scikit-learn 1.9.1, which needs Python 3.11+; 1.7.2 also
        # runs on serverless's 3.10 and yields byte-identical boosters (only booster_ is used).
        environment_key="indep_openness",
        environment_dependencies=[
            "mlflow==3.16.1",
            "lightgbm==4.7.0",
            "scikit-learn==1.7.2",
            "skops==0.15.0",
        ],
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        tags=["intermediate", "l2", "model_prediction", "indep_openness"],
    )

    catalog = "goodparty_data_catalog"
    models_schema = dbt.config.meta_get("indep_openness_models_schema") or "model_predictions"
    alias = dbt.config.meta_get("indep_openness_model_alias") or "production"
    cut_table = dbt.config.meta_get("indep_openness_cutpoints_table")
    cycle = {
        "election_date": dbt.config.meta_get("indep_openness_election_date"),
        "reg_cutoff": dbt.config.meta_get("indep_openness_reg_cutoff"),
        "primary_end_year": int(dbt.config.meta_get("indep_openness_primary_end_year")),
        "caster_column": dbt.config.meta_get("indep_openness_caster_column"),
    }
    state_allowlist = _parse_state_allowlist(dbt.config.meta_get("l2_state_allowlist"))

    n_ref_pdf = dbt.ref("indep_openness_n_ref").toPandas()
    n_ref_by_state = dict(zip(n_ref_pdf["state"], n_ref_pdf["n_ref"].astype(float), strict=False))
    states = sorted(n_ref_by_state)
    if state_allowlist:
        states = sorted(set(states) & state_allowlist)

    boosters, versions, feat_names = load_boosters(catalog, models_schema, alias)
    model_texts = {arm: b.model_to_string() for arm, b in boosters.items()}
    features_sql = build_features_sql("_l2", "_to_score", **cycle)
    # Fingerprint over the full seed, not the allowlist, so a one-state dev run
    # matches cuts from a national notebook run.
    fingerprint = scoring_fingerprint(
        {arm: booster_digest(t) for arm, t in model_texts.items()}, n_ref_by_state, features_sql
    )
    print(
        f"indep openness: {models_schema}@{alias} -> 2plus v{versions[ARM_2PLUS]}, "
        f"0or1 v{versions[ARM_0OR1]}; cycle {cycle}; fingerprint {fingerprint}"
    )

    cut_rows = session.table(cut_table).filter(f"scoring_fingerprint = '{fingerprint}'").toPandas()
    cuts, no_cuts = cuts_by_state(cut_rows, states, fingerprint)
    if no_cuts:
        print(
            f"WARNING: {cut_table} has no complete cut set for fingerprint {fingerprint} in "
            f"{no_cuts}. Run the cut-point notebook for the current models. Leaving the table unchanged."
        )
        return session.createDataFrame([], schema=output_schema())

    full_rescore = True
    if dbt.is_incremental:
        existing = session.table(f"{dbt.this}")
        if "scoring_fingerprint" in existing.columns:
            stamped = {r[0] for r in existing.select("scoring_fingerprint").distinct().collect()}
            full_rescore = stamped != {fingerprint}
            if full_rescore:
                print(f"fingerprint changed from {sorted(stamped, key=str)}: rescoring every voter")

    l2 = dbt.ref("int__l2_nationwide_uniform_w_haystaq")
    missing = missing_l2_columns(l2.columns, cycle["primary_end_year"], cycle["caster_column"])
    if missing:
        raise ValueError(f"L2 is missing columns the features need: {missing}")
    if not state_allowlist:
        unknown = {r[0] for r in l2.select("state_postal_code").distinct().collect()} - set(n_ref_by_state)
        if unknown:
            raise ValueError(f"L2 has states with no n_ref: {sorted(unknown, key=str)}")
    l2 = l2.filter(l2.state_postal_code.isin(states))
    l2.createOrReplaceTempView("_l2")
    # Precinct aggregates always come from the full state; only the scored set shrinks.
    to_score = l2
    if not full_rescore:
        already_scored = session.table(f"{dbt.this}").select("LALVOTERID")
        to_score = l2.join(already_scored, on="LALVOTERID", how="left_anti")
    to_score.createOrReplaceTempView("_to_score")

    score_cols = [raw_score_column(s) for s in SCENARIOS] + [flag_column(s) for s in SCENARIOS]
    scored = session.sql(features_sql).mapInPandas(
        make_scorer(model_texts, feat_names, n_ref_by_state, cuts), schema=scored_schema()
    )
    scored.createOrReplaceTempView("_scored")

    n_ref_values = ", ".join(f"('{st}', {int(n_ref_by_state[st])})" for st in states)
    return session.sql(
        f"""
        with n_ref as (select * from values {n_ref_values} as t(state, n_ref))
        select
            _scored.LALVOTERID,
            _scored.state,
            {", ".join(f"_scored.{c}" for c in score_cols)},
            '{versions[ARM_2PLUS]}' as model_version_2plus,
            '{versions[ARM_0OR1]}' as model_version_0or1,
            cast(n_ref.n_ref as int) as n_ref,
            '{fingerprint}' as scoring_fingerprint,
            current_timestamp() as scored_at
        from _scored
        left join n_ref on _scored.state = n_ref.state
        """
    )
