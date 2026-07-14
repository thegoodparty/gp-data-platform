"""Nationwide voter-turnout LightGBM inference (DATA-2015).

ONE dbt Python model that replaces the research branch's 51 per-state models +
Jinja macro. Reads int__l2_nationwide_uniform, aggregates to precinct grain,
predicts turnout per precinct with the @production LightGBM models, assigns
precincts to districts (>50% majority), and aggregates to district-level
ballots_projected. Output feeds int__model_prediction_voter_turnout.

Collapse-specific changes vs the per-state macro (all verified against the
nationwide table on 2026-06-29):
  - State comes from the real `state_postal_code` column (no lit(state_code)).
  - The odd-year (`AnyElection`) opportunity logic is ROW-LEVEL (state_postal_code IN (...)).
  - The vote-history "voted" test is BOOLEAN (cols are BooleanType nationwide),
    not = 'Y'.

Even-year (`OtherElection`, local) eligibility is guided solely by per-precinct
opportunity presence in `turnout_historical_precincts` — no state-list shortcut,
since local election incidence varies precinct-by-precinct, not by state. This
mirrors the training-side fix (see the training notebook's vote_history_sql).
Previously this branched on `year % 2 == 0` and blanket-zeroed every even-year
eligible non-voter nationwide, inflating the `even_year_local` target/feature
denominator everywhere.

Opportunity affects FEATURES only; predictions are intentionally emitted for
EVERY precinct, including ones whose history shows no even-year local elections
(their lag features are NULL). Each row answers the hypothetical "if such an
election were held here, what would turnout be?" so the product can serve
BallotReady-asserted or user-entered races in places history says are unlikely
(research decision, 2026-07-07). Do not add an opportunity gate on output rows.

The SQL-building helpers are pure (return SQL strings) so they are unit-tested
without Spark/MLflow; `model()` executes them. `import mlflow` is deferred into
`model()` so this module imports in the dbt test env (pyspark + pandas, no mlflow).
"""

import glob
import json
import os
import re
import tempfile
from datetime import datetime
from functools import reduce

import numpy as np
import pandas as pd
from pyspark.sql.functions import col

_VH_COL_RE = re.compile(r"^(General|Primary|OtherElection|AnyElection)_(\d{4})$")

_CATEGORICAL_FEATURES = [
    "BirthDateConfidence_Description",
    "AbsenteeTypes_Description",
    "ConsumerData_CRA_Income_Classification_Code",
    "EthnicGroups_EthnicGroup1Desc",
    "Residence_HHGender_Description",
    "Parties_Description",
    "Residence_HHParties_Description",
    "ConsumerData_Homeowner_Probability_Model",
    "Designated_Market_Area_DMA",
    "ConsumerData_Home_Purchase_Price_Code",
    "Voters_Gender",
    "ConsumerData_Credit_Rating",
    "ConsumerData_Marital_Status",
    "ConsumerData_Language_Code",
    "ConsumerData_CSA",
    "ConsumerData_Religion_Code",
    "ConsumerData_Education_of_Person",
]

# String indicator columns: L2 stores these as 'Y' / null (verified STILL STRING in
# int__l2_nationwide_uniform). Use string equality, not = TRUE.
_Y_INDICATOR_COLS = [
    "ConsumerData_Donor_Charitable_Causes",
    "ConsumerData_Donor_Political_Conservative",
    "ConsumerData_Donor_Political_Liberal",
    "ConsumerData_Political_Donor_State_Level",
    "ConsumerData_Current_Affairs_Politics",
]
# Gun_Owner uses 'Yes' instead of 'Y' (verified STILL STRING nationwide).
_YES_INDICATOR_COLS = [
    "ConsumerDataLL_Gun_Owner",
]
# Actual boolean columns in L2 — safe to check with = TRUE (verified BOOLEAN nationwide).
_BOOLEAN_FLAG_COLS = [
    "Cell_Phone_Number_Available",
    "Landline_Phone_Number_Available",
    "Phone_Number_Available",
    "ConsumerData_Do_Not_Call",
]

# Plain numeric columns (verified INT nationwide) — cast to DOUBLE before AVG so
# pandas does not receive DECIMAL(38,18) (which LightGBM rejects as StringDtype).
_NUMERIC_MEAN_COLS = [
    "ConsumerData_Household_Number_Lines_Of_Credit",
    "ConsumerData_Number_Of_Persons_in_HH",
    "Residence_Families_HHVotersCount",
    "ConsumerData_AreaMedianEducationYears",
    "ConsumerData_Social_Ranking_Index_by_Area",
    "ConsumerData_Social_Ranking_Index_by_Individual",
    "ConsumerData_Likely_Income_Ranking_by_Area",
    "ConsumerData_Likely_Educational_Attainment_Ranking_by_Area",
]

# Dollar-formatted strings in L2 (e.g. '$12500') — strip '$' before casting.
_DOLLAR_COLS = [
    "ConsumerData_Estimated_Income_Amount",
    "ConsumerData_EstimatedAreaMedianHHIncome",
    "ConsumerData_AreaMedianHousingValue",
]

# Percent-formatted string in L2 — strip '%' before casting.
_PCT_COLS = [
    "ConsumerData_AreaPcntHHSpanishSpeaking",
]


def _nh_vt_precinct(qualifier=""):
    """Precinct key: NH/VT use ward/town names (their raw Precinct is mostly NULL);
    everywhere else the raw precinct. qualifier prefixes column refs so the same
    expression works inside multi-table joins (e.g. "l2.")."""
    q = qualifier
    return f"""
    CASE WHEN {q}state_postal_code IN ('NH', 'VT')
         THEN COALESCE({q}Town_Ward, {q}City_Ward, {q}Town_District, {q}City)
         ELSE CAST({q}Precinct AS STRING)
    END
"""


_NH_VT_PRECINCT = _nh_vt_precinct()

# States that hold odd-year elections statewide — eligible non-voters in odd-year
# AnyElection_ columns always get 0 (no per-precinct opportunity check needed).
# Mirrors ODD_YEAR_OPPORTUNITY_STATES in the training notebook.
_ODD_YEAR_OPPORTUNITY_STATES = frozenset(
    [
        "AL",
        "CO",
        "CT",
        "GA",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "MA",
        "MI",
        "MS",
        "MT",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "OH",
        "OR",
        "PA",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
    ]
)
# Collapse change: a SQL IN-list literal for the ROW-LEVEL opportunity branch
# (replaces the per-state scalar `in_opportunity_state`).
_OPP_STATES_SQL = "(" + ", ".join(f"'{s}'" for s in sorted(_ODD_YEAR_OPPORTUNITY_STATES)) + ")"

_SLUG_ELECTION_CODE = {
    "presidential_lag3": "General",
    "midterm": "General",
    "even_year_local": "Local_or_Municipal",
    "off_year_local_lag2": "Local_or_Municipal",
    "even_year_primary": "Primary",
}


def _year_to_model_slugs(year):
    # If a needed slug has no @production model, the pipeline fails — intentionally.
    if year % 2 != 0:
        return ["off_year_local_lag2"]
    elif year % 4 == 2:
        return ["midterm", "even_year_local", "even_year_primary"]
    else:
        return ["presidential_lag3", "even_year_local", "even_year_primary"]


def _detect_election_cols(l2_columns, max_vote_history_year):
    result = []
    for column_name in l2_columns:
        m = _VH_COL_RE.match(column_name)
        if m and int(m.group(2)) <= max_vote_history_year:
            result.append((column_name, m.group(1), int(m.group(2))))
    return result


def _safe_date(column_name):
    # to_date with explicit format handles L2's MM/dd/yyyy string storage;
    # the TRY_CAST branch handles columns already stored as DATE (e.g.
    # Voters_BirthDate / Voters_CalculatedRegDate are DATE in the nationwide table).
    return (
        f"COALESCE(TRY_CAST(`{column_name}` AS DATE), to_date(CAST(`{column_name}` AS STRING), 'MM/dd/yyyy'))"
    )


def _op_years(election_cols, l2_col_set, inference_year):
    """Years (in lag range) needing a per-precinct opportunity flag: odd-year
    AnyElection (non-opportunity-state precincts) and even-year OtherElection
    (all precincts, no state shortcut). AnyElection is always odd and
    OtherElection is always even in the nationwide L2 schema, so no explicit
    parity filter is needed here — the prefix already implies it."""
    return sorted(
        {
            year
            for col_name, prefix, year in election_cols
            if col_name in l2_col_set
            and prefix in ("AnyElection", "OtherElection")
            and 1 <= inference_year - year <= 12
        }
    )


def _opp_view_sql(op_years, catalog, precincts_schema):
    """Nationwide opportunity-flag table over ALL states (was WHERE State = one state).

    precincts_schema = where turnout_historical_precincts lives. PROD default is
    'model_predictions' (it must be promoted there alongside the models; it currently
    exists only in sandbox, which is not a sanctioned prod source). Dev may override.
    """
    opp_col_exprs = ", ".join(
        f"MAX(CASE WHEN election_year_str IN ('AnyElection_{y}', 'OtherElection_{y}') "
        f"THEN 1 ELSE 0 END) AS opp_{y}"
        for y in op_years
    )
    year_filter = " OR ".join(
        f"election_year_str IN ('AnyElection_{y}', 'OtherElection_{y}')" for y in op_years
    )
    return f"""
        SELECT State, County, Precinct, {opp_col_exprs}
        FROM {catalog}.{precincts_schema}.turnout_historical_precincts
        WHERE ({year_filter})
        GROUP BY State, County, Precinct
    """


def _build_precinct_features_sql(l2_col_set, election_cols, inference_year, l2_collection_year):
    """Pure: precinct-grain feature aggregate SQL over the _l2 / _enriched view.

    Collapse changes vs the per-state macro:
      - State from the real `state_postal_code` column (no lit()).
      - Odd-year (AnyElection) eligibility is ROW-LEVEL via state_postal_code IN (...).
      - Even-year (OtherElection) eligibility is precinct-opportunity-only, no state list.
      - The vote-history "voted" test is BOOLEAN (cols are BooleanType), not = 'Y'.
    """
    op_years = _op_years(election_cols, l2_col_set, inference_year)
    if op_years:
        # Enrich _l2 with per-precinct opportunity flags via a (State, County, Precinct) join.
        opp_select = ", ".join(f"COALESCE(hp.opp_{y}, 0) AS opp_{y}" for y in op_years)
        # Precinct must use the same NH/VT ward key as the SELECT/membership:
        # turnout_historical_precincts carries ward names for NH/VT, whose raw
        # Precinct is mostly NULL and would never match.
        from_clause = (
            f"(SELECT l2.*, {opp_select} FROM _l2 AS l2"
            f" LEFT JOIN _hp_opp AS hp"
            f"   ON l2.state_postal_code = hp.State"  # nationwide: join on state too
            f"  AND l2.County = hp.County"
            f"  AND {_nh_vt_precinct('l2.')} = hp.Precinct) AS _enriched"
        )
    else:
        from_clause = "_l2"

    exprs = [
        "state_postal_code AS State",  # real column, no lit()
        "County",
        f"{_NH_VT_PRECINCT} AS Precinct",
        "CAST(COUNT(*) AS DOUBLE) AS n_voters",
        # Age / registration tenure as of Nov 1 of the inference year.
        (
            f"CAST(AVG(DATEDIFF(MAKE_DATE({inference_year}, 11, 1), "
            f"{_safe_date('Voters_BirthDate')}) / 365.25) AS DOUBLE) AS age"
        ),
        (
            f"CAST(AVG(DATEDIFF(MAKE_DATE({inference_year}, 11, 1), "
            f"{_safe_date('Voters_CalculatedRegDate')}) / 365.25) AS DOUBLE) AS reg_for"
        ),
        # Residential mobility signals.
        (
            "CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL "
            "THEN 1.0 ELSE 0.0 END) AS DOUBLE) AS pct_ever_moved"
        ),
        (
            f"CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL "
            f"THEN CAST({inference_year} - YEAR({_safe_date('Voters_MovedFrom_Date')}) AS DOUBLE) "
            f"ELSE NULL END) AS DOUBLE) AS years_since_moved_at_target"
        ),
        # Voters_Active: L2 raw uses 'A'/'I'; some staging tables normalise to active/inactive.
        (
            "CAST(AVG(CASE WHEN UPPER(Voters_Active) IN ('A', 'ACTIVE') THEN 1.0 "
            "WHEN UPPER(Voters_Active) IN ('I', 'INACTIVE') THEN 0.0 "
            "ELSE NULL END) AS DOUBLE) AS Voters_Active"
        ),
        # FECDonors: null means never donated — treat as 0 before averaging.
        (
            "CAST(AVG(COALESCE(CAST(FECDonors_NumberOfDonations AS DOUBLE), 0.0)) AS DOUBLE) "
            "AS FECDonors_NumberOfDonations"
        ),
        f"CAST({inference_year} AS DOUBLE) AS target_year",
        # Length of residence projected to the inference year.
        (
            f"CAST(AVG(ConsumerData_Length_Of_Residence_Code "
            f"- ({l2_collection_year} - {inference_year})) AS DOUBLE) "
            f"AS ConsumerData_Length_Of_Residence_Code"
        ),
    ]

    for column_name in _Y_INDICATOR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{column_name}` = 'Y' THEN 1.0 ELSE 0.0 END) AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _YES_INDICATOR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{column_name}` = 'Yes' THEN 1.0 ELSE 0.0 END) AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _BOOLEAN_FLAG_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{column_name}` = TRUE THEN 1.0 ELSE 0.0 END) AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _NUMERIC_MEAN_COLS:
        if column_name in l2_col_set:
            exprs.append(f"CAST(AVG(CAST(`{column_name}` AS DOUBLE)) AS DOUBLE) AS `{column_name}`")
    for column_name in _DOLLAR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(AVG(CAST(REPLACE(`{column_name}`, '$', '') AS DOUBLE)) AS DOUBLE) AS `{column_name}`"
            )
    for column_name in _PCT_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(AVG(CAST(REPLACE(`{column_name}`, '%', '') AS DOUBLE)) AS DOUBLE) AS `{column_name}`"
            )
    for column_name in _CATEGORICAL_FEATURES:
        if column_name in l2_col_set:
            exprs.append(f"mode(`{column_name}`) AS `{column_name}`")

    # Vote-history columns: replicates training's eligibility CASE. The "voted" test is
    # a BOOLEAN (not = 'Y'). Non-voter eligibility branches by prefix, not year parity:
    #   - General/Primary: always held, eligible non-voter -> 0.
    #   - AnyElection (odd-year local): opportunity-state shortcut, then per-precinct
    #     opportunity check, ROW-LEVEL.
    #   - OtherElection (even-year local): per-precinct opportunity only, no state
    #     shortcut — local election incidence varies precinct-by-precinct, not by state.
    for col_name, prefix, year in election_cols:
        if col_name not in l2_col_set:
            continue
        lag = inference_year - year
        if lag < 1 or lag > 12:
            continue

        if prefix in ("General", "Primary"):
            eligible_nonvoter_sql = "ELSE 0.0"
        elif prefix == "AnyElection":
            eligible_nonvoter_sql = (
                f"WHEN state_postal_code IN {_OPP_STATES_SQL} THEN 0.0 WHEN opp_{year} = 1 THEN 0.0 ELSE NULL"
            )
        else:  # OtherElection
            eligible_nonvoter_sql = f"WHEN opp_{year} = 1 THEN 0.0 ELSE NULL"

        exprs.append(
            f"CAST(AVG("
            f"  CASE WHEN `{col_name}` THEN 1.0"  # BOOLEAN voted-test (was = 'Y')
            f"       WHEN {_safe_date('Voters_BirthDate')} IS NULL THEN NULL"
            f"       WHEN ({year} - YEAR({_safe_date('Voters_BirthDate')})) < 18 THEN NULL"
            f"       WHEN `Voters_CalculatedRegDate` IS NOT NULL"
            f"            AND YEAR({_safe_date('Voters_CalculatedRegDate')}) > {year} THEN NULL"
            f"       {eligible_nonvoter_sql}"
            f"  END"
            f") AS DOUBLE)"
            f"  AS `{prefix}_Minus{lag}`"
        )

    return (
        f"SELECT {', '.join(exprs)} FROM {from_clause} GROUP BY state_postal_code, County, {_NH_VT_PRECINCT}"
    )


def _build_district_membership_sql(l2_col_set, district_types):
    """Pure: precinct -> district membership via STACK + a >50% majority rule.
    Already nationwide-correct (groups by state_postal_code; joins on State/County/Precinct)."""
    valid_cols = [dt for dt in district_types if dt in l2_col_set]
    if not valid_cols:
        raise ValueError("No district columns found in L2 schema")

    n = len(valid_cols)
    stack_args = ", ".join(f"'{dt}', CAST(`{dt}` AS STRING)" for dt in valid_cols)

    return f"""
        WITH precinct_totals AS (
            SELECT
                state_postal_code    AS State,
                County,
                {_NH_VT_PRECINCT}    AS Precinct,
                COUNT(*)             AS total_voters
            FROM _l2
            GROUP BY state_postal_code, County, {_NH_VT_PRECINCT}
        ),
        voter_district AS (
            SELECT
                state_postal_code    AS State,
                County,
                {_NH_VT_PRECINCT}    AS Precinct,
                district_type,
                district_name
            FROM _l2
            LATERAL VIEW STACK({n}, {stack_args}) t AS district_type, district_name
            WHERE district_name IS NOT NULL
        ),
        in_district AS (
            SELECT State, County, Precinct, district_type, district_name,
                   COUNT(*) AS n_in
            FROM voter_district
            GROUP BY State, County, Precinct, district_type, district_name
        ),
        membership AS (
            SELECT c.State, c.County, c.Precinct, c.district_type, c.district_name
            FROM in_district c
            JOIN precinct_totals t USING (State, County, Precinct)
            WHERE c.n_in * 1.0 / t.total_voters > 0.5
        )
        SELECT State, County, Precinct,
               'State' AS district_type, State AS district_name
        FROM precinct_totals
        UNION ALL
        SELECT * FROM membership
    """


def _parse_state_allowlist(raw):
    """DEV-ONLY iteration knob. NEVER set when building the wired int model."""
    if raw is None:
        return None
    normalized = raw.strip().upper()
    if not normalized:
        return None
    parts = re.split(r"[,\s]+", normalized)
    allowlist = {p for p in parts if p}
    return allowlist or None


def _check_lgbm_version(registered_model_name, client):
    import lightgbm as lgb

    tag = client.get_registered_model(registered_model_name).tags.get("lightgbm_version")
    if not tag:
        print(f"WARNING: lightgbm_version tag not set on {registered_model_name}. Skipping check.")
        return
    trained_major = int(tag.split(".")[0])
    running_major = int(lgb.__version__.split(".")[0])
    if running_major != trained_major:
        raise RuntimeError(
            f"LightGBM major version mismatch on {registered_model_name}: "
            f"trained={tag}, cluster={lgb.__version__}. "
            f"Re-run the promote script after upgrading the cluster library."
        )
    if tag != lgb.__version__:
        print(
            f"WARNING: LightGBM minor version mismatch on {registered_model_name} "
            f"(trained={tag}, cluster={lgb.__version__}). Proceeding."
        )


def _predict_precinct(pdf, booster, cat_map, model_slug, model_version, inference_year):
    feat_names = booster.feature_name_

    # Add NaN placeholders for any features the model expects but L2 lacks.
    for feat in feat_names:
        if feat not in pdf.columns:
            pdf[feat] = np.nan

    x = pdf[feat_names].copy()

    # Integer-encode categoricals using the saved cat_map. Unseen values -> NaN (missing).
    for feat, categories in cat_map.items():
        if feat not in x.columns:
            continue
        cat_idx = {v: i for i, v in enumerate(categories)}
        x[feat] = x[feat].map(cat_idx)

    # Coerce any remaining non-numeric columns (Arrow-backed StringDtype etc.) to float.
    still_str = [c for c in x.columns if not pd.api.types.is_numeric_dtype(x[c])]
    if still_str:
        print(f"WARNING: coercing {len(still_str)} non-numeric feature columns to NaN: {still_str}")
        for c in still_str:
            x[c] = pd.to_numeric(x[c], errors="coerce")

    result = pdf[["State", "County", "Precinct", "n_voters"]].copy()
    result["p_hat"] = booster._Booster.predict(x.to_numpy(dtype=float, na_value=np.nan))
    result["model_slug"] = model_slug
    result["model_family"] = model_version
    result["inference_year"] = inference_year
    result["election_code"] = _SLUG_ELECTION_CODE[model_slug]
    return result


def _assert_consistent_model_family(families_by_slug):
    """All promoted turnout models must share one model_family (stamped on every row as
    the load-bearing model_version). Raise on disagreement rather than mislabel rows."""
    distinct = set(families_by_slug.values())
    if len(distinct) != 1:
        raise ValueError(
            f"Promoted turnout models disagree on model_family: {families_by_slug}. "
            f"Promote a consistent set before running."
        )
    return next(iter(distinct))


def _select_cat_map_path(candidate_paths):
    """Resolve the single categorical-feature-map file in a model's artifacts.

    The map is logged under model/ (so it travels with copy_model_version) but with a
    tmp-prefixed filename, so it is located by glob rather than a fixed name. Fail loud on
    zero or multiple matches: a missing or ambiguous map would silently mis-encode features
    and corrupt predictions, which is worse than a hard failure.
    """
    if len(candidate_paths) != 1:
        raise ValueError(
            f"Expected exactly one categorical_feature_map.json in the model artifacts, "
            f"found {len(candidate_paths)}: {sorted(candidate_paths)}"
        )
    return candidate_paths[0]


def _read_model_family_tag(registered_model_tags, registered_model_name):
    """Read the load-bearing model_family from the registered-model tag set at promotion.

    It is stamped on every output row as model_version (part of the election-api id), so a
    missing tag must fail rather than default — an unset tag would mislabel the whole table.
    """
    family = (registered_model_tags or {}).get("model_family")
    if not family:
        raise ValueError(
            f"Registered model {registered_model_name} is missing the required 'model_family' "
            f"tag. Set it at promotion (the promote script tags each model)."
        )
    return family


def model(dbt, session):
    import mlflow
    import mlflow.lightgbm

    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="table",  # full refresh, v1 (locked decision: no incremental)
        tags=["intermediate", "l2", "model_prediction", "voter_turnout"],
    )

    max_vote_history_year = int(dbt.config.meta_get("max_vote_history_year") or 2025)
    models_schema = dbt.config.meta_get("voter_turnout_models_schema") or "model_predictions"
    # turnout_historical_precincts is read from its own schema (NOT models_schema). PROD =
    # model_predictions (promoted there alongside the models); dev may override to sandbox.
    precincts_schema = dbt.config.meta_get("voter_turnout_precincts_schema") or "model_predictions"
    state_allowlist = _parse_state_allowlist(dbt.config.meta_get("l2_state_allowlist"))
    # Single catalog across this repo; hardcode it like the sibling model-backed models
    # (int__civics_viability_scoring / int__techspeed_viability_scoring). dbt.config.get(
    # "database") returns None for dbt-databricks Python models (see dbt/project/CLAUDE.md).
    catalog = "goodparty_data_catalog"

    current_year = datetime.now().year
    inference_years = [current_year, current_year + 1, current_year + 2]

    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    needed_slugs = sorted({slug for y in inference_years for slug in _year_to_model_slugs(y)})
    models, cat_maps, model_families = {}, {}, {}
    for slug in needed_slugs:
        full_name = f"{catalog}.{models_schema}.voter_turnout_model_{slug}"
        _check_lgbm_version(full_name, client)
        # model_family is the load-bearing model_version stamped on every row (part of the
        # election-api id). Read it from the registered-model tag set at promotion, NOT a run
        # artifact: the validated retrains carry no model_family.json, and a tag travels with
        # the model (survives copy_model_version) and is version-agnostic. Read per slug so an
        # inconsistent promotion is caught below, not silently stamped from one slug.
        registered = client.get_registered_model(full_name)
        model_families[slug] = _read_model_family_tag(registered.tags, full_name)
        # Download the @production model once, then load it and resolve its categorical feature
        # map. The map is logged under model/ (so it travels with copy_model_version) but with
        # a tmp-prefixed filename, so glob for it rather than assume a fixed artifact path.
        model_uri = f"models:/{full_name}@production"
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = mlflow.artifacts.download_artifacts(artifact_uri=model_uri, dst_path=tmp)
            models[slug] = mlflow.lightgbm.load_model(model_dir)
            cat_path = _select_cat_map_path(
                glob.glob(
                    os.path.join(model_dir, "**", "*categorical_feature_map.json"),
                    recursive=True,
                )
            )
            with open(cat_path) as f:
                cat_maps[slug] = json.load(f)
    # All promoted models must share one model_family — it is stamped on every row as the
    # load-bearing model_version (part of the election-api id). Fail loudly on disagreement.
    model_family = _assert_consistent_model_family(model_families)

    l2 = dbt.ref("int__l2_nationwide_uniform")
    if state_allowlist:
        l2 = l2.filter(col("state_postal_code").isin(sorted(state_allowlist)))
    l2.createOrReplaceTempView("_l2")
    l2_col_set = set(l2.columns)
    election_cols = _detect_election_cols(l2.columns, max_vote_history_year)

    dist_types = [
        r.district_type
        for r in dbt.ref("int__l2_district_aggregations").select("district_type").distinct().collect()
        if r.district_type and r.district_type != "State"
    ]
    session.sql(_build_district_membership_sql(l2_col_set, dist_types)).createOrReplaceTempView("_membership")

    pred_dfs = []
    for inference_year in inference_years:
        op_years = _op_years(election_cols, l2_col_set, inference_year)
        if op_years:
            session.sql(_opp_view_sql(op_years, catalog, precincts_schema)).createOrReplaceTempView("_hp_opp")
        # ~206k precincts per year (~150 MB) — toPandas is safe; eagerly materialized here so
        # the per-year _hp_opp view is consumed before the next year overwrites it.
        pdf = session.sql(
            _build_precinct_features_sql(l2_col_set, election_cols, inference_year, current_year)
        ).toPandas()

        for slug in _year_to_model_slugs(inference_year):
            pred_pdf = _predict_precinct(
                pdf.copy(),
                booster=models[slug],
                cat_map=cat_maps[slug],
                model_slug=slug,
                model_version=model_family,
                inference_year=inference_year,
            )
            # Arrow-backed StringDtype -> plain object before createDataFrame.
            for c in pred_pdf.columns:
                if not pd.api.types.is_numeric_dtype(pred_pdf[c]):
                    pred_pdf[c] = pred_pdf[c].astype(object)

            pred_dfs.append(
                session.createDataFrame(
                    pred_pdf[
                        [
                            "State",
                            "County",
                            "Precinct",
                            "n_voters",
                            "p_hat",
                            "model_slug",
                            "model_family",
                            "inference_year",
                            "election_code",
                        ]
                    ]
                )
            )

    # Union ALL per-(year, slug) precinct predictions into ONE view, then aggregate ONCE.
    # Do NOT append per-slug aggregations that each read a reused "_precinct_preds" view:
    # Spark resolves temp-view names lazily at execution, so every appended DataFrame would
    # see the LAST iteration's view — collapsing all years/slugs to the final one and
    # duplicating it. The unique-combination test on this model guards that regression.
    reduce(lambda a, b: a.unionByName(b), pred_dfs).createOrReplaceTempView("_precinct_preds")
    return session.sql(
        """
        SELECT
            CAST(p.inference_year AS INT)    AS election_year,
            p.election_code,
            p.State                          AS state,
            m.district_type,
            m.district_name,
            ROUND(SUM(p.p_hat * p.n_voters)) AS ballots_projected,
            p.model_family                   AS model_version,
            current_timestamp()              AS inference_at
        FROM _precinct_preds p
        JOIN _membership m
          ON  p.State    = m.State
          AND p.County   = m.County
          AND p.Precinct = m.Precinct
        GROUP BY
            p.inference_year, p.election_code, p.State,
            m.district_type, m.district_name, p.model_family
        """
    )
