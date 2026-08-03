"""Nationwide PER-VOTER turnout LightGBM inference (precinct-of-1).

Live, incremental analog of int__voter_turnout_lgbm_inference. That model scores
PRECINCTS and aggregates to districts; this one scores each individual voter by
treating them as a degenerate "precinct of 1", so the precinct feature
expressions collapse: AVG(x) -> x, mode(x) -> x, COUNT(*) -> 1, and the
vote-history / mobility features become 0 / 1 / NULL instead of precinct rates.
One row per LALVOTERID for the current year's November election. Output drives
people-api Voter_Status.

Scoring a voter as a precinct-of-1 is intentionally out-of-distribution (the
model was trained on precinct averages): per-voter probabilities skew low and are
compressed at the extremes, but aggregate consistently back to the precinct
model. The encode-then-predict path here is byte-for-byte the same as the
district model's precinct path; only the input grain differs (row-level, no
GROUP BY).

Incremental (merge on LALVOTERID): each run scores only LALVOTERIDs not already
in the target (a left-anti join against dbt.this), so scheduled runs never
rescore the full ~218M-row universe. A --full-refresh (or the first build, when
the relation does not yet exist) scores everyone once with the current
@production model. New rows added incrementally are scored by whatever model is
@production at that time; a full refresh is the way to re-stamp the whole table
after a retrain.

Distributed scoring: the cluster runs in USER_ISOLATION mode, so sparkContext
(broadcast / addFile) is unavailable and the ~150 MB booster overflows the gRPC
closure limit. The driver stages the booster to a UC Volume; the mapInPandas
closure carries only the Volume path (plus cat_map / feat_names), and each
executor reads the model file directly, cached once per worker process. This is
the same pattern proven for this scoring job on the shared cluster.

The SQL-building helpers are pure (return SQL strings) so they are unit-tested
without Spark/MLflow; model() executes them. `import mlflow` is deferred into
model() so this module imports in the dbt test env (pyspark + pandas, no mlflow).
"""

import glob
import json
import os
import re
import shutil
import tempfile
from datetime import datetime

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

# String indicator columns: L2 stores these as 'Y' / null. Use string equality.
_Y_INDICATOR_COLS = [
    "ConsumerData_Donor_Charitable_Causes",
    "ConsumerData_Donor_Political_Conservative",
    "ConsumerData_Donor_Political_Liberal",
    "ConsumerData_Political_Donor_State_Level",
    "ConsumerData_Current_Affairs_Politics",
]
# Gun_Owner uses 'Yes' instead of 'Y'.
_YES_INDICATOR_COLS = ["ConsumerDataLL_Gun_Owner"]
# Actual boolean columns in L2 — safe to check with = TRUE.
_BOOLEAN_FLAG_COLS = [
    "Cell_Phone_Number_Available",
    "Landline_Phone_Number_Available",
    "Phone_Number_Available",
    "ConsumerData_Do_Not_Call",
]
# Plain numeric columns — cast to DOUBLE so pandas does not receive DECIMAL(38,18).
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
_PCT_COLS = ["ConsumerData_AreaPcntHHSpanishSpeaking"]

# Precinct key: NH/VT use ward/town names (their raw Precinct is mostly NULL);
# everywhere else the raw precinct. Voters with no precinct key are unscoreable by
# the precinct model and are excluded (matches the precinct-path coverage).
_NH_VT_PRECINCT = """
    CASE WHEN state_postal_code IN ('NH', 'VT')
         THEN COALESCE(Town_Ward, City_Ward, Town_District, City)
         ELSE CAST(Precinct AS STRING)
    END
"""

# States that hold odd-year elections statewide — eligible non-voters in odd-year
# AnyElection_ columns always get 0 (no per-precinct opportunity check needed).
_ODD_YEAR_OPPORTUNITY_STATES = frozenset(
    [
        "AL", "CO", "CT", "GA", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "MA",
        "MI", "MS", "MT", "NE", "NH", "NJ", "NM", "NY", "NC", "OH", "OR", "PA",
        "UT", "VA", "VT", "WA", "WI",
    ]
)
_OPP_STATES_SQL = "(" + ", ".join(f"'{s}'" for s in sorted(_ODD_YEAR_OPPORTUNITY_STATES)) + ")"


def _detect_election_cols(l2_columns, max_vote_history_year):
    result = []
    for column_name in l2_columns:
        m = _VH_COL_RE.match(column_name)
        if m and int(m.group(2)) <= max_vote_history_year:
            result.append((column_name, m.group(1), int(m.group(2))))
    return result


def _november_model_for_year(year):
    """(model_slug, election_code) for the November election of `year`, collapsed to
    the single projection this table serves: even years are the general (midterm vs
    presidential by the 4-year cycle), odd years are the off-year November
    local/municipal. Mirrors the district model's routing but keeps one row per
    voter. If the needed slug has no @production model the run fails downstream,
    intentionally."""
    if year % 2 == 1:
        return "off_year_local_lag2", "Local_or_Municipal"
    if year % 4 == 2:
        return "midterm", "General"
    return "presidential_lag3", "General"


def _safe_date(column_name):
    # to_date handles L2's MM/dd/yyyy string storage; the TRY_CAST branch handles
    # columns already stored as DATE (Voters_BirthDate / Voters_CalculatedRegDate).
    return (
        f"COALESCE(TRY_CAST(`{column_name}` AS DATE), "
        f"to_date(CAST(`{column_name}` AS STRING), 'MM/dd/yyyy'))"
    )


def _op_years(election_cols, l2_col_set, inference_year):
    """Years (in lag range) needing a per-precinct opportunity flag: odd-year
    AnyElection (non-opportunity-state voters) and even-year OtherElection."""
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
    """Nationwide per-precinct opportunity-flag table.

    precincts_schema = where turnout_historical_precincts lives. PROD default is
    model_predictions (promoted there alongside the models); dev may override.
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


def _build_voter_features_sql(l2_col_set, election_cols, inference_year, l2_collection_year):
    """Pure: voter-grain feature SQL over the _l2 / _enriched view.

    Same expressions as the district model's _build_precinct_features_sql, but
    ROW-LEVEL (no GROUP BY): AVG(f(x)) -> f(x), mode(x) -> x, COUNT(*) -> 1. One
    row per voter, keyed by LALVOTERID. Only voters with a precinct key are kept.
    """
    op_years = _op_years(election_cols, l2_col_set, inference_year)
    if op_years:
        opp_select = ", ".join(f"COALESCE(hp.opp_{y}, 0) AS opp_{y}" for y in op_years)
        from_clause = (
            f"(SELECT l2.*, {opp_select} FROM _l2 AS l2"
            f" LEFT JOIN _hp_opp AS hp"
            f"   ON l2.state_postal_code = hp.State"
            f"  AND l2.County = hp.County"
            f"  AND CAST(l2.Precinct AS STRING) = hp.Precinct) AS _enriched"
        )
    else:
        from_clause = "_l2"

    exprs = [
        # ---- identity + geography (not model features) ----
        "LALVOTERID",
        "state_postal_code AS state",
        # ---- model features (precinct-of-1: AVG collapses to the value itself) ----
        "CAST(1.0 AS DOUBLE) AS n_voters",
        (
            f"CAST(DATEDIFF(MAKE_DATE({inference_year}, 11, 1), "
            f"{_safe_date('Voters_BirthDate')}) / 365.25 AS DOUBLE) AS age"
        ),
        (
            f"CAST(DATEDIFF(MAKE_DATE({inference_year}, 11, 1), "
            f"{_safe_date('Voters_CalculatedRegDate')}) / 365.25 AS DOUBLE) AS reg_for"
        ),
        (
            "CAST(CASE WHEN Voters_MovedFrom_Date IS NOT NULL "
            "THEN 1.0 ELSE 0.0 END AS DOUBLE) AS pct_ever_moved"
        ),
        (
            f"CAST(CASE WHEN Voters_MovedFrom_Date IS NOT NULL "
            f"THEN CAST({inference_year} - YEAR({_safe_date('Voters_MovedFrom_Date')}) AS DOUBLE) "
            f"ELSE NULL END AS DOUBLE) AS years_since_moved_at_target"
        ),
        (
            "CAST(CASE WHEN UPPER(Voters_Active) IN ('A', 'ACTIVE') THEN 1.0 "
            "WHEN UPPER(Voters_Active) IN ('I', 'INACTIVE') THEN 0.0 "
            "ELSE NULL END AS DOUBLE) AS Voters_Active"
        ),
        (
            "CAST(COALESCE(CAST(FECDonors_NumberOfDonations AS DOUBLE), 0.0) AS DOUBLE) "
            "AS FECDonors_NumberOfDonations"
        ),
        f"CAST({inference_year} AS DOUBLE) AS target_year",
        (
            f"CAST(ConsumerData_Length_Of_Residence_Code "
            f"- ({l2_collection_year} - {inference_year}) AS DOUBLE) "
            f"AS ConsumerData_Length_Of_Residence_Code"
        ),
    ]

    for column_name in _Y_INDICATOR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(CASE WHEN `{column_name}` = 'Y' THEN 1.0 ELSE 0.0 END AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _YES_INDICATOR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(CASE WHEN `{column_name}` = 'Yes' THEN 1.0 ELSE 0.0 END AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _BOOLEAN_FLAG_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(CASE WHEN `{column_name}` = TRUE THEN 1.0 ELSE 0.0 END AS DOUBLE) "
                f"AS `{column_name}`"
            )
    for column_name in _NUMERIC_MEAN_COLS:
        if column_name in l2_col_set:
            exprs.append(f"CAST(`{column_name}` AS DOUBLE) AS `{column_name}`")
    for column_name in _DOLLAR_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(REPLACE(`{column_name}`, '$', '') AS DOUBLE) AS `{column_name}`"
            )
    for column_name in _PCT_COLS:
        if column_name in l2_col_set:
            exprs.append(
                f"CAST(REPLACE(`{column_name}`, '%', '') AS DOUBLE) AS `{column_name}`"
            )
    for column_name in _CATEGORICAL_FEATURES:
        if column_name in l2_col_set:
            exprs.append(f"`{column_name}`")  # mode(x) over 1 row -> x itself

    # Vote-history columns: replicates training's eligibility CASE. The "voted" test
    # is a BOOLEAN (cols are BooleanType nationwide), not = 'Y'.
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
                f"WHEN state_postal_code IN {_OPP_STATES_SQL} THEN 0.0 "
                f"WHEN opp_{year} = 1 THEN 0.0 ELSE NULL"
            )
        else:  # OtherElection
            eligible_nonvoter_sql = f"WHEN opp_{year} = 1 THEN 0.0 ELSE NULL"
        exprs.append(
            f"CAST("
            f"  CASE WHEN `{col_name}` THEN 1.0"
            f"       WHEN {_safe_date('Voters_BirthDate')} IS NULL THEN NULL"
            f"       WHEN ({year} - YEAR({_safe_date('Voters_BirthDate')})) < 18 THEN NULL"
            f"       WHEN `Voters_CalculatedRegDate` IS NOT NULL"
            f"            AND YEAR({_safe_date('Voters_CalculatedRegDate')}) > {year} THEN NULL"
            f"       {eligible_nonvoter_sql}"
            f"  END"
            f" AS DOUBLE)"
            f"  AS `{prefix}_Minus{lag}`"
        )

    return (
        f"SELECT {', '.join(exprs)} FROM {from_clause} WHERE {_NH_VT_PRECINCT} IS NOT NULL"
    )


def _parse_state_allowlist(raw):
    """DEV-ONLY iteration knob to score a subset of states (e.g. one small state)
    so the model can be validated without a full ~218M-row run."""
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


def _select_cat_map_path(candidate_paths):
    """Resolve the single categorical-feature-map file in a model's artifacts.
    Fail loud on zero or multiple matches — a missing or ambiguous map would
    silently mis-encode features and corrupt predictions."""
    if len(candidate_paths) != 1:
        raise ValueError(
            f"Expected exactly one categorical_feature_map.json in the model artifacts, "
            f"found {len(candidate_paths)}: {sorted(candidate_paths)}"
        )
    return candidate_paths[0]


def _read_model_family_tag(registered_model_tags, registered_model_name):
    """Read the load-bearing model_family from the registered-model tag set at
    promotion. Stamped on every row as model_version, so a missing tag must fail
    rather than default."""
    family = (registered_model_tags or {}).get("model_family")
    if not family:
        raise ValueError(
            f"Registered model {registered_model_name} is missing the required 'model_family' "
            f"tag. Set it at promotion (the promote script tags each model)."
        )
    return family


def _make_scorer(model_file, cat_map, feat_names):
    """Build the mapInPandas closure. Carries only the Volume path (tiny) plus
    cat_map / feat_names; each executor reads the booster from the Volume once per
    worker process. Encode-then-predict is identical to the district model."""

    def _score_partition(iterator):
        import builtins

        import lightgbm as lgb

        cache = getattr(builtins, "_GP_VOTER_BOOSTER_CACHE", None)
        if cache is None:
            cache = {}
            builtins._GP_VOTER_BOOSTER_CACHE = cache
        if model_file not in cache:
            cache[model_file] = lgb.Booster(model_file=model_file)
        booster = cache[model_file]

        for pdf in iterator:
            if len(pdf) == 0:
                continue
            x = pd.DataFrame(index=pdf.index)
            for feat in feat_names:
                x[feat] = pdf[feat] if feat in pdf.columns else np.nan
            # Integer-encode categoricals via the saved cat_map; unseen -> NaN.
            for feat, categories in cat_map.items():
                if feat in x.columns:
                    cat_idx = {v: i for i, v in enumerate(categories)}
                    x[feat] = x[feat].map(cat_idx)
            # Coerce any remaining non-numeric feature columns to NaN.
            for c in x.columns:
                if not pd.api.types.is_numeric_dtype(x[c]):
                    x[c] = pd.to_numeric(x[c], errors="coerce")
            preds = booster.predict(x[feat_names].to_numpy(dtype=float, na_value=np.nan))
            out = pdf[["LALVOTERID", "state"]].copy()
            out["prob_vote"] = preds
            # prediction: binary vote/no-vote at the 0.5 threshold (matches the
            # promoted snapshot exactly — verified 218,102,378/218,102,378 rows).
            out["prediction"] = np.where(out["prob_vote"] >= 0.5, 1.0, 0.0)
            yield out

    return _score_partition


def model(dbt, session):
    import mlflow
    import mlflow.lightgbm

    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        tags=["intermediate", "l2", "model_prediction", "voter_turnout"],
    )

    # Dynamic year: default to the current calendar year (evaluated on the cluster at
    # run time), so the projection turns the page on Jan 1 with nothing hardcoded. The
    # whole table only advances on a --full-refresh (incremental skips voters already
    # scored), so schedule an annual full refresh right after the new year. The var is
    # an override for backfills. model_slug / election_code default to the November
    # election for that year; a full manual override needs both vars set.
    current_year = datetime.now().year
    year_override = dbt.config.meta_get("voter_turnout_inference_year")
    inference_year = int(year_override) if year_override else current_year
    slug_override = dbt.config.meta_get("voter_turnout_model_slug")
    code_override = dbt.config.meta_get("voter_turnout_election_code")
    if slug_override and code_override:
        model_slug, election_code = slug_override, code_override
    else:
        model_slug, election_code = _november_model_for_year(inference_year)
    # Most recent year L2 has COMPLETE vote history. Manually maintained (the same
    # value the district model uses), NOT auto-detected: L2 pre-provisions empty
    # future vote-history columns, so column presence is not data presence. Bump it
    # when L2 gains a new complete year. The lag window still tracks the target
    # because lag = inference_year - year, so it shifts as inference_year advances.
    max_vote_history_year = int(dbt.config.meta_get("max_vote_history_year") or 2025)
    models_schema = dbt.config.meta_get("voter_turnout_models_schema") or "model_predictions"
    precincts_schema = dbt.config.meta_get("voter_turnout_precincts_schema") or "model_predictions"
    # UC Volume the driver stages the booster to and executors read it from. PROD
    # needs a Volume in a shared schema (model_predictions has none today — create one
    # at promotion). Dev can point at private_nigel/object_storage via the var.
    booster_volume = (
        dbt.config.meta_get("voter_turnout_booster_volume")
        or "/Volumes/goodparty_data_catalog/model_predictions/object_storage"
    )
    state_allowlist = _parse_state_allowlist(dbt.config.meta_get("l2_state_allowlist"))
    catalog = "goodparty_data_catalog"

    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    full_name = f"{catalog}.{models_schema}.voter_turnout_model_{model_slug}"
    _check_lgbm_version(full_name, client)
    model_family = _read_model_family_tag(client.get_registered_model(full_name).tags, full_name)

    tmp = tempfile.mkdtemp()
    model_dir = mlflow.artifacts.download_artifacts(
        artifact_uri=f"models:/{full_name}@production", dst_path=tmp
    )
    sk_model = mlflow.lightgbm.load_model(model_dir)
    feat_names = list(sk_model.feature_name_)
    cat_path = _select_cat_map_path(
        glob.glob(os.path.join(model_dir, "**", "*categorical_feature_map.json"), recursive=True)
    )
    with open(cat_path) as f:
        cat_map = json.load(f)

    # Stage the booster to the Volume so USER_ISOLATION executors can read it
    # directly (no sparkContext broadcast, no executor-side MLflow).
    model_file = f"{booster_volume}/voter_turnout_{model_slug}_booster.txt"
    local_booster = os.path.join(tmp, "booster.txt")
    sk_model._Booster.save_model(local_booster)
    shutil.copyfile(local_booster, model_file)

    l2 = dbt.ref("int__l2_nationwide_uniform")
    if state_allowlist:
        l2 = l2.filter(col("state_postal_code").isin(sorted(state_allowlist)))
    # Incremental: score only LALVOTERIDs not already in the target. This is the
    # guard that keeps scheduled runs off the full ~218M-row universe.
    if dbt.is_incremental:
        already_scored = session.table(f"{dbt.this}").select("LALVOTERID")
        l2 = l2.join(already_scored, on="LALVOTERID", how="left_anti")
    l2.createOrReplaceTempView("_l2")
    l2_col_set = set(l2.columns)
    election_cols = _detect_election_cols(l2.columns, max_vote_history_year)

    op_years = _op_years(election_cols, l2_col_set, inference_year)
    if op_years:
        session.sql(_opp_view_sql(op_years, catalog, precincts_schema)).createOrReplaceTempView(
            "_hp_opp"
        )

    # l2_collection_year is the current year (when L2 was collected); it only differs
    # from inference_year on a backfill override, where the length-of-residence
    # projection must shift accordingly.
    voter_features = session.sql(
        _build_voter_features_sql(l2_col_set, election_cols, inference_year, current_year)
    )

    out_schema = (
        "LALVOTERID string, state string, prob_vote double, prediction double"
    )
    scored = voter_features.mapInPandas(
        _make_scorer(model_file, cat_map, feat_names), schema=out_schema
    )
    scored.createOrReplaceTempView("_scored")

    # Stamp the fixed dimensional columns once, in SQL, to match the promoted
    # snapshot schema (LALVOTERID, prob_vote, prediction, election_year,
    # election_code, model_version, state).
    return session.sql(
        f"""
        SELECT
            LALVOTERID,
            prob_vote,
            prediction,
            CAST({inference_year} AS INT) AS election_year,
            '{election_code}'             AS election_code,
            '{model_family}'              AS model_version,
            state
        FROM _scored
        """
    )
