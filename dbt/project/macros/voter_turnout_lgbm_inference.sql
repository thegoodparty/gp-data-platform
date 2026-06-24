{% macro voter_turnout_lgbm_inference(state_code, l2_ref) %}
{#
  Generates the full Python body for the per-state voter turnout inference
  dbt models (int__voter_turnout_lgbm_inference_{state}.py).

  These 51 model files are thin wrappers — each contains only a config()
  block and a call to this macro. All inference logic lives here so it only
  needs to be maintained in one place. To regenerate the wrapper files after
  changing this macro, run scripts/generate_voter_turnout_inference_models.py.
  The generated files are committed to the repo so dbt can discover them as
  static models (dbt Python models require literal strings in dbt.ref() calls,
  which rules out runtime generation).

  Flow (runs once per state after each L2 load):
    1. Load the per-state L2 staging table and add state_postal_code via lit().
    2. Build precinct-level feature vectors in one Spark SQL aggregate per
       inference year (current year + 1 + 2).
    3. Load the needed LightGBM models + cat_maps from MLflow (@production alias),
       or fail loudly if a required model has not been promoted to production.
    4. Predict turnout probability per precinct for each applicable model slug.
    5. Join to a precinct→district membership CTE (built from L2 via STACK)
       and aggregate to district-level ballots_projected.
    6. Write to model_predictions.ballots_projected (incremental merge).

  Parameters
  ----------
  state_code : str   Upper-case postal code, e.g. 'AL'
  l2_ref     : str   dbt model name for the per-state L2 staging view,
                     e.g. 'stg_dbt_source__l2_s3_al_uniform'
#}

import json
import re
import tempfile
from datetime import datetime
from functools import reduce

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from pyspark.sql.functions import lit


_VH_COL_RE = re.compile(r'^(General|Primary|OtherElection|AnyElection)_(\d{4})$')

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

# String indicator columns: L2 stores these as 'Y' / null (or 'Yes' for Gun_Owner).
# Must use string equality, not = TRUE — a string column tested as boolean always
# evaluates FALSE in Spark SQL and would silently produce 0.0 for every precinct.
_Y_INDICATOR_COLS = [
    "ConsumerData_Donor_Charitable_Causes",
    "ConsumerData_Donor_Political_Conservative",
    "ConsumerData_Donor_Political_Liberal",
    "ConsumerData_Political_Donor_State_Level",
    "ConsumerData_Current_Affairs_Politics",
]
# Gun_Owner uses 'Yes' instead of 'Y'
_YES_INDICATOR_COLS = [
    "ConsumerDataLL_Gun_Owner",
]
# Actual boolean columns in L2 — safe to check with = TRUE
_BOOLEAN_FLAG_COLS = [
    "Cell_Phone_Number_Available",
    "Landline_Phone_Number_Available",
    "Phone_Number_Available",
    "ConsumerData_Do_Not_Call",
]

# Plain numeric columns — cast directly to DOUBLE before AVG.
# CAST AS DOUBLE required: without it Spark AVG returns DECIMAL(38,18) which
# pandas 3 represents as StringDtype, causing LightGBM to reject the column.
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

_NH_VT_PRECINCT = """
    CASE WHEN state_postal_code IN ('NH', 'VT')
         THEN COALESCE(Town_Ward, City_Ward, Town_District, City)
         ELSE CAST(Precinct AS STRING)
    END
"""

# States that hold odd-year elections statewide — eligible non-voters in odd-year
# AnyElection_ columns always get 0 here (no per-precinct opportunity check needed).
# Mirrors ODD_YEAR_OPPORTUNITY_STATES in the training notebook.
_ODD_YEAR_OPPORTUNITY_STATES = frozenset([
    'AL', 'CO', 'CT', 'GA', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'MA',
    'MI', 'MS', 'MT', 'NE', 'NH', 'NJ', 'NM', 'NY', 'NC', 'OH', 'OR', 'PA',
    'UT', 'VA', 'VT', 'WA', 'WI',
])


def _year_to_model_slugs(year):
    # If a needed slug has no @production model, the pipeline fails here — intentionally.
    # Train the missing model (the _lagN convention means you can train on existing
    # vote history without waiting for new data) rather than adding a fallback.
    if year % 2 != 0:
        return ["off_year_local_lag2"]
    elif year % 4 == 2:
        return ["midterm", "even_year_local"]
    else:
        return ["presidential_lag3", "even_year_local"]


_SLUG_ELECTION_CODE = {
    "presidential_lag3":   "General",
    "midterm":             "General",
    "even_year_local":     "Local_or_Municipal",
    "off_year_local_lag2": "Local_or_Municipal",
}


def _check_lgbm_version(registered_model_name, client):
    import lightgbm as lgb
    tag = client.get_registered_model(registered_model_name).tags.get("lightgbm_version")
    if not tag:
        print(f"WARNING: lightgbm_version tag not set on {registered_model_name}. Skipping version check.")
        return
    trained_major = int(tag.split(".")[0])
    running_major = int(lgb.__version__.split(".")[0])
    if running_major != trained_major:
        raise RuntimeError(
            f"LightGBM major version mismatch on {registered_model_name}: "
            f"trained={tag}, cluster={lgb.__version__}. "
            f"Re-run promote_models_to_prod.py after upgrading the cluster library."
        )
    if tag != lgb.__version__:
        print(f"WARNING: LightGBM minor version mismatch on {registered_model_name} "
              f"(trained={tag}, cluster={lgb.__version__}). Proceeding.")


def _detect_election_cols(l2_columns, max_vote_history_year):
    result = []
    for col in l2_columns:
        m = _VH_COL_RE.match(col)
        if m and int(m.group(2)) <= max_vote_history_year:
            result.append((col, m.group(1), int(m.group(2))))
    return result


def _build_precinct_features(session, l2_col_set, election_cols, inference_year,
                              l2_collection_year, state_code, catalog, models_schema):
    # to_date with explicit format handles L2's MM/dd/yyyy string storage.
    # COALESCE with TRY_CAST also handles staging tables where the column
    # is already DATE type.
    _safe_date = lambda col: (
        f"COALESCE(TRY_CAST(`{col}` AS DATE), "
        f"to_date(CAST(`{col}` AS STRING), 'MM/dd/yyyy'))"
    )

    # ── Vote history eligibility setup ────────────────────────────────────────
    # Odd-year AnyElection/OtherElection columns in states without statewide odd-year
    # elections require a per-precinct opportunity check against turnout_historical_precincts.
    # All other columns can determine eligibility from column prefix + year alone.
    in_opportunity_state = state_code in _ODD_YEAR_OPPORTUNITY_STATES

    odd_nonop_years = sorted({
        year
        for col_name, prefix, year in election_cols
        if col_name in l2_col_set
        and prefix in ('AnyElection', 'OtherElection')
        and year % 2 == 1
        and not in_opportunity_state
        and 1 <= inference_year - year <= 12
    })

    # For non-opportunity states with odd-year local columns, pre-build a
    # precinct-level opportunity flag table and enrich _l2 with it via a subquery.
    # The subquery avoids column-name ambiguity in the GROUP BY.
    if odd_nonop_years:
        opp_col_exprs = ", ".join(
            f"MAX(CASE WHEN election_year_str = 'AnyElection_{y}'"
            f"          OR election_year_str = 'OtherElection_{y}' THEN 1 ELSE 0 END)"
            f" AS opp_{y}"
            for y in odd_nonop_years
        )
        year_filter = " OR ".join(
            f"election_year_str IN ('AnyElection_{y}', 'OtherElection_{y}')"
            for y in odd_nonop_years
        )
        session.sql(f"""
            SELECT County, Precinct, {opp_col_exprs}
            FROM {catalog}.{models_schema}.turnout_historical_precincts
            WHERE State = '{state_code}' AND ({year_filter})
            GROUP BY County, Precinct
        """).createOrReplaceTempView("_hp_opp")

        opp_select = ", ".join(
            f"COALESCE(hp.opp_{y}, 0) AS opp_{y}"
            for y in odd_nonop_years
        )
        from_clause = (
            f"(SELECT l2.*, {opp_select}"
            f" FROM _l2 AS l2"
            f" LEFT JOIN _hp_opp AS hp"
            f"   ON l2.County = hp.County"
            f"  AND CAST(l2.Precinct AS STRING) = hp.Precinct) AS _enriched"
        )
    else:
        from_clause = "_l2"

    exprs = [
        f"state_postal_code                                              AS State",
        f"County",
        f"{_NH_VT_PRECINCT}                                             AS Precinct",
        f"CAST(COUNT(*) AS DOUBLE)                                      AS n_voters",

        # Age and registration tenure as of Nov 1 of the inference year — matches
        # training which used DATEDIFF(MAKE_DATE(target_year, 11, 1), col) / 365.25.
        (f"CAST(AVG(DATEDIFF(MAKE_DATE({inference_year}, 11, 1),"
         f"                  {_safe_date('Voters_BirthDate')}) / 365.25) AS DOUBLE)"
         f"                                                               AS age"),
        (f"CAST(AVG(DATEDIFF(MAKE_DATE({inference_year}, 11, 1),"
         f"                  {_safe_date('Voters_CalculatedRegDate')}) / 365.25) AS DOUBLE)"
         f"                                                               AS reg_for"),

        # Residential mobility signals
        ("CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL"
         "              THEN 1.0 ELSE 0.0 END) AS DOUBLE)               AS pct_ever_moved"),
        # Use _safe_date to extract the year — handles both DATE-typed staging columns
        # (where CAST AS STRING gives yyyy-MM-dd, breaking SUBSTRING-based year extraction)
        # and raw L2 MM/dd/yyyy strings. Training used the MM/dd/yyyy path only.
        (f"CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL"
         f"              THEN CAST({inference_year}"
         f"                   - YEAR({_safe_date('Voters_MovedFrom_Date')})"
         f"                   AS DOUBLE)"
         f"              ELSE NULL END) AS DOUBLE)"
         f"                                                               AS years_since_moved_at_target"),

        # Voters_Active: L2 raw uses 'A'/'I'; some staging tables normalise to 'active'/'inactive'
        ("CAST(AVG(CASE WHEN UPPER(Voters_Active) IN ('A', 'ACTIVE')   THEN 1.0"
         "              WHEN UPPER(Voters_Active) IN ('I', 'INACTIVE') THEN 0.0"
         "              ELSE NULL END) AS DOUBLE)                        AS Voters_Active"),

        # FECDonors: null means never donated — treat as 0 before averaging
        ("CAST(AVG(COALESCE(CAST(FECDonors_NumberOfDonations AS DOUBLE), 0.0)) AS DOUBLE)"
         "                                                               AS FECDonors_NumberOfDonations"),

        f"CAST({inference_year} AS DOUBLE)                              AS target_year",

        # Length of residence projected to inference year. L2 records length as of
        # collection date (l2_collection_year = datetime.now().year at run time).
        # Training applied col - (CURRENT_YEAR - target_year); same formula here.
        (f"CAST(AVG(ConsumerData_Length_Of_Residence_Code"
         f"         - ({l2_collection_year} - {inference_year})) AS DOUBLE)"
         f"                                                               AS ConsumerData_Length_Of_Residence_Code"),
    ]

    for col in _Y_INDICATOR_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{col}` = 'Y' THEN 1.0 ELSE 0.0 END) AS DOUBLE)"
                f"  AS `{col}`"
            )

    for col in _YES_INDICATOR_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{col}` = 'Yes' THEN 1.0 ELSE 0.0 END) AS DOUBLE)"
                f"  AS `{col}`"
            )

    for col in _BOOLEAN_FLAG_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CASE WHEN `{col}` = TRUE THEN 1.0 ELSE 0.0 END) AS DOUBLE)"
                f"  AS `{col}`"
            )

    for col in _NUMERIC_MEAN_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CAST(`{col}` AS DOUBLE)) AS DOUBLE)  AS `{col}`"
            )

    for col in _DOLLAR_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CAST(REPLACE(`{col}`, '$', '') AS DOUBLE)) AS DOUBLE)  AS `{col}`"
            )

    for col in _PCT_COLS:
        if col in l2_col_set:
            exprs.append(
                f"CAST(AVG(CAST(REPLACE(`{col}`, '%', '') AS DOUBLE)) AS DOUBLE)  AS `{col}`"
            )

    for col in _CATEGORICAL_FEATURES:
        if col in l2_col_set:
            exprs.append(f"mode(`{col}`)  AS `{col}`")

    # ── Vote history columns ───────────────────────────────────────────────────
    # Replicates the 8-branch CASE from training's vote_history_sql (valued CTE):
    #   1. voted = 'Y'                              → 1
    #   2. BirthDate IS NULL                        → NULL
    #   3. age < 18 at election year                → NULL
    #   4. registered after election year           → NULL
    #   5. General_ or Primary_                     → 0  (always held)
    #   6. even year                                → 0  (even-year local opportunity assumed)
    #   7. state in ODD_YEAR_OPPORTUNITY_STATES     → 0  (statewide odd-year elections)
    #   8. precinct had opportunity (from table)    → 0  (odd-year local precinct check)
    #   else                                        → NULL
    # Branches 5–8 are resolved at Python generation time where possible; only
    # branch 8 requires a JOIN to turnout_historical_precincts (pre-built above).
    for col_name, prefix, year in election_cols:
        if col_name not in l2_col_set:
            continue
        lag = inference_year - year
        if lag < 1 or lag > 12:
            continue

        if prefix in ('General', 'Primary'):
            eligible_nonvoter_sql = "ELSE 0.0"                          # branch 5
        elif year % 2 == 0:
            eligible_nonvoter_sql = "ELSE 0.0"                          # branch 6
        elif in_opportunity_state:
            eligible_nonvoter_sql = "ELSE 0.0"                          # branch 7
        else:
            # Branch 8: non-opportunity state, odd year — check precinct
            eligible_nonvoter_sql = f"WHEN opp_{year} = 1 THEN 0.0 ELSE NULL"

        exprs.append(
            f"CAST(AVG("
            f"  CASE WHEN `{col_name}` = 'Y' THEN 1.0"
            f"       WHEN {_safe_date('Voters_BirthDate')} IS NULL THEN NULL"
            f"       WHEN ({year} - YEAR({_safe_date('Voters_BirthDate')})) < 18 THEN NULL"
            f"       WHEN `Voters_CalculatedRegDate` IS NOT NULL"
            f"            AND YEAR({_safe_date('Voters_CalculatedRegDate')}) > {year} THEN NULL"
            f"       {eligible_nonvoter_sql}"
            f"  END"
            f") AS DOUBLE)"
            f"  AS `{prefix}_Minus{lag}`"
        )

    return session.sql(
        f"SELECT {', '.join(exprs)}"
        f" FROM {from_clause}"
        f" GROUP BY state_postal_code, County, {_NH_VT_PRECINCT}"
    )


def _build_district_membership(session, l2_col_set, district_types):
    valid_cols = [dt for dt in district_types if dt in l2_col_set]
    if not valid_cols:
        raise ValueError("No district columns found in L2 schema")

    n = len(valid_cols)
    stack_args = ", ".join(f"'{dt}', CAST(`{dt}` AS STRING)" for dt in valid_cols)

    return session.sql(f"""
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
    """)


def _predict_precinct(pdf, booster, cat_map, model_slug, model_version, inference_year):
    feat_names = booster.feature_name_

    # Add NaN placeholders for any features the model expects but L2 lacks
    for feat in feat_names:
        if feat not in pdf.columns:
            pdf[feat] = np.nan

    X = pdf[feat_names].copy()

    # Integer-encode categoricals using saved cat_map.
    # Values absent from training become NaN — LightGBM treats these as missing.
    for feat, categories in cat_map.items():
        if feat not in X.columns:
            continue
        cat_idx = {v: i for i, v in enumerate(categories)}
        X[feat] = X[feat].map(cat_idx)

    # Coerce any remaining non-numeric columns (e.g. State, County if present in
    # feat_names, or Arrow-backed StringDtype categoricals not in cat_map) to float.
    # pd.api.types.is_numeric_dtype catches both object and StringDtype — the latter
    # is returned by Arrow-backed reads on Databricks and is missed by dtype == object.
    still_str = [c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])]
    if still_str:
        print(f"WARNING: coercing {len(still_str)} non-numeric feature columns to NaN: {still_str}")
        for c in still_str:
            X[c] = pd.to_numeric(X[c], errors="coerce")

    result = pdf[["State", "County", "Precinct", "n_voters"]].copy()
    result["p_hat"]          = booster._Booster.predict(
        X.to_numpy(dtype=float, na_value=np.nan)
    )
    result["model_slug"]     = model_slug
    result["model_family"]   = model_version
    result["inference_year"] = inference_year
    result["election_code"]  = _SLUG_ELECTION_CODE[model_slug]
    return result


def model(dbt, session):
    max_vote_history_year = int("{{ var('max_vote_history_year', 2025) }}")
    current_year    = datetime.now().year
    inference_years = [current_year, current_year + 1, current_year + 2]

    catalog        = dbt.config.get("database")  # Unity Catalog name; follows the dbt project
    models_schema  = "{{ var('voter_turnout_models_schema', 'model_predictions') }}"

    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    needed_slugs = sorted({
        slug
        for year in inference_years
        for slug in _year_to_model_slugs(year)
    })

    models, cat_maps = {}, {}
    model_family = None
    for slug in needed_slugs:
        full_name = f"{catalog}.{models_schema}.voter_turnout_model_{slug}"
        _check_lgbm_version(full_name, client)
        mv = client.get_model_version_by_alias(full_name, "production")
        models[slug] = mlflow.lightgbm.load_model(f"models:/{full_name}@production")
        with tempfile.TemporaryDirectory() as tmp:
            local_cat = mlflow.artifacts.download_artifacts(
                run_id=mv.run_id,
                artifact_path=f"{slug}_categorical_feature_map.json",
                dst_path=tmp,
            )
            with open(local_cat) as f:
                cat_maps[slug] = json.load(f)
            if model_family is None:
                local_mf = mlflow.artifacts.download_artifacts(
                    run_id=mv.run_id,
                    artifact_path="model_family.json",
                    dst_path=tmp,
                )
                with open(local_mf) as f:
                    model_family = json.load(f)["model_family"]

    l2 = dbt.ref("{{ l2_ref }}").withColumn("state_postal_code", lit("{{ state_code }}"))
    l2.cache()
    l2.createOrReplaceTempView("_l2")
    l2_col_set = set(l2.columns)

    election_cols = _detect_election_cols(l2.columns, max_vote_history_year)

    dist_types = [
        r.district_type
        for r in dbt.ref("int__l2_district_aggregations")
                     .select("district_type").distinct().collect()
        if r.district_type and r.district_type != "State"
    ]
    membership_df = _build_district_membership(session, l2_col_set, dist_types)
    membership_df.createOrReplaceTempView("_membership")

    result_parts = []

    for inference_year in inference_years:
        precinct_df = _build_precinct_features(
            session, l2_col_set, election_cols, inference_year, current_year,
            "{{ state_code }}", catalog, models_schema
        )
        # NOTE FOR FUTURE AGENTS: pulling the full precinct feature DataFrame into
        # driver memory via toPandas() is safe at per-state grain (largest states
        # are ~15k precincts). If this pipeline is ever changed to aggregate
        # nationwide rather than per-state, replace this call with a write to a
        # persisted intermediate Delta table (e.g. catalog.private_schema.precinct_features_tmp),
        # then read it back with spark.table() before calling toPandas() to avoid OOM.
        pdf = precinct_df.toPandas()

        for slug in _year_to_model_slugs(inference_year):
            pred_pdf = _predict_precinct(
                pdf.copy(),
                booster        = models[slug],
                cat_map        = cat_maps[slug],
                model_slug     = slug,
                model_version  = model_family,
                inference_year = inference_year,
            )

            # Convert Arrow-backed StringDtype columns (State, County, Precinct) to
            # plain object dtype before createDataFrame — Spark cannot convert
            # ChunkedArray from the Arrow-backed string representation.
            for col in pred_pdf.columns:
                if not pd.api.types.is_numeric_dtype(pred_pdf[col]):
                    pred_pdf[col] = pred_pdf[col].astype(object)

            preds_spark = session.createDataFrame(
                pred_pdf[["State", "County", "Precinct", "n_voters",
                           "p_hat", "model_slug", "model_family",
                           "inference_year", "election_code"]]
            )
            preds_spark.createOrReplaceTempView("_precinct_preds")

            result_parts.append(session.sql("""
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
            """))

    return reduce(lambda a, b: a.unionByName(b), result_parts)

{% endmacro %}
