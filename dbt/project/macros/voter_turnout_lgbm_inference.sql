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

_BOOLEAN_FLAG_COLS = [
    "ConsumerDataLL_Gun_Owner",
    "ConsumerData_Donor_Charitable_Causes",
    "ConsumerData_Donor_Political_Conservative",
    "ConsumerData_Donor_Political_Liberal",
    "ConsumerData_Political_Donor_State_Level",
    "ConsumerData_Current_Affairs_Politics",
    "Cell_Phone_Number_Available",
    "Landline_Phone_Number_Available",
    "Phone_Number_Available",
    "ConsumerData_Do_Not_Call",
]

_NUMERIC_MEAN_COLS = [
    "ConsumerData_Length_Of_Residence_Code",
    "ConsumerData_Household_Number_Lines_Of_Credit",
    "ConsumerData_Number_Of_Persons_in_HH",
    "Residence_Families_HHVotersCount",
    "ConsumerData_AreaMedianEducationYears",
    "ConsumerData_Social_Ranking_Index_by_Area",
    "ConsumerData_Social_Ranking_Index_by_Individual",
    "ConsumerData_Likely_Income_Ranking_by_Area",
    "ConsumerData_Likely_Educational_Attainment_Ranking_by_Area",
    "ConsumerData_Estimated_Income_Amount",
    "ConsumerData_EstimatedAreaMedianHHIncome",
    "ConsumerData_AreaMedianHousingValue",
    "ConsumerData_AreaPcntHHSpanishSpeaking",
]

_NH_VT_PRECINCT = """
    CASE WHEN state_postal_code IN ('NH', 'VT')
         THEN COALESCE(Town_Ward, City_Ward, Town_District, City)
         ELSE CAST(Precinct AS STRING)
    END
"""


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


def _build_precinct_features(session, l2_col_set, election_cols, inference_year):
    exprs = [
        f"state_postal_code                                              AS State",
        f"County",
        f"{_NH_VT_PRECINCT}                                             AS Precinct",
        f"CAST(COUNT(*) AS DOUBLE)                                      AS n_voters",
        (f"CAST(AVG(CAST({inference_year} - YEAR(Voters_BirthDate) AS DOUBLE)) AS DOUBLE)"
         f"                                                               AS age"),
        (f"CAST(AVG(CAST({inference_year} - YEAR(Voters_CalculatedRegDate) AS DOUBLE)) AS DOUBLE)"
         f"                                                               AS reg_for"),
        ("CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL"
         "              THEN 1.0 ELSE 0.0 END) AS DOUBLE)               AS pct_ever_moved"),
        (f"CAST(AVG(CASE WHEN Voters_MovedFrom_Date IS NOT NULL"
         f"              THEN CAST({inference_year} - YEAR(Voters_MovedFrom_Date) AS DOUBLE)"
         f"              ELSE NULL END) AS DOUBLE)"
         f"                                                               AS years_since_moved_at_target"),
        ("CAST(AVG(CASE WHEN LOWER(Voters_Active) = 'active'"
         "              THEN 1.0 ELSE 0.0 END) AS DOUBLE)               AS Voters_Active"),
        ("CAST(AVG(COALESCE(CAST(FECDonors_NumberOfDonations AS DOUBLE), 0.0)) AS DOUBLE)"
         "                                                               AS FECDonors_NumberOfDonations"),
        f"CAST({inference_year} AS DOUBLE)                              AS target_year",
    ]

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

    for col in _CATEGORICAL_FEATURES:
        if col in l2_col_set:
            exprs.append(f"mode(`{col}`)  AS `{col}`")

    for col_name, prefix, year in election_cols:
        if col_name not in l2_col_set:
            continue
        lag = inference_year - year
        if lag < 1 or lag > 12:
            continue
        exprs.append(
            f"CAST(AVG(CASE WHEN `{col_name}` = 'Y' THEN 1.0"
            f"              WHEN `{col_name}` IS NULL THEN NULL"
            f"              ELSE 0.0 END) AS DOUBLE)"
            f"  AS `{prefix}_Minus{lag}`"
        )

    return session.sql(
        f"SELECT {', '.join(exprs)}"
        f" FROM _l2"
        f" GROUP BY state_postal_code, County, {_NH_VT_PRECINCT}"
    )


def _build_district_membership(session, l2_col_set, district_types):
    valid_cols = [dt for dt in district_types if dt in l2_col_set]
    if not valid_cols:
        raise ValueError("No district columns found in L2 schema")

    n = len(valid_cols)
    stack_args = ", ".join(f"'{dt}', `{dt}`" for dt in valid_cols)

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

    for feat in feat_names:
        if feat not in pdf.columns:
            pdf[feat] = np.nan

    X = pdf[feat_names].copy()

    for feat, categories in cat_map.items():
        if feat not in X.columns:
            continue
        cat_idx = {v: i for i, v in enumerate(categories)}
        X[feat] = X[feat].map(cat_idx)

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
        precinct_df = _build_precinct_features(session, l2_col_set, election_cols, inference_year)
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
                    m.district_type, m.district_name, p.model_version
            """))

    return reduce(lambda a, b: a.unionByName(b), result_parts)

{% endmacro %}
