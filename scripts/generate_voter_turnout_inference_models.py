"""
Generate 51 per-state dbt Python model files for voter turnout LightGBM inference.

Each file is a thin Jinja wrapper that calls the voter_turnout_lgbm_inference macro.
All inference logic lives in macros/voter_turnout_lgbm_inference.sql.

Run with:
    uv run scripts/generate_voter_turnout_inference_models.py

Re-run after any change to the macro to regenerate all files.
"""
import os

STATES = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
    "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME",
    "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM",
    "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VA", "VT", "WA", "WI", "WV", "WY",
]

OUT_DIR = os.path.join(
    os.path.dirname(__file__),
    "../dbt/project/models/intermediate/l2",
)

TEMPLATE = """\
{{{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['state', 'election_year', 'election_code', 'district_type', 'district_name'],
        submission_method='all_purpose_cluster',
        http_path='/sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya',
        tags=['intermediate', 'model_prediction', 'voter_turnout'],
    )
}}}}
{{{{ voter_turnout_lgbm_inference(
    state_code='{state_code}',
    l2_ref='stg_dbt_source__l2_s3_{state_lower}_uniform',
) }}}}
"""

os.makedirs(OUT_DIR, exist_ok=True)

for state in STATES:
    filename = f"int__voter_turnout_lgbm_inference_{state.lower()}.py"
    path = os.path.join(OUT_DIR, filename)
    content = TEMPLATE.format(state_code=state, state_lower=state.lower())
    with open(path, "w") as f:
        f.write(content)

print(f"Generated {len(STATES)} model files in {os.path.abspath(OUT_DIR)}")
