{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['state', 'election_year', 'election_code', 'district_type', 'district_name'],
        submission_method='all_purpose_cluster',
        http_path='/sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya',
        tags=['intermediate', 'model_prediction', 'voter_turnout'],
    )
}}
{{ voter_turnout_lgbm_inference(
    state_code='MS',
    l2_ref='stg_dbt_source__l2_s3_ms_uniform',
) }}
