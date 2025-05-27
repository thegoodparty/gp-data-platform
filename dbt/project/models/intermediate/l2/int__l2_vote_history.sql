{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        auto_liquid_cluster=true,
        tags=["intermediate", "l2", "vote_history", "all_states"],
    )
}}

-- Note that `depends_on` is required in config since ref() is called inside for loops
-- depending on SQL execution
-- see https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies
/*
depends_on: [
            {{ ref("stg_dbt_source__l2_s3_ak_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_al_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ar_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_az_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ca_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_co_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ct_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_dc_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_de_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_fl_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ga_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_hi_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ia_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_id_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_il_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_in_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ks_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ky_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_la_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ma_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_md_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_me_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_mi_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_mn_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_mo_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ms_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_mt_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nc_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nd_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ne_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nh_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nj_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nm_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_nv_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ny_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_oh_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ok_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_or_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_pa_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ri_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_sc_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_sd_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_tn_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_tx_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_ut_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_va_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_vt_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_wa_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_wi_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_wv_vote_history") }},
            {{ ref("stg_dbt_source__l2_s3_wy_vote_history") }},
        ]
*/
-- TODO: set schema based on DBT_CLOUD_ACCOUNT_ID; not currently working as in docs
-- see: https://docs.getdbt.com/docs/build/environment-variables
{% if env_var("DBT_ENVIRONMENT") == "prod" %} {% set source_schema_name = "dbt" %}
{% else %} {% set source_schema_name = "dbt_hugh" %}
{% endif %}

-- set table name pattern
{% set table_prefix = "stg_dbt_source__l2_s3_" %}
{% set table_suffix = "_vote_history" %}

-- get table names
{% set table_names = get_l2_table_names(
    source_schema_name, table_prefix, table_suffix
) %}

-- get unique column names between all columns
{% set all_columns = get_all_l2_columns(source_schema_name, table_names) %}

select
    state_usps,
    {% for col in all_columns -%}
        {{ col }} {%- if not loop.last -%}, {% endif %}
    {% endfor %}
/*
    `all_columns` and `table_names` depend on SQL execution, skip during parsing phase
    see: https://docs.getdbt.com/reference/dbt-jinja-functions/execute
*/
{%- if execute -%}
    from
        (
            /*
            (1) loop over all state tables
            (2) use NULL for columns that don't exist in the table
            (3) insert state_usps from regexp'd table name
            (4) insert loaded_at from incremental logic for the state subset of data
            */
            {% for table_name in table_names -%}
                {%- set cols_in_table = dbt_utils.get_filtered_columns_in_relation(
                    from=ref(table_name)
                ) -%}
                select
                    regexp_extract(
                        "{{ table_name }}",
                        'stg_dbt_source__l2_s3_([a-z]{2})_vote_history',
                        1
                    ) as state_usps,
                    {% for col in all_columns -%}
                        {% if col in cols_in_table %}
                            {{ col }} {%- if not loop.last -%}, {% endif -%}
                        {% else %}
                            null as {{ col }} {%- if not loop.last -%}, {% endif -%}
                        {% endif %}
                    {%- endfor %}
                from {{ ref(table_name) }}
                {% if is_incremental() %}
                    where
                        loaded_at > (
                            select max(loaded_at)
                            from {{ this }}
                            where
                                state_usps = regexp_extract(
                                    "{{ table_name }}",
                                    'stg_dbt_source__l2_s3_([a-z]{2})_vote_history',
                                    1
                                )
                        )
                {% endif %}
                {%- if not loop.last %}
                    union all
                {% endif %}
            {%- endfor %}
        )
{%- endif -%}
