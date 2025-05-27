-- TODO: may need to add "if execute" block to prevent errors during parsing
-- TODO: add config
{{
    config(
        materialized="table",
        auto_liquid_cluster=true,
        tags=["intermediate", "l2", "vote_history"],
    )
}}
-- TODO: set schema based on DBT_CLOUD_ACCOUNT_ID; not currently working as in docs
-- see: https://docs.getdbt.com/docs/build/environment-variables
{% if env_var("DBT_ENVIRONMENT") == "prod" %} {% set source_schema_name = "dbt" %}
{% else %} {% set source_schema_name = "dbt_hugh" %}
{% endif %}

{% set table_prefix = "stg_dbt_source__l2_s3_" %}
{% set table_suffix = "_vote_history" %}

-- get table names
{% set table_names = get_l2_table_names(
    source_schema_name, table_prefix, table_suffix
) %}

-- get unique column names between all columns
{% set all_columns = get_all_l2_columns(source_schema_name, table_names) %}


-- TODO: see notes in scratch_notepad
-- TODO: loop over all columns and tables to create a union of all tables with all
-- columns
-- TODO: incremental loads on loaded_at by each state
-- select 'a', 'b'
select
    {% for column in all_columns -%}
        '{{ column }}' {% if not loop.last %}, {% endif %}
    {%- endfor %}
