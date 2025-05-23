-- TODO: add config
-- TODO: set schema based on DBT_CLOUD_ACCOUNT_ID; not currently working as in docs
-- see: https://docs.getdbt.com/docs/build/environment-variables
{% if env_var("DBT_ENVIRONMENT") == "prod" %} {% set source_schema_name = "dbt" %}
{% else %} {% set source_schema_name = "dbt_hugh" %}
{% endif %}

{% set table_prefix = "stg_dbt_source__l2_s3_" %}
{% set table_suffix = "_vote_history" %}

{% set results = run_query(
    get_l2_table_names(source_schema_name, table_prefix, table_suffix)
) %}

{% set table_names = [] %}
{% for result in results %}
    {% do table_names.append(result.get("table_name")) %}
{% endfor %}

/*
select
{% for table_name in table_names %}
    '{{ table_name }}' {% if not loop.last %}, {% endif %}
{% endfor %}
*/
-- TODO: see notes in scratch_notepad
-- TODO: get all unique columns
-- TODO: loop over all columns and tables to create a union of all tables with all
-- columns
select 'a', 'b'
