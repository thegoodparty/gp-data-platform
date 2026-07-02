{#-
    Named accessors over the L2 column classification in the
    l2_column_classification seed (its `family` column is the source of truth).
    Models that call these read the seed via run_query, so they must declare:
    -- depends_on: {{ ref("l2_column_classification") }}
    Family map: district = 'district', PII = 'identity_pii',
    partisan = 'partisan' + 'haystaq_partisan'.
-#}
{% macro l2_columns_in_families(
    families, classification_seed="l2_column_classification"
) %}
    {#- L2 column names whose family is in `families` (case-insensitive). -#}
    {%- set columns = [] -%}
    {%- if execute -%}
        {%- set quoted = [] -%}
        {%- for f in families -%}
            {%- set _ = quoted.append("'" ~ (f | lower) ~ "'") -%}
        {%- endfor -%}
        {%- set results = run_query(
            "select column_name from "
            ~ ref(classification_seed)
            ~ " where family is not null and lower(cast(family as string)) in ("
            ~ (quoted | join(", "))
            ~ ") order by column_name"
        ) -%}
        {%- set columns = results.columns[0].values() -%}
    {%- endif -%}
    {{ return(columns) }}
{% endmacro %}


{% macro l2_columns_excluding_families(
    families, classification_seed="l2_column_classification"
) %}
    {#- Complement of l2_columns_in_families: every classified column whose
        family is NOT in `families`. -#}
    {%- set columns = [] -%}
    {%- if execute -%}
        {%- set quoted = [] -%}
        {%- for f in families -%}
            {%- set _ = quoted.append("'" ~ (f | lower) ~ "'") -%}
        {%- endfor -%}
        {%- set results = run_query(
            "select column_name from "
            ~ ref(classification_seed)
            ~ " where family is not null and lower(cast(family as string)) not in ("
            ~ (quoted | join(", "))
            ~ ") order by column_name"
        ) -%}
        {%- set columns = results.columns[0].values() -%}
    {%- endif -%}
    {{ return(columns) }}
{% endmacro %}


{% macro l2_district_columns() %}
    {#- District identifiers (family = 'district'): state, congressional,
        county commission, ward, school district, etc. Returns column names;
        for backticked SELECT/UNPIVOT SQL use get_l2_district_columns. -#}
    {{ return(l2_columns_in_families(["district"])) }}
{% endmacro %}


{% macro l2_pii_columns() %}
    {#- PII / direct identifiers (family = 'identity_pii'): names, street
        addresses, phones, emails, DOB, device IDs, raw LALVOTERID. Zip and
        census geography are not PII and are kept. -#}
    {{ return(l2_columns_in_families(["identity_pii"])) }}
{% endmacro %}


{% macro l2_partisan_columns() %}
    {#- Partisan / political columns (family in 'partisan', 'haystaq_partisan'):
        party registration, partisan models, donor attributes, Haystaq
        candidate/party/ideology/horserace models. Non-partisan issue models
        (family 'haystaq_issue') are not included. -#}
    {{ return(l2_columns_in_families(["partisan", "haystaq_partisan"])) }}
{% endmacro %}


{% macro l2_serve_available_columns(classification_seed="l2_column_classification") %}
    {#- The serve allowlist: columns flagged is_available in the seed. This is a
        curated set, not a pure family filter (e.g. a religion demographic is
        held back), so serve_agent_voters keys off this flag rather than family. -#}
    {%- set columns = [] -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select column_name from "
            ~ ref(classification_seed)
            ~ " where lower(cast(is_available as string)) = 'true' order by column_name"
        ) -%}
        {%- set columns = results.columns[0].values() -%}
    {%- endif -%}
    {{ return(columns) }}
{% endmacro %}
