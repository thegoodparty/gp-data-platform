-- ddhq_votes arrives with the DDHQ source when the matcha cluster is re-run and
-- the er_source table reloaded. Detect it at run time so this model (and the
-- support score downstream) keeps building in the window before that reload:
-- present -> pass through; absent -> emit null.
{% set src = source("er_source", "clustered_elected_officials") %}
{% set has_ddhq_votes = false %}
{% if execute %}
    {% set src_cols = (
        adapter.get_columns_in_relation(src)
        | map(attribute="name")
        | map("lower")
        | list
    ) %}
    {% set has_ddhq_votes = "ddhq_votes" in src_cols %}
{% endif %}

with

    source as (select * from {{ src }}),

    renamed as (

        select
            cluster_id,
            unique_id,
            source_id,
            source_name,
            first_name,
            last_name,
            first_name_aliases,
            state,
            party,
            candidate_office,
            office_level,
            office_type,
            district_raw,
            cast(district_identifier as int) as district_identifier,
            email,
            phone,
            official_office_name,
            city,
            cast(term_start_date as date) as term_start_date,
            cast(term_end_date as date) as term_end_date,
            cast(ballotready_position_id as bigint) as ballotready_position_id,
            cast(br_office_holder_id as int) as br_office_holder_id,
            cast(br_candidate_id as int) as br_candidate_id,
            ts_officeholder_id,
            ts_position_id,
            cast(gp_api_user_id as bigint) as gp_api_user_id,
            cast(gp_api_campaign_id as bigint) as gp_api_campaign_id,
            gp_api_elected_office_id,
            gp_api_organization_slug,
            {% if has_ddhq_votes %} cast(ddhq_votes as bigint) as ddhq_votes
            {% else %} cast(null as bigint) as ddhq_votes
            {% endif %}

        from source

    )

select *
from renamed
