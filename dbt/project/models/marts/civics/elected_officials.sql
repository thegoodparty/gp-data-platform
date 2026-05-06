{{ config(materialized="table") }}

-- Civics mart elected_officials table
-- Person-grain merge of BR + TS elected officials, full-outer-joined on
-- the canonical gp_elected_official_id with COALESCE per column.
-- Mirrors the candidate.sql / candidacy.sql merge pattern.
--
-- Grain: One row per elected official (person, by br_candidate_id).
-- Today's row count: ~375,321 (BR person count; TS-only is 0).
--
-- TS contributions are filtered upstream:
-- * Reused-only canonicals (53) excluded from TS person rollup
-- * Conflicting is_incumbent values NULL'd at canonical grain
-- * Per-field FIRST_VALUE rollup preserves alternate contacts
-- ...so this mart's COALESCE ingests clean TS data.
--
-- ICP flags are NOT exposed at person grain (per Hugh's commit 8c22079
-- which removed them from elected_officials). Position-scoped semantics
-- mean a person isn't ICP — an office is. Term-grain ICP remains on
-- elected_official_terms for consumers who need it.
{% set br_wins_cols = [
    "first_name",
    "last_name",
    "full_name",
    "email",
    "candidate_office",
    "office_level",
    "office_type",
    "state",
    "city",
    "district",
    "party_affiliation",
    "is_judicial",
    "mailing_address_line_1",
    "mailing_city",
    "mailing_state",
    "mailing_zip",
    "tier",
    "created_at",
    "updated_at",
] -%}

{# TS EO source doesn't have facebook_url, website_url, linkedin_url,
   twitter_url, middle_name, or suffix — those stay BR-only. Only phone
   has both sides, and TS wins per the candidacy precedent. #}
{%- set ts_wins_cols = ["phone"] -%}

with
    br as (select * from {{ ref("int__civics_elected_official_ballotready_person") }}),

    ts as (select * from {{ ref("int__civics_elected_official_techspeed_person") }}),

    merged as (
        select
            -- PK + natural key (same value on both sides where both present)
            coalesce(
                br.gp_elected_official_id, ts.gp_elected_official_id
            ) as gp_elected_official_id,
            coalesce(br.br_candidate_id, ts.br_candidate_id) as br_candidate_id,

            -- Shared BR-wins
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}

            -- Shared TS-wins
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}

            -- BR-only
            br.middle_name,
            br.suffix,
            br.selected_gp_elected_official_term_id,
            br.selected_br_office_holder_id,
            br.office_phone,
            br.central_phone,
            br.term_start_date,
            br.term_end_date,
            br.website_url,
            br.linkedin_url,
            br.facebook_url,
            br.twitter_url,
            br.mailing_address_line_2,

            -- TS-only
            ts.selected_ts_officeholder_id,
            ts.selected_ts_position_id,
            ts.is_incumbent,
            coalesce(ts.ts_incumbent_conflict, false) as ts_incumbent_conflict,

            -- Source presence (join-based)
            array_compact(
                array(
                    case
                        when br.gp_elected_official_id is not null then 'ballotready'
                    end,
                    case when ts.gp_elected_official_id is not null then 'techspeed' end
                )
            ) as source_systems

        from br
        full outer join ts on br.gp_elected_official_id = ts.gp_elected_official_id
    )

select *
from merged
