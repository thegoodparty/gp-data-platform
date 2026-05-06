-- Civics mart elected_officials table
-- Person-grain merge of BR + TS + gp-api elected officials, full-outer-joined
-- on the canonical gp_elected_official_id with provider precedence (BR > TS > gp_api)
-- per column.
--
-- Grain: One row per elected official (person, by br_candidate_id) for
-- BR-matched rows; gp_api-only rows (Splink found no BR/TS match) have
-- their own salted UUID with NULL br_candidate_id.
--
-- gp_api adoption mechanism: gp_api person rollup adopts BR's
-- gp_elected_official_id via the bridge for matched users; falls back to
-- self-key for unmatched. The cluster-based merge_key approach used in
-- candidacy_stage.sql does NOT apply here — EO cluster grain is term and
-- the mart grain is person, and a Splink cluster can contain multiple BR
-- persons (ER false positives via gp_api triangulation). Canonical
-- adoption via bridge attaches gp_api to ONE specific BR person (the
-- bridge-resolved one), avoiding gp_api duplication across BR people in
-- the same cluster.
--
-- ICP flags are NOT exposed at person grain (per Hugh's commit 8c22079).
-- Term-grain ICP remains on elected_official_terms.
{# Shared columns where all three providers contribute; precedence BR > TS > gp_api.
   party_affiliation is in the chain too but written explicitly below because
   the gp_api person intermediate exposes it as `gp_api_party_affiliation`
   (we keep the raw value as a separate audit column). #}
{%- set br_wins_with_gp_fallback_cols = [
    "first_name",
    "last_name",
    "full_name",
    "candidate_office",
    "office_level",
    "office_type",
    "state",
    "created_at",
    "updated_at",
] -%}

{# BR/TS only (no gp_api source for these); BR wins #}
{%- set br_wins_brts_cols = [
    "email",
    "city",
    "district",
    "is_judicial",
    "mailing_address_line_1",
    "mailing_city",
    "mailing_state",
    "mailing_zip",
    "tier",
] -%}

{# TS-wins phone with BR fallback (validated DATA-1731) #}
{%- set ts_wins_cols = ["phone"] -%}

with
    br as (select * from {{ ref("int__civics_elected_official_ballotready_person") }}),
    ts as (select * from {{ ref("int__civics_elected_official_techspeed_person") }}),
    gp as (select * from {{ ref("int__civics_elected_official_gp_api_person") }}),

    merged as (
        select
            -- All three share gp_elected_official_id when matched (BR/TS via
            -- deterministic crosswalk, gp_api via bridge); FOJ collapses correctly
            coalesce(
                br.gp_elected_official_id,
                ts.gp_elected_official_id,
                gp.gp_elected_official_id
            ) as gp_elected_official_id,
            coalesce(br.br_candidate_id, ts.br_candidate_id) as br_candidate_id,

            -- Shared columns with gp_api fallback (covers gp_api-only mart rows)
            {% for col in br_wins_with_gp_fallback_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}, gp.{{ col }}) as {{ col }},
            {% endfor %}

            -- party_affiliation explicit because gp_api side is named
            -- gp_api_party_affiliation
            coalesce(
                br.party_affiliation, ts.party_affiliation, gp.gp_api_party_affiliation
            ) as party_affiliation,

            -- BR/TS-only (no gp_api source for these)
            {% for col in br_wins_brts_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}

            -- TS-wins phone
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}

            -- BR-only (TS doesn't carry these)
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

            -- gp_api-only (separate audit/contact columns; not in precedence chain)
            gp.gp_api_user_id,
            gp.selected_gp_api_elected_office_id,
            gp.selected_gp_api_campaign_id,
            gp.selected_gp_api_organization_slug,
            gp.email as gp_api_email,
            gp.phone as gp_api_phone,
            gp.gp_api_party_affiliation,
            gp.hubspot_contact_id,

            -- source_systems: join-based per Dan's preference + candidacy convention
            array_compact(
                array(
                    case
                        when br.gp_elected_official_id is not null then 'ballotready'
                    end,
                    case
                        when ts.gp_elected_official_id is not null then 'techspeed'
                    end,
                    case when gp.gp_elected_official_id is not null then 'gp_api' end
                )
            ) as source_systems

        from br
        full outer join ts on br.gp_elected_official_id = ts.gp_elected_official_id
        full outer join
            gp
            on coalesce(br.gp_elected_official_id, ts.gp_elected_official_id)
            = gp.gp_elected_official_id
    )

select *
from merged
