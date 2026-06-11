{{ config(materialized="table", tags=["civics", "serve"]) }}

-- One row per serve org (organizations mart, organization_type = 'serve'),
-- resolving each org to the L2 district its official serves. This is the
-- cohort->district leg of the Constituents Served pipeline (DATA-1988, epic
-- DATA-1359): downstream models join the resolved (state, l2_district_type,
-- normalized_district_name) tuple to L2-derived tables to count the people
-- who live in each official's jurisdiction.
--
-- Resolution is override-first: the org's own election-api district record
-- (coalesce(override_district_id, district_id), which bakes in the manual
-- override corrections made through the product) wins; the LLM
-- position->district crosswalk is the fallback for orgs with a BallotReady
-- position but no district row. Orgs resolving through neither path carry
-- resolution_path = 'unresolved' (mostly custom free-text positions - a
-- product fix, not a pipeline fix).
--
-- Rows are never excluded here. Pending product decisions (ever-serve vs
-- currently-active cohort, internal-email orgs in or out) surface as flag
-- columns plus var-driven filters so a decision is a parameter flip, not a
-- rewrite. requires_review marks deterministic container-mismatch rules (a
-- local office resolved to a too-large containing district); is_geo_seat
-- marks the jurisdiction-vs-electoral-seat semantics question. Flagged rows
-- flow through; downstream consumers decide what to do with them.
with
    serve_orgs as (
        -- the cohort predicate lives here and only here: "ever-serve" by
        -- default. A future ever-vs-active decision lands as one more
        -- predicate in this CTE.
        select * from {{ ref("organizations") }} where organization_type = 'serve'
    ),

    districts as (select * from {{ ref("m_election_api__district") }}),

    crosswalk as (
        select *
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
        where is_matched
    ),

    resolved as (
        select
            o.organization_slug,
            o.user_id,
            o.elected_office_id,
            o.ballotready_position_id,
            o.position_name,
            o.custom_position_name,
            coalesce(o.override_district_id, o.district_id) as district_id,
            case
                when d.id is not null
                then 'district_mart'
                when x.br_database_id is not null
                then 'crosswalk'
                else 'unresolved'
            end as resolution_path,
            -- override-only orgs have no position, hence a null
            -- position_state: the district row is the authoritative state
            -- source on both paths
            coalesce(d.state, x.state) as state,
            coalesce(d.l2_district_type, x.l2_district_type) as l2_district_type,
            coalesce(d.l2_district_name, x.l2_district_name) as l2_district_name,
            -- combined searchable office title for the deterministic flag
            -- rules below (position titles and custom titles both carry
            -- scope markers)
            lower(
                concat_ws(
                    ' ',
                    coalesce(o.position_name, ''),
                    coalesce(o.custom_position_name, '')
                )
            ) as title_text,
            o.user_email
        from serve_orgs o
        left join districts d on coalesce(o.override_district_id, o.district_id) = d.id
        left join crosswalk x on o.ballotready_position_id = x.br_database_id
    ),

    final as (
        select
            organization_slug,
            user_id,
            elected_office_id,
            ballotready_position_id,
            position_name,
            custom_position_name,
            district_id,
            resolution_path,
            state,
            l2_district_type,
            l2_district_name,
            {{ normalize_l2_district_name("l2_district_name") }}
            as normalized_district_name,

            -- 'State' is a synthetic L2 district type for statewide offices
            -- (every other type, including 'Other', is a real L2 column).
            -- Statewide orgs are reported separately downstream, never mixed
            -- into the local headline.
            coalesce(l2_district_type = 'State', false) as is_statewide,

            -- requires_review rules: deterministic, no discretion. Each rule
            -- targets the container-fallback failure mode (an office matched
            -- to a district that contains, but is larger than, its real
            -- jurisdiction), which validation found to be the dominant
            -- precision risk on both resolution paths.
            -- (a) matched to the synthetic statewide district but the title
            -- is not a statewide office
            coalesce(
                l2_district_type = 'State'
                and not title_text
                rlike 'u\\.?s\\.? senate|united states senate|governor|secretary of state|attorney general|treasurer|auditor|comptroller|superintendent of public instruction',
                false
            ) as review_rule_statewide_container,

            -- (b) matched to a whole county but nothing in the title says
            -- county-level office. Virginia-style independent cities are
            -- county-equivalents that L2 stores in the County column with a
            -- ' CITY' name suffix; a city office matched to one of those is
            -- correct, so they are excluded.
            coalesce(
                l2_district_type = 'County'
                and not title_text rlike 'county|borough|parish'
                and not normalized_district_name like '% CITY',
                false
            ) as review_rule_county_container,

            -- (c) title names a sub-jurisdiction (a subarea seat or a
            -- community education council, whose office governs less than
            -- the matched district) while the match is a whole county,
            -- city, or school district
            coalesce(
                title_text rlike 'subarea|community education council'
                and l2_district_type
                in ('County', 'City', 'School_District', 'Unified_School_District'),
                false
            ) as review_rule_subscope_marker,

            review_rule_statewide_container
            or review_rule_county_container
            or review_rule_subscope_marker as requires_review,

            nullif(
                concat_ws(
                    '; ',
                    case
                        when review_rule_statewide_container
                        then 'non-statewide office matched to the statewide district'
                    end,
                    case
                        when review_rule_county_container
                        then
                            'matched to a whole county but title has no county/borough/parish term'
                    end,
                    case
                        when review_rule_subscope_marker
                        then
                            'title names a sub-jurisdiction but matched to a whole district'
                    end
                ),
                ''
            ) as review_reason,

            -- the title carries a geographic seat designation (Ward 3,
            -- District F, Zone 1) while the matched district is the whole
            -- jurisdiction: under "whole governed jurisdiction" semantics
            -- (the v1 default) these are correct; under electoral-district
            -- semantics they overcount. Flag only - the semantics decision
            -- is pending. Seat/place/position numbering is at-large, not
            -- geographic, and is deliberately not matched.
            coalesce(
                title_text
                rlike '(ward|district|zone|area|division|subdistrict|precinct|region)[ .#-]*([0-9]+|[a-h])\\b'
                and not (
                    l2_district_type
                    rlike 'Ward|Subdistrict|SubDistrict|Commissioner|Supervisorial|Legislative|Congressional|State_House|State_Senate|Precinct|School_Board|Board_of_Education_District|Council_District|Judicial'
                    or l2_district_type in ('State', 'Other')
                ),
                false
            ) as is_geo_seat,

            -- internal/test accounts; whether they count toward the public
            -- metric is a pending product decision, so flag, never exclude
            coalesce(user_email ilike '%goodparty%', false) as is_internal_email
        from resolved
    )

select *
from final
{% if var("serve_cohort_exclude_internal_email_orgs", false) %}
    where not is_internal_email
{% endif %}
