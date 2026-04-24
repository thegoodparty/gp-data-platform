-- Civics mart elected_official_terms table
-- Consumes the BR+TS intermediate and adds: contact rollup, provider
-- precedence, incumbent conflict resolution, ICP flags, source attribution.
--
-- Two categories of TS enrichment:
-- 1. Person-level (phone, email): fanned out via br_candidate_id
-- 2. Term-scoped (ts_officeholder_id, is_incumbent): 1:1 on br_office_holder_id
--
-- Grain: One row per BR elected official term (person-position-term).
with
    enriched as (
        select * from {{ ref("int__civics_elected_official_ballotready_techspeed") }}
    ),

    -- TS records matched to BR, with br_candidate_id for person-level fan-out.
    ts_matched as (
        select ts_officeholder_id, ts_phone, ts_email, ts_updated_at, br_candidate_id
        from enriched
        where has_direct_ts_term_match and br_candidate_id is not null
    ),

    -- Latest non-null phone/email per person (br_candidate_id).
    -- Avoids losing valid contact data across multi-TS-ID candidates.
    -- Window function with IGNORE NULLS for deterministic per-field selection.
    -- Secondary sort on ts_officeholder_id breaks ties when timestamps match.
    ts_contact_rollup as (
        select distinct
            br_candidate_id,
            first_value(nullif(ts_phone, '')) ignore nulls over (
                partition by br_candidate_id
                order by ts_updated_at desc, ts_officeholder_id desc
                rows between unbounded preceding and unbounded following
            ) as ts_phone,
            first_value(nullif(ts_email, '')) ignore nulls over (
                partition by br_candidate_id
                order by ts_updated_at desc, ts_officeholder_id desc
                rows between unbounded preceding and unbounded following
            ) as ts_email
        from ts_matched
    ),

    -- Assemble term rows: enriched spine with person-level TS contact rollup.
    merged as (
        select enriched.*, tc.ts_phone as tc_ts_phone, tc.ts_email as tc_ts_email
        from enriched
        left join
            ts_contact_rollup as tc on enriched.br_candidate_id = tc.br_candidate_id
    )

select
    -- PK
    merged.gp_elected_official_id as gp_elected_official_term_id,

    -- Person FK (NULL for vacancies; CASE guard because macro coalesces nulls)
    case
        when merged.br_candidate_id is not null
        then
            {{
                generate_salted_uuid(
                    fields=[
                        "'elected_official'",
                        "cast(merged.br_candidate_id as string)",
                    ]
                )
            }}
    end as gp_elected_official_id,

    -- Source IDs
    merged.br_office_holder_id,
    merged.br_candidate_id,
    merged.br_position_id,
    merged.br_candidacy_id,
    merged.br_geo_id,
    merged.ts_officeholder_id,

    -- Name (BR)
    merged.first_name,
    merged.last_name,
    merged.middle_name,
    merged.suffix,
    merged.full_name,

    -- Contact (mixed precedence: TS wins phone, BR wins email)
    coalesce(merged.tc_ts_phone, merged.phone) as phone,
    coalesce(merged.email, merged.tc_ts_email) as email,
    merged.office_phone,
    merged.central_phone,

    -- Position/Office (BR)
    merged.position_name,
    merged.normalized_position_name,
    merged.candidate_office,
    merged.office_level,
    merged.office_type,

    -- Geography (BR)
    merged.state,
    merged.city,
    merged.district,

    -- Term (BR)
    merged.term_start_date,
    merged.term_end_date,

    -- Flags
    merged.is_appointed,
    merged.is_judicial,
    merged.is_vacant,
    merged.is_off_cycle,
    -- is_incumbent: NULL when ts_incumbent_conflict is true (unreliable value)
    case
        when merged.has_direct_ts_term_match and not merged.ts_incumbent_conflict
        then merged.ts_is_incumbent
    end as is_incumbent,

    -- Party (BR)
    merged.party_affiliation,

    -- Social (BR)
    merged.website_url,
    merged.linkedin_url,
    merged.facebook_url,
    merged.twitter_url,

    -- Mailing address (BR)
    merged.mailing_address_line_1,
    merged.mailing_address_line_2,
    merged.mailing_city,
    merged.mailing_state,
    merged.mailing_zip,

    -- Metadata
    merged.br_position_tier as tier,
    merged.candidate_id_source,

    -- source_systems: output-based — TS included only when a TS value survived
    -- into a final output column (contact or direct-match fields)
    array_compact(
        array(
            'ballotready',
            case
                when
                    (
                        merged.tc_ts_phone is not null
                        and (merged.phone is null or merged.tc_ts_phone != merged.phone)
                    )
                    or (merged.email is null and merged.tc_ts_email is not null)
                    or merged.ts_officeholder_id is not null
                then 'techspeed'
            end
        )
    ) as source_systems,

    -- has_ts_person_enrichment: output-based — true when at least one contact
    -- field's final value came from TS
    (
        merged.tc_ts_phone is not null
        and (merged.phone is null or merged.tc_ts_phone != merged.phone)
    )
    or (
        merged.email is null and merged.tc_ts_email is not null
    ) as has_ts_person_enrichment,

    merged.has_direct_ts_term_match,

    -- ICP flags (computed upstream in int__civics_elected_official_ballotready;
    -- effective-date gate already applied there)
    merged.is_win_icp,
    merged.is_serve_icp,
    merged.is_win_supersize_icp,

    merged.created_at,
    merged.updated_at

from merged
