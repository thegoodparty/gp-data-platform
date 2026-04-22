-- Civics mart elected_official_terms table
-- BR elected official terms enriched with TS contact data via deterministic join.
-- BR is the spine; TS enrichment is additive columns only (row count = BR).
--
-- Two categories of TS enrichment:
-- 1. Person-level (phone, email): fanned out via br_candidate_id
-- 2. Term-scoped (ts_officeholder_id, is_incumbent): 1:1 on br_office_holder_id
--
-- Grain: One row per BR elected official term (person-position-term).
with
    br as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    ts as (select * from {{ ref("int__civics_elected_official_techspeed") }}),

    -- TS step 1: one row per ts_officeholder_id with bigint cast for join.
    -- Known limitation: 86 IDs (0.3%) are reused across different people in TS.
    -- The dedup picks one arbitrarily; does not affect the deterministic ID match.
    ts_position_dedup as (
        select
            cast(ts_officeholder_id as bigint) as ts_officeholder_id_bigint,
            ts_officeholder_id,
            is_incumbent,
            phone as ts_phone,
            email as ts_email,
            updated_at as ts_updated_at
        from ts
        qualify
            row_number() over (partition by ts_officeholder_id order by updated_at desc)
            = 1
    ),

    -- TS step 2: match each TS record to BR to get br_candidate_id.
    -- All 30,375 unique TS IDs match exactly one BR row.
    ts_matched as (
        select
            td.ts_officeholder_id,
            td.ts_phone,
            td.ts_email,
            td.ts_updated_at,
            br.br_candidate_id
        from ts_position_dedup td
        inner join br on br.br_office_holder_id = td.ts_officeholder_id_bigint
    ),

    -- TS step 3: latest non-null phone/email per person (br_candidate_id).
    -- Avoids losing valid contact data across 2,475 multi-TS-ID candidates.
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
        where br_candidate_id is not null
    ),

    -- Flag ts_officeholder_ids with conflicting is_incumbent values (26 of 86
    -- reused IDs). is_incumbent will be NULLed for these in the final select.
    ts_incumbent_conflicts as (
        select ts_officeholder_id
        from ts
        group by ts_officeholder_id
        having count(distinct is_incumbent) > 1
    ),

    -- Assemble term rows: BR spine with two TS LEFT JOINs.
    merged as (
        select
            br.*,
            -- Person-level TS enrichment
            tc.ts_phone,
            tc.ts_email,
            -- Term-scoped TS match
            td.ts_officeholder_id,
            case
                when td.ts_officeholder_id is not null and ic.ts_officeholder_id is null
                then td.is_incumbent
            end as ts_is_incumbent,
            -- Match flags
            td.ts_officeholder_id is not null as has_direct_ts_term_match
        from br
        left join ts_contact_rollup tc on br.br_candidate_id = tc.br_candidate_id
        left join
            ts_position_dedup td
            on br.br_office_holder_id = td.ts_officeholder_id_bigint
        left join
            ts_incumbent_conflicts ic on td.ts_officeholder_id = ic.ts_officeholder_id
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

    -- Contact (mixed precedence)
    coalesce(merged.ts_phone, merged.phone) as phone,  -- TS wins
    coalesce(merged.email, merged.ts_email) as email,  -- BR wins
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
    merged.ts_is_incumbent as is_incumbent,

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
    merged.tier,
    merged.candidate_id_source,

    -- source_systems: output-based — TS included only when a TS value survived
    -- into a final output column (contact or direct-match fields)
    array_compact(
        array(
            'ballotready',
            case
                when
                    (
                        merged.ts_phone is not null
                        and (merged.phone is null or merged.ts_phone != merged.phone)
                    )
                    or (merged.email is null and merged.ts_email is not null)
                    or merged.ts_officeholder_id is not null
                then 'techspeed'
            end
        )
    ) as source_systems,

    -- has_ts_person_enrichment: output-based — true when at least one contact
    -- field's final value came from TS (not just join presence).
    -- Phone: TS wins coalesce, so TS enriched when ts_phone is non-null AND differs
    -- from BR phone.
    -- Email: BR wins coalesce, so TS only contributes when BR email is null.
    (
        merged.ts_phone is not null
        and (merged.phone is null or merged.ts_phone != merged.phone)
    )
    or (
        merged.email is null and merged.ts_email is not null
    ) as has_ts_person_enrichment,

    merged.has_direct_ts_term_match,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                merged.term_start_date is null
                or merged.term_start_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                merged.term_start_date is null
                or merged.term_start_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_win_supersize
    end as is_win_supersize_icp,
    merged.created_at,
    merged.updated_at

from merged
left join
    {{ ref("int__icp_offices") }} as icp
    on merged.br_position_id = icp.br_database_position_id
