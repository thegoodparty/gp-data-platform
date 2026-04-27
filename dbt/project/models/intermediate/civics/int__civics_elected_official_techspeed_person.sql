{{ config(materialized="table", tags=["civics"]) }}

-- TS-side person-grain rollup. Adopts BR canonical IDs via the crosswalk,
-- suppresses reused-ID contamination, rolls up phone/email per-field, and
-- detects is_incumbent conflicts at canonical grain.
--
-- Canonicals whose TS contributions are entirely from reused
-- ts_officeholder_ids are EXCLUDED — no clean TS rows means no TS
-- contribution, and the person mart's full-outer-join correctly yields
-- BR-only.
--
-- TS-only fallback is unsolved (zero rows today). The unmatched-TS singular
-- test on the crosswalk fires on any TS-only ID, blocking the build until
-- a fallback canonical-id strategy is designed.
with
    ts_position as (select * from {{ ref("int__civics_elected_official_techspeed") }}),

    crosswalk as (
        select * from {{ ref("int__civics_elected_official_canonical_ids") }}
    ),

    -- Adopt canonical IDs. INNER JOIN — TS-only records drop here (zero today).
    ts_with_canonical as (
        select
            ts.*,
            xw.canonical_gp_elected_official_id,
            xw.canonical_gp_elected_official_term_id,
            xw.br_office_holder_id,
            xw.br_candidate_id,
            xw.ts_officeholder_id_is_reused
        from ts_position as ts
        inner join crosswalk as xw on ts.ts_officeholder_id = xw.ts_officeholder_id
    ),

    -- Suppress reused IDs. Conservative — drops both contamination and legit
    -- same-person multi-position cases. ~86 IDs flagged; ~53 reused-only
    -- canonicals dropped entirely (no clean rows).
    ts_clean as (
        select * from ts_with_canonical where not ts_officeholder_id_is_reused
    ),

    -- Per-field FIRST_VALUE IGNORE NULLS rollup at canonical grain.
    -- Preserves alternate contacts: 107 phones (across 908 multi-phone
    -- canonicals) and 1,097 emails (across 210 multi-email canonicals)
    -- saved vs single-row dedup.
    contact_rollup as (
        select distinct
            canonical_gp_elected_official_id,
            first_value(nullif(phone, '')) ignore nulls over (
                partition by canonical_gp_elected_official_id
                order by
                    updated_at desc nulls last,
                    ts_officeholder_id desc,
                    ts_position_id desc nulls last,
                    gp_elected_official_id desc
                rows between unbounded preceding and unbounded following
            ) as phone,
            first_value(nullif(email, '')) ignore nulls over (
                partition by canonical_gp_elected_official_id
                order by
                    updated_at desc nulls last,
                    ts_officeholder_id desc,
                    ts_position_id desc nulls last,
                    gp_elected_official_id desc
                rows between unbounded preceding and unbounded following
            ) as email
        from ts_clean
    ),

    -- is_incumbent conflict detection at canonical grain. min/max ignore
    -- NULLs; "is distinct from" between them flags real disagreement.
    -- ~350 affected canonicals.
    incumbent_stats as (
        select
            canonical_gp_elected_official_id,
            min(is_incumbent) as min_is_incumbent,
            max(is_incumbent) as max_is_incumbent
        from ts_clean
        group by canonical_gp_elected_official_id
    ),

    -- Pick latest non-reused row per canonical for non-rolled-up attributes.
    latest_per_canonical as (
        select *
        from ts_clean
        qualify
            row_number() over (
                partition by canonical_gp_elected_official_id
                order by
                    updated_at desc nulls last,
                    ts_officeholder_id desc,
                    ts_position_id desc nulls last,
                    gp_elected_official_id desc
            )
            = 1
    )

select
    -- Person-grain canonical UUID (PK)
    lpc.canonical_gp_elected_official_id as gp_elected_official_id,

    -- Person-grain natural key (from BR via crosswalk)
    lpc.br_candidate_id,

    -- Audit / provenance
    lpc.ts_officeholder_id as selected_ts_officeholder_id,
    lpc.ts_position_id as selected_ts_position_id,
    lpc.canonical_gp_elected_official_term_id as selected_gp_elected_official_term_id,
    lpc.br_office_holder_id as selected_br_office_holder_id,

    -- Name (latest non-reused row)
    lpc.first_name,
    lpc.last_name,
    lpc.full_name,

    -- Contact (per-field FIRST_VALUE rollup)
    cr.phone,
    cr.email,

    -- Latest non-reused row's office attributes
    lpc.candidate_office,
    lpc.office_level,
    lpc.office_type,
    lpc.state,
    lpc.city,
    lpc.district,

    -- is_incumbent: NULL when contributing rows conflict, else the consistent value.
    case
        when stats.min_is_incumbent is distinct from stats.max_is_incumbent
        then null
        else stats.max_is_incumbent
    end as is_incumbent,

    -- Conflict flag: surfaced even when is_incumbent is NULL'd.
    coalesce(
        stats.min_is_incumbent is distinct from stats.max_is_incumbent, false
    ) as ts_incumbent_conflict,

    -- Other person-level flags
    lpc.is_judicial,

    -- Party (latest non-reused row)
    lpc.party_affiliation,

    -- Mailing (latest non-reused row)
    lpc.mailing_address_line_1,
    lpc.mailing_city,
    lpc.mailing_state,
    lpc.mailing_zip,

    -- Tier (latest non-reused row)
    lpc.tier,

    -- Timestamps from latest non-reused row
    lpc.created_at,
    lpc.updated_at

from latest_per_canonical as lpc
left join
    contact_rollup as cr
    on lpc.canonical_gp_elected_official_id = cr.canonical_gp_elected_official_id
left join
    incumbent_stats as stats
    on lpc.canonical_gp_elected_official_id = stats.canonical_gp_elected_official_id
