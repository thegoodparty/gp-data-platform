-- Term-grain BR fact table (one row per BR elected-official term,
-- br_office_holder_id) with minimal TS provenance markers.
--
-- TS provenance (ts_officeholder_id, has_direct_ts_term_match,
-- ts_officeholder_id_is_reused) is surfaced via LEFT JOIN to the EO crosswalk so
-- consumers can identify TS-matched terms. NO TS-derived term-grain attributes are
-- exposed here — those are person-grain and live on elected_officials only.
--
-- ICP flags pass through from int__civics_elected_official_ballotready, where Win ICP /
-- Win Supersize ICP are gated by the candidacy's election_day (NULL election_day yields
-- NULL flag); is_serve_icp has no date gate. source_systems is join-based.
--
-- DDHQ rows: a 2026+ DDHQ win of a BR position that BR has no term for
-- (int__civics_elected_official_ddhq_terms), once the win has a person. Keyed on
-- the win's own id, which is also ddhq_term_id. They have no BR ids and no term
-- dates yet: elected_date is the election, and the term starts when BR or the
-- official says so. A BR term that covers a DDHQ win carries that win's
-- ddhq_term_id, so the win's id stays traceable when BR publishes the term.
with
    br_terms as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    -- TS provenance via crosswalk LEFT JOIN. Crosswalk grain (1 row per
    -- ts_officeholder_id with deterministic 1:1 to br_office_holder_id)
    -- means at most 1 TS row per BR term — no fan-out.
    ts_provenance as (
        select br_office_holder_id, ts_officeholder_id, ts_officeholder_id_is_reused
        from {{ ref("int__civics_elected_official_canonical_ids") }}
    ),

    gp_api_bridge as (
        select * from {{ ref("int__civics_elected_official_gp_api_bridge") }}
    ),

    -- DDHQ general-winner votes matched to this BR term via the matcha cluster
    -- (BR-keyed rows only; 1 per br_office_holder_id, so no fan-out). Attached to
    -- every official, not just gp-api ones.
    ddhq_votes as (
        select br_office_holder_id, ddhq_winning_votes
        from {{ ref("int__civics_elected_official_ddhq_matched_votes") }}
        where br_office_holder_id is not null
    ),

    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    ddhq_wins as (select * from {{ ref("int__civics_elected_official_ddhq_terms") }}),

    -- The DDHQ win a BR term covers, if any (one per BR term: the earliest win).
    ddhq_covered as (
        select br_office_holder_id, ddhq_term_id, elected_date
        from ddhq_wins
        where br_office_holder_id is not null
        qualify
            row_number() over (
                partition by br_office_holder_id order by elected_date, ddhq_term_id
            )
            = 1
    ),

    br_rows as (
        select
            -- PK (term-grain canonical UUID, from BR intermediate)
            br.gp_elected_official_term_id,

            -- Person FK (from BR intermediate; NULL for vacancies)
            br.gp_elected_official_id,

            -- Canonical person. min over the person ids reached by br_candidate_id
            -- and the bridged gp_api_user_id (one person by construction).
            least(bp.gp_person_id, gpp.gp_person_id) as gp_person_id,

            -- Source IDs
            br.br_office_holder_id,
            br.br_candidate_id,
            br.br_position_id,
            br.br_candidacy_id,
            br.br_geo_id,

            -- TS provenance (NULL when no direct TS match)
            ts.ts_officeholder_id,
            ts.ts_officeholder_id is not null as has_direct_ts_term_match,
            coalesce(
                ts.ts_officeholder_id_is_reused, false
            ) as ts_officeholder_id_is_reused,

            -- Name (BR term)
            br.first_name,
            br.last_name,
            br.middle_name,
            br.suffix,
            br.full_name,

            -- Contact (BR-only at term grain; TS contact lives on elected_officials)
            br.email,
            br.phone,
            br.office_phone,
            br.central_phone,

            -- Position/Office (BR term)
            br.position_name,
            br.normalized_position_name,
            br.candidate_office,
            br.office_level,
            br.office_type,

            -- Geography (BR term)
            br.state,
            br.city,
            br.district,

            -- Term dates
            br.term_start_date,
            br.term_end_date,

            -- Term-scoped flags (BR)
            br.is_appointed,
            br.is_judicial,
            br.is_vacant,
            br.is_off_cycle,

            -- Party (BR term)
            br.party_affiliation,

            -- Social (BR term)
            br.website_url,
            br.linkedin_url,
            br.facebook_url,
            br.twitter_url,
            br.urls,

            -- Mailing (BR term)
            br.mailing_address_line_1,
            br.mailing_address_line_2,
            br.mailing_city,
            br.mailing_state,
            br.mailing_zip,

            -- Metadata
            br.br_position_tier as tier,
            br.candidate_id_source,

            -- ICP (passed through from BR intermediate)
            br.is_win_icp,
            br.is_serve_icp,
            br.is_win_supersize_icp,

            -- gp_api term attachment (bridge LEFT JOIN; 1:1 by br_office_holder_id)
            gp.gp_api_elected_office_id,
            gp.gp_api_user_id,
            gp.gp_api_campaign_id,
            gp.gp_api_organization_slug,
            gp.hubspot_company_id,
            gp.days_to_sworn_in,

            -- DDHQ general-winner votes for this office (the official's own votes,
            -- resolved via the matcha cluster). NULL when the office did not match a
            -- DDHQ winner. Feeds the election_api support score.
            dv.ddhq_winning_votes,

            -- source_systems: join-based per the candidacy mart convention. BR is
            -- always
            -- present (this mart is BR-spine); TS is added when the term has a
            -- direct ts_officeholder_id match; gp_api when the bridge attached a
            -- record.
            array_compact(
                array(
                    'ballotready',
                    case when ts.ts_officeholder_id is not null then 'techspeed' end,
                    case when gp.gp_api_elected_office_id is not null then 'gp_api' end
                )
            ) as source_systems,

            br.created_at,
            br.updated_at,
            dc.ddhq_term_id,
            dc.elected_date

        from br_terms as br
        left join ts_provenance as ts on br.br_office_holder_id = ts.br_office_holder_id
        left join gp_api_bridge as gp on br.br_office_holder_id = gp.br_office_holder_id
        left join ddhq_votes as dv on br.br_office_holder_id = dv.br_office_holder_id
        left join
            person_ids as bp
            on bp.record_key = 'ballotready|' || cast(br.br_candidate_id as string)
        left join
            person_ids as gpp
            on gpp.record_key = 'gp_api|' || cast(gp.gp_api_user_id as string)
        left join ddhq_covered as dc on br.br_office_holder_id = dc.br_office_holder_id
    ),

    -- The official a DDHQ win attaches to: the person's existing row, or the
    -- DDHQ row elected_officials made for them.
    officials as (
        select gp_person_id, min(gp_elected_official_id) as gp_elected_official_id
        from {{ ref("elected_officials") }}
        where gp_person_id is not null
        group by gp_person_id
    ),

    ddhq_rows as (
        select
            d.ddhq_term_id as gp_elected_official_term_id,
            o.gp_elected_official_id,
            d.gp_person_id,
            cast(null as int) as br_office_holder_id,
            cast(null as int) as br_candidate_id,
            cast(d.br_position_id as int) as br_position_id,
            cast(null as int) as br_candidacy_id,
            cast(null as string) as br_geo_id,
            cast(null as string) as ts_officeholder_id,
            false as has_direct_ts_term_match,
            false as ts_officeholder_id_is_reused,
            d.first_name,
            d.last_name,
            cast(null as string) as middle_name,
            cast(null as string) as suffix,
            d.full_name,
            cast(null as string) as email,
            cast(null as string) as phone,
            cast(null as string) as office_phone,
            cast(null as string) as central_phone,
            d.position_name,
            d.normalized_position_name,
            d.candidate_office,
            d.office_level,
            d.office_type,
            d.state,
            d.city,
            d.district,
            cast(null as date) as term_start_date,
            cast(null as date) as term_end_date,
            d.is_appointed,
            d.is_judicial,
            false as is_vacant,
            cast(null as boolean) as is_off_cycle,
            d.party_affiliation,
            cast(null as string) as website_url,
            cast(null as string) as linkedin_url,
            cast(null as string) as facebook_url,
            cast(null as string) as twitter_url,
            cast(null as array<struct<type:string, url:string>>) as urls,
            cast(null as string) as mailing_address_line_1,
            cast(null as string) as mailing_address_line_2,
            cast(null as string) as mailing_city,
            cast(null as string) as mailing_state,
            cast(null as string) as mailing_zip,
            cast(null as int) as tier,
            'ddhq' as candidate_id_source,
            d.is_win_icp,
            d.is_serve_icp,
            d.is_win_supersize_icp,
            cast(null as string) as gp_api_elected_office_id,
            cast(null as bigint) as gp_api_user_id,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as string) as gp_api_organization_slug,
            cast(null as string) as hubspot_company_id,
            cast(null as int) as days_to_sworn_in,
            cast(null as bigint) as ddhq_winning_votes,
            array('ddhq') as source_systems,
            d.created_at,
            d.updated_at,
            d.ddhq_term_id,
            d.elected_date
        from ddhq_wins as d
        inner join officials as o using (gp_person_id)
        where d.br_office_holder_id is null and d.gp_person_id is not null
    )

select *
from br_rows
union all
select *
from ddhq_rows
