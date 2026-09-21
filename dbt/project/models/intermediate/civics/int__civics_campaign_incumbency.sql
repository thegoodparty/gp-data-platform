-- Office-first incumbency for product campaigns: on election day, does this
-- signup already hold the seat they are running for?
--
-- Exists because candidacy.is_incumbent only covers campaigns that clear the
-- candidacy mart's vendor-corroboration gate, which is 27% of 2026 Win
-- campaigns. Anchoring on the campaign's own ballotready_position_id instead
-- of a candidacy row reaches 88%, and needs no third-party confirmation that
-- the person is on a ballot.
--
-- Semantics match candidacy.is_incumbent exactly, and are three-valued:
-- true  - a covering term on this seat belongs to this person
-- false - a covering term exists but is held by someone else, or is vacant
-- null  - no term covers election day, or the seat does not resolve
with
    campaign_base as (
        select
            c.campaign_id,
            c.election_date,
            c.user_id,
            lower(trim(c.user_first_name)) as first_name,
            lower(trim(c.user_last_name)) as last_name,
            cast(c.ballotready_position_id as bigint) as br_position_id
        from {{ ref("campaigns") }} as c
        where
            c.is_latest_version
            and not coalesce(c.is_demo, false)
            and c.ballotready_position_id is not null
            and c.election_date is not null
    ),

    -- Two seat keys. The campaign's own position id is exact, so it matches
    -- candidacy.is_incumbent and decides on its own wherever it finds a term.
    -- But BR position ids are cycle-specific, so the same seat carries a
    -- different id each cycle and a holder elected in an earlier cycle is
    -- invisible to it. Normalized position plus geo recovers those.
    position_seat as (
        select
            cast(database_id as bigint) as br_position_id,
            cast(
                normalized_position.`databaseId` as bigint
            ) as br_normalized_position_id,
            geo_id,
            lower(trim(name)) as position_name
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
        where database_id is not null
    ),

    campaign_seat as (
        select
            cb.campaign_id,
            cb.election_date,
            cb.user_id,
            cb.first_name,
            cb.last_name,
            cb.br_position_id,
            ps.br_normalized_position_id,
            ps.geo_id,
            ps.position_name
        from campaign_base as cb
        left join position_seat as ps on cb.br_position_id = ps.br_position_id
    ),

    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    -- A null term_start_date would backdate the holder across cycles they may
    -- not have served, so those terms are dropped; a null term_end_date reads
    -- as no scheduled end, still serving.
    term_seat as (
        select
            t.gp_person_id,
            lower(trim(t.first_name)) as first_name,
            lower(trim(t.last_name)) as last_name,
            t.is_vacant,
            t.term_start_date,
            t.term_end_date,
            t.br_position_id,
            ps.br_normalized_position_id,
            ps.geo_id,
            ps.position_name
        from {{ ref("elected_official_terms") }} as t
        left join position_seat as ps on t.br_position_id = ps.br_position_id
        where t.term_start_date is not null
    ),

    -- Exact leg: the seat the campaign declared, matched by its own position id.
    covering_exact as (
        select
            cs.campaign_id,
            count(*) as n_covering_terms,
            -- Vacancy terms keep the prior holder's name, so they must never
            -- satisfy the name arm. They still count as coverage, which is
            -- what makes an empty seat read false rather than null.
            max(
                case
                    when ts.is_vacant
                    then 0
                    when
                        ts.gp_person_id = p.gp_person_id
                        or (
                            ts.first_name = cs.first_name
                            and ts.last_name = cs.last_name
                        )
                    then 1
                    else 0
                end
            )
            = 1 as is_incumbent
        from campaign_seat as cs
        inner join
            term_seat as ts
            on ts.br_position_id = cs.br_position_id
            and ts.term_start_date <= cs.election_date
            and (ts.term_end_date is null or ts.term_end_date >= cs.election_date)
        left join
            person_ids as p on p.record_key = 'gp_api|' || cast(cs.user_id as string)
        group by cs.campaign_id
    ),

    -- Recovery leg: the same seat under a different cycle's position id.
    --
    -- The normalized key spans every seat of that type in the geo, so on a
    -- multi-seat body it also reaches seats the campaign is not running for.
    -- Requiring the position names to match is what keeps this leg answering
    -- "holds the seat they are running for" rather than "holds some seat".
    -- Without that guard it flags 196 campaigns running for a different ward,
    -- post or seat of the same body, to gain 94 genuine cross-cycle holders.
    covering_recovered as (
        select
            cs.campaign_id,
            count(*) as n_covering_terms,
            max(
                case
                    when ts.is_vacant
                    then 0
                    when ts.position_name is null or cs.position_name is null
                    then 0
                    when ts.position_name != cs.position_name
                    then 0
                    when
                        ts.gp_person_id = p.gp_person_id
                        or (
                            ts.first_name = cs.first_name
                            and ts.last_name = cs.last_name
                        )
                    then 1
                    else 0
                end
            )
            = 1 as is_incumbent
        from campaign_seat as cs
        inner join
            term_seat as ts
            on ts.br_normalized_position_id = cs.br_normalized_position_id
            and ts.geo_id = cs.geo_id
            and ts.term_start_date <= cs.election_date
            and (ts.term_end_date is null or ts.term_end_date >= cs.election_date)
        left join
            person_ids as p on p.record_key = 'gp_api|' || cast(cs.user_id as string)
        where cs.br_normalized_position_id is not null and cs.geo_id is not null
        group by cs.campaign_id
    ),

    -- greatest(), not coalesce(): Databricks greatest() skips nulls, so it
    -- preserves the three-valued output while letting either leg establish
    -- TRUE. coalesce() would let the exact leg's FALSE mask a real
    -- cross-cycle holder, since coalesce(false, true) is false.
    combined as (
        select
            cs.campaign_id,
            cs.election_date,
            greatest(ex.is_incumbent, rc.is_incumbent) as is_incumbent,
            ex.is_incumbent as exact_is_incumbent,
            ex.n_covering_terms as exact_n_covering_terms,
            rc.n_covering_terms as recovered_n_covering_terms
        from campaign_seat as cs
        left join covering_exact as ex on cs.campaign_id = ex.campaign_id
        left join covering_recovered as rc on cs.campaign_id = rc.campaign_id
    )

select
    campaign_id,
    election_date,
    is_incumbent,
    -- Both metadata columns describe the leg that produced is_incumbent, so a
    -- recovered TRUE reports the recovery leg's key and term count.
    case
        when exact_is_incumbent is not null and exact_is_incumbent = is_incumbent
        then exact_n_covering_terms
        else recovered_n_covering_terms
    end as n_covering_terms,
    case
        when exact_is_incumbent is not null and exact_is_incumbent = is_incumbent
        then 'br_position_id'
        when is_incumbent is not null
        then 'normalized_position'
    end as seat_key_source
from combined
