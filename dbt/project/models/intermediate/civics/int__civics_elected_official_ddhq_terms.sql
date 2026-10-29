-- DDHQ election winners as elected-official terms, for wins BallotReady has a
-- seat for but no term. Grain: one row per 2026+ DDHQ candidacy that won its
-- general or runoff (its latest winning stage), keyed on the DDHQ candidacy-stage
-- record of that win.
--
-- Scope: only wins with a BR position, from the DDHQ race's adopted BR race or,
-- failing that, its office match. The position carries the office fields and ICP
-- flags. A win without one has no comparable office and waits for a later pass.
--
-- br_office_holder_id marks a BR term that already covers the win: same position,
-- same person or surname, starting within 60 days before to 400 days after the
-- election. elected_official_terms adds only uncovered wins and stamps the covering
-- BR term with this row's ddhq_term_id. BR rarely publishes these terms (about 1%
-- of DDHQ-only winners), so the DDHQ id usually stays the term's id.
--
-- gp_person_id is null until a matcha person run clusters the DDHQ record; the
-- marts carry a win only once it has a person.
with
    winning_stages as (
        select
            s.*,
            cast(s.source_candidate_id as string)
            || '_'
            || cast(s.source_race_id as string) as ddhq_record_id
        from {{ ref("int__civics_candidacy_stage_ddhq") }} as s
        where
            s.election_date >= '2026-01-01'
            and s.is_winner
            and s.election_stage not like 'primary%'
        -- A candidacy's runoff decides it when there is one, so its latest win is
        -- the one that confers office.
        qualify
            row_number() over (
                partition by s.gp_candidacy_id
                order by s.election_date desc, s.gp_candidacy_stage_id
            )
            = 1
    ),

    race_positions as (
        select gp_election_stage_id, br_position_id
        from {{ ref("int__civics_election_stage_ballotready") }}
        where br_position_id is not null
    ),

    office_positions as (
        select ddhq_election_stage_key, br_position_id
        from {{ ref("int__civics_ddhq_br_position_match") }}
    ),

    positions as (
        select
            br_database_position_id as br_position_id,
            br_position_name,
            normalized_position_type,
            is_judicial,
            is_appointed,
            icp_office_win,
            icp_office_serve,
            icp_win_supersize,
            icp_win_effective_date
        from {{ ref("int__icp_offices") }}
    ),

    position_levels as (
        select database_id as br_position_id, level
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    wins as (
        select
            w.ddhq_record_id,
            {{ generate_salted_uuid(fields=["'ddhq_winner'", "w.ddhq_record_id"]) }}
            as ddhq_term_id,
            pid.gp_person_id,
            w.source_candidate_id as ddhq_candidate_id,
            w.source_race_id as ddhq_race_id,
            w.gp_candidacy_id,
            coalesce(rp.br_position_id, op.br_position_id) as br_position_id,
            case
                when rp.br_position_id is not null
                then 'race'
                when op.br_position_id is not null
                then 'office'
            end as br_position_source,
            w.candidate_first_name as first_name,
            w.candidate_last_name as last_name,
            w.candidate_full_name as full_name,
            w.state_postal_code as state,
            w.party_affiliation,
            w.district,
            w.election_date as elected_date,
            w.election_stage,
            w.is_uncontested,
            w.created_at,
            w.updated_at
        from winning_stages as w
        left join race_positions as rp using (gp_election_stage_id)
        left join
            office_positions as op
            on op.ddhq_election_stage_key
            =
            {{ generate_ddhq_election_stage_key("w.ddhq_race_id", "w.election_stage") }}
        left join person_ids as pid on pid.record_key = 'ddhq|' || w.ddhq_record_id
    ),

    br_terms as (
        select
            t.br_office_holder_id,
            t.br_position_id,
            t.term_start_date,
            lower(trim(t.last_name)) as last_name,
            pid.gp_person_id
        from {{ ref("int__civics_elected_official_ballotready") }} as t
        left join
            person_ids as pid
            on pid.record_key = 'ballotready|' || cast(t.br_candidate_id as string)
        where t.br_position_id is not null and t.term_start_date is not null
    ),

    -- One covering BR term per win; the earliest start is the term the win began.
    covered as (
        select wins.ddhq_record_id, b.br_office_holder_id
        from wins
        inner join
            br_terms as b
            on b.br_position_id = wins.br_position_id
            and b.term_start_date
            between date_sub(wins.elected_date, 60) and date_add(wins.elected_date, 400)
            and (
                b.gp_person_id = wins.gp_person_id
                or b.last_name = lower(trim(wins.last_name))
            )
        qualify
            row_number() over (
                partition by wins.ddhq_record_id
                order by b.term_start_date, b.br_office_holder_id
            )
            = 1
    )

select
    wins.ddhq_record_id,
    wins.ddhq_term_id,
    wins.gp_person_id,
    wins.ddhq_candidate_id,
    wins.ddhq_race_id,
    wins.gp_candidacy_id,
    wins.br_position_id,
    wins.br_position_source,
    covered.br_office_holder_id,
    wins.first_name,
    wins.last_name,
    wins.full_name,
    wins.state,
    wins.party_affiliation,
    p.br_position_name as position_name,
    p.normalized_position_type as normalized_position_name,
    {{
        generate_candidate_office_from_position(
            "p.br_position_name", "p.normalized_position_type"
        )
    }} as candidate_office,
    initcap(pl.level) as office_level,
    {{ map_office_type("candidate_office") }} as office_type,
    {{ extract_city_from_office_name("p.br_position_name") }} as city,
    coalesce(
        {{ extract_district_geographic("p.br_position_name") }},
        nullif(trim(wins.district), '')
    ) as district,
    p.is_judicial,
    p.is_appointed,
    -- Win ICP is gated on the election date, as on BR terms; Serve is not.
    case
        when p.icp_win_effective_date is null
        then p.icp_office_win
        when wins.elected_date < p.icp_win_effective_date
        then false
        else p.icp_office_win
    end as is_win_icp,
    p.icp_office_serve as is_serve_icp,
    case
        when p.icp_win_effective_date is null
        then p.icp_win_supersize
        when wins.elected_date < p.icp_win_effective_date
        then false
        else p.icp_win_supersize
    end as is_win_supersize_icp,
    wins.elected_date,
    wins.election_stage,
    wins.is_uncontested,
    wins.created_at,
    wins.updated_at
from wins
left join covered using (ddhq_record_id)
left join positions as p using (br_position_id)
left join position_levels as pl using (br_position_id)
where wins.br_position_id is not null
