-- One row per win_election_frame row: where the candidate ran. Three measures,
-- each aggregated to a district or county so no voter-level value leaves here.
--
-- independent_lean: share of the district's voters the independent-openness model
-- flags under one fixed setup (single seat, open), so it describes the place, not
-- the race. The flag cut is per state, so the lean is relative within a state.
--
-- Nearby measures use the county holding at least half the district's voters;
-- a district without one, and statewide races, get no county and null nearby
-- values. Same-race candidates never count as nearby: they shape the odds
-- directly rather than describing the area.
with
    frame as (
        select user_id, position_id, is_real_user from {{ ref("win_election_frame") }}
    ),

    -- 2026 candidacies in partisan races, the only races whose ballot shows who
    -- is independent.
    partisan_candidacies as (
        select
            gp_person_id,
            br_position_database_id as position_id,
            lower(trim(party_affiliation)) as party
        from {{ ref("candidacy") }}
        where
            is_partisan
            and (
                year(general_election_date) = 2026 or year(primary_election_date) = 2026
            )
            and br_position_database_id is not null
    ),

    positions_in_scope as (
        select distinct position_id
        from frame
        where position_id is not null
        union
        select distinct position_id
        from partisan_candidacies
    ),

    position_district as (
        select
            positions_in_scope.position_id,
            position.district_id,
            district.type = 'State' as is_statewide,
            district.state as district_state
        from positions_in_scope
        inner join
            {{ ref("m_election_api__position") }} as position
            on cast(positions_in_scope.position_id as string) = position.br_database_id
        inner join
            {{ ref("m_people_api__district") }} as district
            on position.district_id = district.id
    ),

    voter_districts as (
        select distinct district_id from position_district where not is_statewide
    ),

    -- The district-voter mart keys voters by the same salted id the voter mart mints.
    l2_voters as (
        select
            {{ generate_salted_uuid(fields=["LALVOTERID"], salt="l2") }} as voter_id,
            lalvoterid,
            county
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
    ),

    -- Statewide districts have no district-voter rows (by design upstream), so
    -- their lean comes from the state's voters directly.
    district_county as (
        select
            district_voter.district_id,
            voter.county,
            count(*) as voters,
            count(score.lalvoterid) as scored_voters,
            count_if(score.indep_openness_single_open) as flagged_voters
        from {{ ref("m_people_api__districtvoter") }} as district_voter
        inner join
            voter_districts on district_voter.district_id = voter_districts.district_id
        inner join l2_voters as voter on district_voter.voter_id = voter.voter_id
        left join
            {{ ref("int__indep_openness_voter_scores") }} as score
            on voter.lalvoterid = score.lalvoterid
        group by 1, 2
    ),

    district_summary as (
        select
            district_id,
            sum(voters) as voters,
            sum(scored_voters) as scored_voters,
            sum(flagged_voters) as flagged_voters,
            max_by(county, voters) as main_county,
            max(voters) / sum(voters) as main_county_share
        from district_county
        where county is not null
        group by 1
    ),

    state_summary as (
        select
            state,
            count(*) as scored_voters,
            count_if(indep_openness_single_open) as flagged_voters
        from {{ ref("int__indep_openness_voter_scores") }}
        group by 1
    ),

    position_geography as (
        select
            position_district.position_id,
            position_district.district_id,
            position_district.is_statewide,
            position_district.district_state,
            coalesce(
                district_summary.scored_voters, state_summary.scored_voters
            ) as scored_voters,
            coalesce(
                district_summary.flagged_voters, state_summary.flagged_voters
            ) as flagged_voters,
            district_summary.main_county_share,
            case
                when district_summary.main_county_share >= 0.5
                then district_summary.main_county
            end as nearby_county
        from position_district
        left join
            district_summary
            on position_district.district_id = district_summary.district_id
        left join
            state_summary
            on position_district.is_statewide
            and position_district.district_state = state_summary.state
    ),

    frame_geography as (
        select
            frame.user_id,
            frame.position_id,
            frame.is_real_user,
            position_geography.district_id,
            position_geography.is_statewide,
            position_geography.district_state,
            position_geography.scored_voters,
            position_geography.flagged_voters,
            position_geography.main_county_share,
            position_geography.nearby_county
        from frame
        left join
            position_geography on frame.position_id = position_geography.position_id
    ),

    win_users_nearby as (
        select me.user_id, count(distinct other.user_id) as win_users_nearby
        from frame_geography as me
        left join
            frame_geography as other
            on me.district_state = other.district_state
            and me.nearby_county = other.nearby_county
            and other.is_real_user
            and me.user_id != other.user_id
            and me.position_id != other.position_id
        where me.nearby_county is not null
        group by 1
    ),

    -- A partisan candidate runs in every county holding at least 10% of their
    -- district's voters: a multi-county seat is on each of those ballots.
    candidate_counties as (
        select distinct
            district_county.district_id,
            position_district.district_state,
            district_county.county
        from district_county
        inner join
            district_summary
            on district_county.district_id = district_summary.district_id
        inner join
            position_district
            on district_county.district_id = position_district.district_id
        where
            district_county.county is not null
            and district_county.voters >= 0.1 * district_summary.voters
    ),

    -- One row per person per race, so a fusion candidate on several party lines
    -- counts once, as independent only if no line is D or R. Unknown labels
    -- count in neither the numerator nor the denominator.
    partisan_candidates as (
        select
            partisan_candidacies.position_id,
            partisan_candidacies.gp_person_id,
            not bool_or(
                partisan_candidacies.party
                rlike '^(democrat|democratic|democratic-farmer-labor|republican)( party)?$'
            ) as is_independent
        from partisan_candidacies
        where
            partisan_candidacies.party is not null
            and partisan_candidacies.party not in ('', 'nonpartisan', 'unknown')
        group by 1, 2
    ),

    county_candidates as (
        select distinct
            candidate_counties.district_state,
            candidate_counties.county,
            partisan_candidates.position_id,
            partisan_candidates.gp_person_id,
            partisan_candidates.is_independent
        from partisan_candidates
        inner join
            position_district
            on partisan_candidates.position_id = position_district.position_id
        inner join
            candidate_counties
            on position_district.district_id = candidate_counties.district_id
    ),

    independents_nearby as (
        select
            me.user_id,
            count(
                distinct county_candidates.position_id, county_candidates.gp_person_id
            ) as partisan_candidates_nearby,
            count(
                distinct case
                    when county_candidates.is_independent
                    then
                        concat(
                            county_candidates.position_id,
                            '|',
                            county_candidates.gp_person_id
                        )
                end
            ) as independents_nearby
        from frame_geography as me
        left join
            county_candidates
            on me.district_state = county_candidates.district_state
            and me.nearby_county = county_candidates.county
            and me.position_id != county_candidates.position_id
        where me.nearby_county is not null
        group by 1
    )

select
    frame_geography.user_id,
    frame_geography.district_id,
    case
        when frame_geography.is_statewide
        then 'statewide'
        when frame_geography.district_id is not null
        then 'district'
        else 'unmatched'
    end as district_match,
    frame_geography.scored_voters as district_scored_voters,
    -- Below 500 scored voters a share is too noisy to read.
    case
        when frame_geography.scored_voters >= 500
        then frame_geography.flagged_voters / frame_geography.scored_voters
    end as independent_lean,
    frame_geography.nearby_county,
    frame_geography.main_county_share,
    win_users_nearby.win_users_nearby,
    independents_nearby.partisan_candidates_nearby,
    independents_nearby.independents_nearby,
    independents_nearby.independents_nearby / nullif(
        independents_nearby.partisan_candidates_nearby, 0
    ) as independent_share_nearby,
    current_timestamp() as built_at
from frame_geography
left join win_users_nearby on frame_geography.user_id = win_users_nearby.user_id
left join independents_nearby on frame_geography.user_id = independents_nearby.user_id
