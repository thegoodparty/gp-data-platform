-- Voters per district and county, with how many the independent-openness model
-- scores and flags. Joining every district voter to L2 is the expensive step
-- behind win_election_geography, and its inputs only change with an L2 load, so
-- it runs monthly and the daily mart reads this small table.
with
    -- The district-voter mart keys voters by the same salted id the voter mart mints.
    l2_voters as (
        select
            {{ generate_salted_uuid(fields=["LALVOTERID"], salt="l2") }} as voter_id,
            lalvoterid,
            county
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
    )

select
    district_voter.district_id,
    voter.county,
    count(*) as voters,
    count(score.lalvoterid) as scored_voters,
    count_if(score.indep_openness_single_open) as flagged_voters
from {{ ref("m_people_api__districtvoter") }} as district_voter
inner join l2_voters as voter on district_voter.voter_id = voter.voter_id
left join
    {{ ref("int__indep_openness_voter_scores") }} as score
    on voter.lalvoterid = score.lalvoterid
group by 1, 2
