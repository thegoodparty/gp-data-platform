-- Two-way, district-level: (a) any race whose (district, year, day type)
-- exists in the model output must carry that row's values (a NULL or a
-- mismatch means the join silently missed or served stale numbers);
-- (b) any race carrying values must trace back to an identical model-output
-- row (nothing fabricated, nothing stale). Zero tolerance both ways.
-- The joins below are LEFT so a race with no position/district match stays
-- in scope: its NULL tuple can never match a model row, so if it somehow
-- carries values, direction (b) flags it instead of losing it.
-- This equality transfer is also what makes ordering, paired nullness, and
-- pre-horizon nullness hold on the mart without their own tests: the model
-- output enforces them at the source, and every served value must equal a
-- model row here.
with
    race_key as (
        select
            tbl_race.id,
            tbl_race.projected_turnout,
            tbl_race.projected_turnout_lower,
            tbl_race.projected_turnout_upper,
            tbl_race.inference_at,
            tbl_district.state,
            tbl_district.l2_district_type,
            tbl_district.l2_district_name,
            year(tbl_race.election_date) as election_year,
            case
                when tbl_race.election_code = 'LocalOrMunicipal'
                then 'Local_or_Municipal'
                else tbl_race.election_code
            end as model_election_code
        from {{ ref("m_election_api__race") }} as tbl_race
        left join
            {{ ref("m_election_api__position") }} as tbl_position
            on tbl_race.position_id = tbl_position.id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_position.district_id = tbl_district.id
    )

select race_key.id, 'race missing or mismatching its model row' as failure
from race_key
inner join
    {{ ref("int__voter_turnout_inference") }} as projections
    on race_key.state = projections.state
    and race_key.l2_district_type = projections.district_type
    and race_key.l2_district_name = projections.district_name
    and race_key.election_year = projections.election_year
    and race_key.model_election_code = projections.election_code
where
    race_key.projected_turnout
    is distinct from cast(projections.ballots_projected as int)
    or race_key.projected_turnout_lower
    is distinct from cast(projections.ballots_projected_lower as int)
    or race_key.projected_turnout_upper
    is distinct from cast(projections.ballots_projected_upper as int)
    or race_key.inference_at is distinct from projections.inference_at
union all
select race_key.id, 'race carries values with no model row behind them' as failure
from race_key
left join
    {{ ref("int__voter_turnout_inference") }} as projections
    on race_key.state = projections.state
    and race_key.l2_district_type = projections.district_type
    and race_key.l2_district_name = projections.district_name
    and race_key.election_year = projections.election_year
    and race_key.model_election_code = projections.election_code
where
    (
        race_key.projected_turnout is not null
        or race_key.projected_turnout_lower is not null
        or race_key.projected_turnout_upper is not null
        or race_key.inference_at is not null
    )
    and projections.state is null
