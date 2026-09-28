-- When to expect BallotReady candidate data for an upcoming general election.
--
-- A dbt *analysis*: compiled and schema-validated, never materialized. Run it ad
-- hoc when a customer reports an empty competitive landscape, to tell a vendor
-- publication schedule apart from a defect on our side.
--
-- BR publishes general rosters state by state, months after qualifying closes, in
-- an order that held between the 2024 and 2026 cycles. So the reference cycle's
-- curve projects: median and p90 candidacy creation per state, as days before
-- election day, held constant against the target date. A state whose expected
-- date is still ahead is on schedule; only one well past p90 is worth escalating.
-- One cycle transition is an observation, not a trend -- re-validate each cycle
-- rather than trusting the offsets indefinitely.
--
-- Measure coverage here, from the marts, and not the two ways that look easier
-- and give confidently wrong answers by state:
-- - The S3 bulk feed (`stg_airbyte_source__ballotready_s3_candidacies_v3`) lags
-- the GraphQL API unevenly: it carried 19 TX candidacies for one general where
-- the API and marts carried ~2,600. It also spans only a few election years.
-- - Sampling the first N races from the API undercounts, because unpublished
-- races sort first: a 600-race sample read TX as 0% when it was ~49% covered.
-- Enumerate every race for the election, or use the marts.
--
-- Querying the API directly, a Race's global id type is `PositionElection`, not
-- `Race`; a `Race` gid returns null with no GraphQL error.
--
-- To move to a new cycle, change the two dates in `cycle` and nothing else.
with
    cycle as (
        select
            date '2026-11-03' as target_election_date,
            date '2024-11-05' as reference_election_date
    ),

    -- Our current book, so the output is ordered by who is actually affected
    -- rather than by raw race counts. State-level aggregate only.
    campaigns as (
        select
            get_json_object(campaign.details, '$.state') as state,
            count(*) as active_pro_campaigns
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as campaign
        cross join cycle as c
        where
            campaign.is_active
            and campaign.is_pro
            and get_json_object(campaign.details, '$.electionDate')
            = cast(c.target_election_date as string)
        group by 1
    ),

    candidacy_per_race as (
        select race_id, count(*) as candidacy_count
        from {{ ref("m_election_api__candidacy") }}
        group by 1
    ),

    -- Coverage as it stands right now for the target election.
    current_coverage as (
        select
            race.state,
            count(*) as races,
            sum(
                case when candidacy.race_id is null then 0 else 1 end
            ) as races_with_candidacies
        from {{ ref("m_election_api__race") }} as race
        cross join cycle as c
        left join candidacy_per_race as candidacy on candidacy.race_id = race.id
        where race.election_date = c.target_election_date
        group by 1
    ),

    -- The reference cycle's publication curve. `created_at` is the vendor's own
    -- candidacy creation timestamp, which is the closest proxy we hold for when
    -- they published. The 100-candidacy floor keeps tiny-sample states from
    -- producing a projection that looks precise and is not.
    reference_curve as (
        select
            race.state,
            count(*) as reference_candidacies,
            datediff(
                max(c.reference_election_date),
                cast(
                    from_unixtime(
                        percentile_approx(unix_timestamp(candidacy.created_at), 0.50)
                    ) as date
                )
            ) as p50_days_before_election,
            datediff(
                max(c.reference_election_date),
                cast(
                    from_unixtime(
                        percentile_approx(unix_timestamp(candidacy.created_at), 0.90)
                    ) as date
                )
            ) as p90_days_before_election
        from {{ ref("m_election_api__candidacy") }} as candidacy
        join {{ ref("m_election_api__race") }} as race on race.id = candidacy.race_id
        cross join cycle as c
        where race.election_date = c.reference_election_date
        group by 1
        having count(*) >= 100
    )

select
    coverage.state,
    coalesce(campaigns.active_pro_campaigns, 0) as active_pro_campaigns,
    coverage.races,
    round(
        100.0 * coverage.races_with_candidacies / coverage.races, 1
    ) as pct_races_covered,
    curve.p50_days_before_election,
    curve.p90_days_before_election,
    date_add(c.target_election_date, - curve.p50_days_before_election) as expected_p50,
    date_add(c.target_election_date, - curve.p90_days_before_election) as expected_p90,
    -- The question this answers is whether the vendor's wave has *started*, not
    -- whether it is complete, so the data check comes first and uses a low floor.
    -- 10% separates a started wave from an unstarted one robustly: mid-wave states
    -- land far above it and unstarted ones sit under 1%. A 50% cut would label a
    -- state that is visibly half published as late.
    -- Past p90 with the wave unstarted is the escalate signal; past p50 is a watch
    -- signal. A null curve means the reference cycle had too small a sample.
    case
        when curve.p90_days_before_election is null
        then 'no reference sample'
        when coverage.races_with_candidacies * 1.0 / coverage.races >= 0.10
        then 'publishing'
        when
            current_date()
            > date_add(c.target_election_date, - curve.p90_days_before_election)
        then 'overdue'
        when
            current_date()
            > date_add(c.target_election_date, - curve.p50_days_before_election)
        then 'past median'
        else 'on schedule'
    end as publication_status
from current_coverage as coverage
cross join cycle as c
left join campaigns on campaigns.state = coverage.state
left join reference_curve as curve on curve.state = coverage.state
order by coalesce(campaigns.active_pro_campaigns, 0) desc, coverage.state
