-- Product campaigns with the BallotReady position and race filled in for
-- signups that never picked one.
--
-- Without a position or race the candidacy prematch and the gp_api civics
-- models drop the campaign, even when BallotReady lists the same person that
-- day. A name alone is not enough to fill one, since nothing else ties the
-- signup to a race, so a contact (email or phone) must agree too. A person with
-- two candidacies on one day gets no position rather than a guess.
with
    campaigns as (select * from {{ ref("campaigns") }}),

    unresolved as (
        select
            campaign_id,
            election_date,
            upper(trim(campaign_state)) as state,
            lower(trim(user_email)) as email,
            right(regexp_replace(user_phone, '[^0-9]', ''), 10) as phone,
            lower(trim(user_first_name)) as first_name,
            lower(trim(user_last_name)) as last_name
        from campaigns
        where
            is_latest_version
            and not coalesce(is_demo, false)
            and ballotready_position_id is null
            and ballotready_race_id is null
            and election_date is not null
    ),

    br_candidacy as (
        select
            br_candidacy_id,
            cast(br_position_id as bigint) as br_position_id,
            cast(br_race_id as bigint) as br_race_id,
            cast(br_normalized_position_id as int) as br_normalized_position_id,
            position_name,
            election_day,
            upper(trim(state)) as state,
            lower(trim(email)) as email,
            right(regexp_replace(phone, '[^0-9]', ''), 10) as phone,
            lower(trim(first_name)) as first_name,
            lower(trim(nickname)) as nickname,
            lower(trim(last_name)) as last_name
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    matched as (
        select
            u.campaign_id,
            b.br_candidacy_id,
            b.br_position_id,
            b.br_race_id,
            b.br_normalized_position_id,
            b.position_name,
            case when b.email = u.email then 'email' else 'phone' end as match_key
        from unresolved as u
        inner join
            br_candidacy as b
            on b.election_day = u.election_date
            and b.state = u.state
            and b.last_name = u.last_name
            and (b.first_name = u.first_name or b.nickname = u.first_name)
            and (b.email = u.email or (length(u.phone) = 10 and b.phone = u.phone))
    ),

    inferred as (
        select
            campaign_id,
            max(br_candidacy_id) as br_candidacy_id,
            max(br_position_id) as br_position_id,
            max(br_race_id) as br_race_id,
            max(br_normalized_position_id) as br_normalized_position_id,
            max(position_name) as position_name,
            -- email outranks phone when both matched
            min(match_key) as match_key
        from matched
        group by campaign_id
        having count(distinct br_candidacy_id) = 1
    )

select
    c.* except (
        ballotready_position_id,
        ballotready_race_id,
        campaign_office,
        normalized_position_name
    ),
    coalesce(c.ballotready_position_id, i.br_position_id) as ballotready_position_id,
    coalesce(c.ballotready_race_id, i.br_race_id) as ballotready_race_id,
    case
        when i.campaign_id is not null and nullif(trim(c.campaign_office), '') is null
        then i.position_name
        else c.campaign_office
    end as campaign_office,
    coalesce(c.normalized_position_name, np.name) as normalized_position_name,
    i.br_candidacy_id as inferred_br_candidacy_id,
    i.match_key as inferred_br_match_key
from campaigns as c
left join inferred as i on c.campaign_id = i.campaign_id and c.is_latest_version
left join
    {{ ref("int__ballotready_normalized_position") }} as np
    on i.br_normalized_position_id = np.database_id
