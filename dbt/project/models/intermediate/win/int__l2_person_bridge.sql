-- Win signup -> L2 voter record. Easy-match pass: exact (declared state,
-- first name, last name), resolved when the match is unique, or when exactly
-- one of a 2-5 match set shares the user's zip.
--
-- Deliberately narrow. Nicknames, edit distance, nationwide search and
-- middle-name tiebreaks belong to the entity-resolution pipeline; a user this
-- model cannot resolve simply gets no row, so a bridge built there can replace
-- this one without a schema change for consumers.
--
-- The ambiguity counts stay HERE and are not promoted into any feature table.
-- The match is name-based and the corroboration label is name-matched entity
-- resolution, so name commonness is a leak channel into anything trained on
-- that label. They are diagnostics for judging a link, not model inputs.
with
    population as (
        select
            u.user_id,
            u.campaign_state,
            upper(trim(u.user_first_name)) as first_name,
            upper(trim(u.user_last_name)) as last_name,
            substring(trim(u.user_zip), 1, 5) as zip5
        from {{ ref("users_win_candidacy") }} as u
        where
            u.is_latest_version
            and not coalesce(u.is_demo, false)
            and u.user_first_name is not null
            and u.user_last_name is not null
            and u.campaign_state is not null
        -- One row per user: a user with several campaigns would double-count
        -- every per-user match window below and duplicate the grain.
        qualify
            row_number() over (
                partition by u.user_id
                order by u.election_date desc nulls last, u.campaign_id desc
            )
            = 1
    ),

    matches as (
        select
            p.user_id,
            l.lalvoterid,
            substring(l.residence_addresses_zip, 1, 5) = p.zip5 as zip_matches,
            count(*) over (partition by p.user_id) as n_name_matches,
            sum(
                case
                    when substring(l.residence_addresses_zip, 1, 5) = p.zip5
                    then 1
                    else 0
                end
            ) over (partition by p.user_id) as n_zip_matches
        from population as p
        inner join
            {{ ref("int__l2_nationwide_uniform") }} as l
            on l.residence_addresses_state = p.campaign_state
            and upper(trim(l.voters_firstname)) = p.first_name
            and upper(trim(l.voters_lastname)) = p.last_name
    )

select
    user_id,
    lalvoterid,
    case when n_name_matches = 1 then 'unique' else 'zip_resolved' end as match_type,
    n_name_matches,
    n_zip_matches
from matches
where
    n_name_matches = 1
    or (n_name_matches between 2 and 5 and n_zip_matches = 1 and zip_matches)
