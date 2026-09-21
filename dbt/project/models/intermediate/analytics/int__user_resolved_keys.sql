-- Keys that reach each downstream source, at gp-api user grain. Consumers join
-- this instead of re-deriving the person link, and instead of the scalar id
-- columns on the people mart, which go null exactly when a person holds more
-- than one record of that type and so silently drop the ambiguous rows.
with
    users as (select user_id, gp_person_id from {{ ref("users") }}),

    -- Person-graph membership for gp-api records only. record_key is
    -- source_name || '|' || source_id, so the user id is the trailing segment.
    gp_api_members as (
        select
            gp_person_id,
            cast(substring_index(record_key, '|', -1) as bigint) as user_id,
            first_seen_at
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'gp_api'
    ),

    -- Restricted to accounts this model actually has a row for. The node model
    -- deliberately seeds a label for a gp-api user id absent from the staging
    -- user table, so the graph's gp-api membership can be a superset of the
    -- spine. Ranking over the superset would hand a person zero primaries
    -- whenever their earliest account is one of those absent ids.
    spine_members as (
        select m.gp_person_id, m.user_id, m.first_seen_at
        from gp_api_members as m
        inner join users as u using (user_id)
    ),

    -- Primary account is the earliest gp-api record in the person group, not
    -- the record that minted the person id: only 61% of user persons are
    -- minted by a gp-api record, so a mint-based flag would be false for every
    -- account of the rest and would fail to select one row per person.
    account_ranks as (
        select
            user_id,
            count(*) over (partition by gp_person_id) as account_count,
            row_number() over (
                partition by gp_person_id order by first_seen_at asc, user_id asc
            )
            = 1 as is_primary_account
        from spine_members
    ),

    groups as (
        select
            cast(substring_index(record_key, '|', -1) as bigint) as user_id,
            had_conflict
        from {{ ref("int__civics_person_groups") }}
        where source_name = 'gp_api'
    )

select
    u.user_id,
    u.gp_person_id,
    coalesce(r.account_count, 1) as account_count,
    coalesce(r.is_primary_account, true) as is_primary_account,
    coalesce(g.had_conflict, false) as had_conflict
from users as u
left join account_ranks as r using (user_id)
left join groups as g using (user_id)
