-- Every upstream race inside the 2-month grace period that passes the mart's
-- other gates (place retained, position matched) must stay in
-- m_election_api__race: the campaign plan reads races after election day, so
-- dropping them breaks it the morning after an election. Both window edges
-- are padded by a day to absorb build-vs-test-time date skew.
with
    eligible as (
        select tbl_race.br_database_id
        from {{ ref("int__enhanced_race") }} as tbl_race
        where
            tbl_race.br_database_id is not null
            and tbl_race.election_date
            between current_date()
            - interval '2 months'
            + interval '1 day' and current_date()
            - interval '1 day'
            and tbl_race.place_id in (select id from {{ ref("m_election_api__place") }})
            and tbl_race.br_position_database_id
            in (select br_database_id from {{ ref("m_election_api__position") }})
    )
select eligible.br_database_id
from eligible
left join
    {{ ref("m_election_api__race") }} as tbl_mart
    on eligible.br_database_id = tbl_mart.br_database_id
where tbl_mart.br_database_id is null
