-- Zero zip rows means the office never reaches the picker, however well its district
-- resolves.
with
    upcoming_legislative_positions as (
        select distinct position.br_database_id, position.name
        from {{ ref("m_election_api__position") }} as position
        join
            {{ ref("m_election_api__district") }} as district
            on district.id = position.district_id
        join
            {{ ref("election") }} as election
            on election.br_position_database_id
            = cast(position.br_database_id as bigint)
        where
            district.l2_district_type in (
                'State_House_District',
                'State_Senate_District',
                'State_Legislative_District',
                'US_Congressional_District'
            )
            and election.election_date > current_date()
            and election.election_date <= current_date() + interval 2 years
    )
select upcoming_legislative_positions.*
from upcoming_legislative_positions
where
    br_database_id not in (
        select br_database_id
        from {{ ref("int__zip_code_to_br_office") }}
        where br_database_id is not null
    )
