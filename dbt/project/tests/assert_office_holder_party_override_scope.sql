-- The independent-run party correction may only move a term from a
-- Democratic or Republican office party to independent lines. Anything else
-- means it reached a term it should not have, or published a seat label like
-- "Nonpartisan" as someone's party.
with
    source_party as (
        select br_office_holder_id, party_names
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        qualify
            row_number() over (
                partition by br_office_holder_id order by office_holder_updated_at desc
            )
            = 1
    )

select
    office_holder.id,
    office_holder.party_names,
    source_party.party_names as source_party_names
from {{ ref("m_election_api__office_holder") }} as office_holder
inner join
    source_party on office_holder.br_office_holder_id = source_party.br_office_holder_id
where
    size(source_party.party_names) > 0
    and not office_holder.party_names <=> source_party.party_names
    and (
        not exists (source_party.party_names, p -> p rlike '^(Democratic|Republican)')
        or exists (
            office_holder.party_names,
            p -> p not in (
                'Independent',
                'Unaffiliated',
                'No Party Preference',
                'Petitioning Candidate'
            )
        )
    )
