-- The run-based party correction may only replace a term's party with one
-- stated affiliation: all Democratic lines, all Republican lines, or only
-- independent lines, and only on a term whose feed party states an
-- affiliation. Anything else means it reached a term it should not have, or
-- published a seat label like "Nonpartisan" or a town line as a party. Rows
-- set by the party override seed are exempt.
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
left join
    {{ ref("office_holder_party_overrides") }} as party_override
    on office_holder.br_office_holder_id = party_override.br_office_holder_id
where
    party_override.br_office_holder_id is null
    and size(source_party.party_names) > 0
    and not office_holder.party_names <=> source_party.party_names
    and (
        not exists (
            source_party.party_names,
            p -> p rlike '^(Democratic|Republican)'
            or p in (
                'Independent',
                'Unaffiliated',
                'No Party Preference',
                'Petitioning Candidate'
            )
        )
        or not (
            forall(office_holder.party_names, p -> p rlike '^Democratic')
            or forall(office_holder.party_names, p -> p rlike '^Republican')
            or forall(
                office_holder.party_names,
                p -> p in (
                    'Independent',
                    'Unaffiliated',
                    'No Party Preference',
                    'Petitioning Candidate'
                )
            )
        )
    )
