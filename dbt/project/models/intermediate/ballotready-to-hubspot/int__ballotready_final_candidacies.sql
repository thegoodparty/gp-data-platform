{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["intermediate", "ballotready"],
    )
}}
-- Creates the final set of BallotReady candidacies to be uploaded to HubSpot
-- Add in the contested results
with
    br_with_contest as (
        select
            br.* except (number_of_seats_available),
            cs.uncontested,
            cs.numcands as number_of_candidates,
            cs.number_of_seats_available
        from {{ ref("int__ballotready_cleancandidacies") }} br
        left join
            {{ ref("int__candidacies_contest_final_counts") }} cs
            on br.br_contest_id = cs.contest_id
    ),

    -- Filter in for net new results
    br_new_candidacies as (
        select t1.*
        from br_with_contest t1
        where
            t1.br_candidate_code not in (
                select hs_candidate_code from {{ ref("int__hubspot_candidacy_codes") }}
            )
    ),

    -- write formatted new batch data to a table
    br_new_candidacies_final as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "election_date",
                        "candidate_office",
                        "state",
                        "last_name",
                        "first_name",
                    ],
                    salt="ballotready",
                )
            }} as id,
            cast(election_date as date) as election_date,
            official_office_name,
            candidate_office,
            office_level,
            cast(candidate_id_tier as int) as candidate_id_tier,
            cast(number_of_seats_available as int) as number_of_seats_available,
            election_type,
            party_affiliation,
            party_list,
            first_name,
            middle_name,
            last_name,
            state,
            phone,
            email,
            city,
            case
                when district like '%, %'
                then left(district, position(', ' in district) - 1)
                else district
            end as district,
            office_type,
            type,
            contact_owner,
            owner_name,
            candidate_id_source,
            uncontested,
            number_of_candidates,
            current_timestamp() as created_at
        from br_new_candidacies
    )
select *
from br_new_candidacies_final
