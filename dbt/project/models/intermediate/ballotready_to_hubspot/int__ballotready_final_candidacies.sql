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
    -- Existing HubSpot candidate codes for deduplication
    hubspot_candidate_codes as (
        select hubspot_candidate_code
        from {{ ref("int__hubspot_ytd_candidacies") }}
        where hubspot_candidate_code is not null
    ),

    -- Existing HubSpot phone numbers for deduplication (any contact with a phone)
    hubspot_phones as (
        select distinct
            nullif(trim(regexp_replace(properties_phone, '[^0-9]', '')), '') as phone
        from {{ ref("int__hubspot_ytd_candidacies") }}
        where properties_phone is not null
    ),

    br_with_contest as (
        select
            br.* except (number_of_seats_available),
            cs.uncontested,
            cs.numcands as number_of_candidates,
            cs.number_of_seats_available
        from {{ ref("int__ballotready_clean_candidacies") }} br
        left join
            {{ ref("int__candidacies_contest_final_counts") }} cs
            on br.br_contest_id = cs.contest_id
    ),

    -- Filter in for net new results
    br_new_candidacies as (
        select t1.*
        from br_with_contest t1
        where
            t1.br_candidate_code is not null
            and t1.br_candidate_code not in (
                select hubspot_candidate_code from hubspot_candidate_codes
            )
            and t1.phone not in (select phone from hubspot_phones)
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
            cast(geographic_tier as int) as geographic_tier,
            cast(
                cast(number_of_seats_available as float) as int
            ) as number_of_seats_available,
            election_type,
            party_affiliation,
            party_list,
            parties,
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
            seat,
            office_type,
            type,
            contact_owner,
            owner_name,
            candidate_id_source,
            candidacy_id,
            ballotready_race_id,
            br_contest_id,
            br_candidate_code,
            uncontested,
            number_of_candidates,
            candidate_slug,
            candidacy_created_at,
            candidacy_updated_at,
            current_timestamp() as created_at
        from br_new_candidacies
        qualify
            row_number() over (partition by id order by candidacy_updated_at desc) = 1
    )
select *
from br_new_candidacies_final
