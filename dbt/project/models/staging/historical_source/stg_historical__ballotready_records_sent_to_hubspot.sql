with
    source as (
        select * from {{ source("historical", "ballotready_records_sent_to_hubspot") }}
    ),
    renamed as (
        select
            {{ adapter.quote("candidacy_id") }} as br_candidacy_id,
            {{ adapter.quote("election_date") }},
            {{ adapter.quote("official_office_name") }},
            {{ adapter.quote("candidate_office") }},
            {{ adapter.quote("office_level") }},
            {{ adapter.quote("geographic_tier") }},
            {{ adapter.quote("number_of_seats_available") }},
            {{ adapter.quote("election_type") }},
            {{ adapter.quote("party_affiliation") }},
            {{ adapter.quote("party_list") }},
            {{ adapter.quote("parties") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("middle_name") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("city") }},
            {{ adapter.quote("district") }},
            {{ adapter.quote("seat") }},
            {{ adapter.quote("office_type") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("contact_owner") }},
            {{ adapter.quote("owner_name") }},
            {{ adapter.quote("candidate_id_source") }},
            {{ adapter.quote("br_contest_id") }},
            {{ adapter.quote("br_candidate_code") }},
            {{ adapter.quote("uncontested") }},
            {{ adapter.quote("number_of_candidates") }},
            {{ adapter.quote("candidate_slug") }},
            {{ adapter.quote("upload_timestamp") }},
            {{ adapter.quote("match_type") }},
            {{ adapter.quote("fuzzy_match_score") }},
            {{ adapter.quote("fuzzy_matched_hs_candidate_code") }},
            {{ adapter.quote("fuzzy_matched_hs_contact_id") }},
            {{ adapter.quote("fuzzy_matched_first_name") }},
            {{ adapter.quote("fuzzy_matched_last_name") }},
            {{ adapter.quote("fuzzy_matched_state") }},
            {{ adapter.quote("fuzzy_matched_office_type") }},
            {{ adapter.quote("ballotready_race_id") }} as br_race_id

        from source
    )
select *
from renamed
