with

    source as (select * from {{ source("er_source", "pairwise_candidacy_stages") }}),

    renamed as (

        select
            -- match scores
            cast(match_weight as double) as match_weight,
            cast(match_probability as double) as match_probability,

            -- record identifiers
            unique_id_l,
            unique_id_r,
            source_name_l,
            source_name_r,
            source_id_l,
            source_id_r,
            source_dataset_l,
            source_dataset_r,

            -- last_name comparison
            last_name_l,
            last_name_r,
            cast(gamma_last_name as int) as gamma_last_name,
            cast(tf_last_name_l as double) as tf_last_name_l,
            cast(tf_last_name_r as double) as tf_last_name_r,
            cast(bf_last_name as double) as bf_last_name,
            cast(bf_tf_adj_last_name as double) as bf_tf_adj_last_name,

            -- first_name comparison
            first_name_l,
            first_name_r,
            first_name_aliases_l,
            first_name_aliases_r,
            cast(gamma_first_name as int) as gamma_first_name,
            cast(tf_first_name_l as double) as tf_first_name_l,
            cast(tf_first_name_r as double) as tf_first_name_r,
            cast(bf_first_name as double) as bf_first_name,
            cast(bf_tf_adj_first_name as double) as bf_tf_adj_first_name,

            -- party comparison
            party_l,
            party_r,
            cast(gamma_party as int) as gamma_party,
            cast(bf_party as double) as bf_party,

            -- email comparison
            email_l,
            email_r,
            cast(gamma_email as int) as gamma_email,
            cast(bf_email as double) as bf_email,

            -- phone comparison
            phone_l,
            phone_r,
            cast(gamma_phone as int) as gamma_phone,
            cast(bf_phone as double) as bf_phone,

            -- state comparison
            state_l,
            state_r,
            cast(gamma_state as int) as gamma_state,
            cast(bf_state as double) as bf_state,

            -- election_date comparison
            cast(election_date_l as date) as election_date_l,
            cast(election_date_r as date) as election_date_r,
            cast(gamma_election_date as int) as gamma_election_date,
            cast(bf_election_date as double) as bf_election_date,

            -- official_office_name comparison
            official_office_name_l,
            official_office_name_r,
            cast(gamma_official_office_name as int) as gamma_official_office_name,
            cast(bf_official_office_name as double) as bf_official_office_name,

            -- district_identifier comparison
            cast(district_identifier_l as int) as district_identifier_l,
            cast(district_identifier_r as int) as district_identifier_r,
            cast(gamma_district_identifier as int) as gamma_district_identifier,
            cast(bf_district_identifier as double) as bf_district_identifier,

            -- race-level context
            cast(br_race_id_l as int) as br_race_id_l,
            cast(br_race_id_r as int) as br_race_id_r,
            candidate_office_l,
            candidate_office_r,
            office_level_l,
            office_level_r,
            office_type_l,
            office_type_r,
            district_raw_l,
            district_raw_r,
            seat_name_l,
            seat_name_r,
            cast(br_candidacy_id_l as int) as br_candidacy_id_l,
            cast(br_candidacy_id_r as int) as br_candidacy_id_r,
            election_stage_l,
            election_stage_r,

            -- blocking rule that generated this pair
            cast(match_key as int) as match_key

        from source

    )

select *
from renamed
