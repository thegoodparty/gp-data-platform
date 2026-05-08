with
    source as (
        select * from {{ source("airbyte_source", "ddhq_gdrive_election_results") }}
    ),

    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(date as date) as election_date,
            votes,
            cast(try_cast(race_id as float) as int) as ddhq_race_id,
            candidate,
            {{ ref("parse_human_name") }} (candidate) as candidate_parsed_name,
            candidate_parsed_name.first as candidate_first_name,
            candidate_parsed_name.middle as candidate_middle_name,
            candidate_parsed_name.last as candidate_last_name,
            candidate_parsed_name.suffix as candidate_suffix,
            candidate_parsed_name.nickname as candidate_nickname,
            {{ cast_to_boolean("is_winner", ["y"], ["n"]) }} as is_winner,
            race_name,
            cast(cast(nullif(candidate_id, '') as float) as int) as candidate_id,
            election_type,
            cast(is_uncontested as boolean) as is_uncontested,
            candidate_party,
            _ab_source_file_url,
            cast(
                try_cast(number_of_seats_in_election as float) as int
            ) as number_of_seats_in_election,
            _ab_source_file_last_modified,
            total_number_of_ballots_in_race
        from source
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    us_states as (select * from {{ ref("us_states") }}),

    -- Resolve state postal code from the 2-letter prefix of race_name.
    -- A source-level test confirms all race_names match ^[A-Z]{2} .
    with_state as (
        select
            r.*,
            split(r.race_name, ' ')[0] as state_prefix,
            cs.state_cleaned_postal_code as state_postal_code,
            us.state_name as state
        from renamed as r
        left join
            clean_states as cs
            on upper(split(r.race_name, ' ')[0]) = upper(trim(cs.state_raw))
        left join us_states as us on cs.state_cleaned_postal_code = us.state_postal_code
    ),

    with_derived_fields as (
        select
            * except (state_prefix),
            trim(
                substring(race_name, length(state_prefix) + 2)
            ) as official_office_name,
            {{ parse_ddhq_candidate_office("race_name") }} as candidate_office,
            {{ map_office_type("candidate_office") }} as office_type,
            {{ parse_ddhq_office_level("race_name") }} as office_level,
            coalesce(
                nullif(
                    regexp_extract(
                        race_name,
                        '(?i)(?:ward|district|seat|place|position|zone) ([^ ]+)$'
                    ),
                    ''
                ),
                nullif(regexp_extract(race_name, ' ([0-9]+)$'), ''),
                ''
            ) as district,
            case
                when
                    lower(election_type) like '%runoff%'
                    and lower(election_type) like '%primary%'
                then 'primary runoff'
                when lower(election_type) like '%runoff%'
                then 'general runoff'
                when lower(election_type) like '%primary%'
                then 'primary'
                else 'general'
            end as election_stage,
            {{ parse_party_affiliation("candidate_party") }} as party_affiliation
        from with_state
    ),

    invalid as (
        select _airbyte_raw_id
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results_invalid") }}
    )

select *
from with_derived_fields
where
    _airbyte_raw_id
    not in (select _airbyte_raw_id from invalid where _airbyte_raw_id is not null)
