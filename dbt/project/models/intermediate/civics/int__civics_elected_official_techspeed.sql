{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed office holders -> Civics mart elected_officials schema
-- Source: stg_airbyte_source__techspeed_gdrive_officeholders
--
-- Grain: One row per officeholder-position (current snapshot, deduplicated).
--
-- Key differences from BallotReady EO model:
-- - ts_officeholder_id is NOT a person-level PK (reused across people)
-- - UUID uses composite key: ts_officeholder_id + position_id + office_name_clean
-- - No term dates (TechSpeed doesn't track terms)
-- - State requires clean_states join (mixed format + blanks)
-- - Multi-delivery dedup via ROW_NUMBER on date_processed_date
-- - Schema is narrower (omits BR-only columns); merge model owns alignment
--
with
    clean_states as (select * from {{ ref("clean_states") }}),

    office_holders as (
        select * from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
    ),

    with_clean_state as (
        select
            oh.*,
            -- Normalized office_name for dedup + UUID consistency
            trim(office_name) as office_name_clean,
            -- State: nullify blanks so not_null test catches unresolved rows
            nullif(
                trim(coalesce(cs.state_cleaned_postal_code, oh.state)), ''
            ) as state_clean
        from office_holders oh
        left join clean_states cs on upper(trim(oh.state)) = upper(trim(cs.state_raw))
    ),

    derived_fields as (
        select
            *,
            -- is_judicial: from source office_type (before normalization)
            case
                when
                    lower(trim(office_type))
                    in ('judge', 'municipal court', 'municipal court judge')
                then true
                else false
            end as is_judicial,
            -- candidate_office: TS-native approach (matches int__techspeed_candidates)
            initcap(trim(office_normalized)) as candidate_office,
            -- office_level: from level field, city→Local, else initcap
            case
                when lower(trim(level)) = 'city' then 'Local' else initcap(trim(level))
            end as office_level_derived,
            -- office_type: TS-native case normalization map (covers all 34 source
            -- variants)
            case
                when
                    lower(trim(office_type))
                    in ('city council', 'city council member', 'city commissioner')
                then 'City Council'
                when
                    lower(trim(office_type))
                    in ('town council', 'town commission', 'village council')
                then 'Town Council'
                when lower(trim(office_type)) in ('school board', 'educational board')
                then 'School Board'
                when lower(trim(office_type)) in ('mayor', 'city mayor')
                then 'Mayor'
                when
                    lower(trim(office_type))
                    in ('judge', 'municipal court', 'municipal court judge')
                then 'Judge'
                when lower(trim(office_type)) in ('clerk/treasurer', 'clerk/treasure')
                then 'Clerk/Treasurer'
                when lower(trim(office_type)) = 'county supervisor'
                then 'County Supervisor'
                when lower(trim(office_type)) = 'sheriff'
                then 'Sheriff'
                when lower(trim(office_type)) in ('state house', 'state senate')
                then initcap(trim(office_type))
                when lower(trim(office_type)) = 'town council/county supervisor'
                then 'Town Council'
                when lower(trim(office_type)) in ('surveyor', 'attorney')
                then initcap(trim(office_type))
                else 'Other'
            end as office_type_derived,
            -- party_affiliation: via parse_party_affiliation macro
            {{ parse_party_affiliation("party") }} as party_affiliation
        from with_clean_state
    ),

    elected_officials as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "'techspeed_officeholder'",
                        "cast(ts_officeholder_id as string)",
                        "cast(position_id as string)",
                        "office_name_clean",
                    ]
                )
            }} as gp_elected_official_id,
            ts_officeholder_id,
            position_id as ts_position_id,
            normalized_position_id as ts_normalized_position_id,
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,
            nullif(trim(email), '') as email,
            nullif(trim(phone_clean), '') as phone,
            office_name as position_name,
            office_normalized as normalized_position_name,
            candidate_office,
            office_level_derived as office_level,
            office_type_derived as office_type,
            state_clean as state,
            nullif(trim(city), '') as city,
            nullif(trim(district_name), '') as district,
            is_incumbent,
            is_judicial,
            is_uncontested,
            party_affiliation,
            nullif(trim(street_address), '') as mailing_address_line_1,
            nullif(trim(city), '') as mailing_city,
            state_clean as mailing_state,
            nullif(trim(postal_code), '') as mailing_zip,
            cast(tier as int) as tier,
            nullif(trim(county_municipality), '') as county_municipality,
            nullif(trim(normalized_location), '') as normalized_location,
            ts_status,
            'techspeed' as candidate_id_source,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at
        from derived_fields
        qualify
            row_number() over (
                partition by ts_officeholder_id, position_id, office_name_clean
                order by date_processed_date desc nulls last, _airbyte_extracted_at desc
            )
            = 1
    )

select
    gp_elected_official_id,
    ts_officeholder_id,
    ts_position_id,
    ts_normalized_position_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    position_name,
    normalized_position_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    is_incumbent,
    is_judicial,
    is_uncontested,
    party_affiliation,
    mailing_address_line_1,
    mailing_city,
    mailing_state,
    mailing_zip,
    tier,
    county_municipality,
    normalized_location,
    ts_status,
    candidate_id_source,
    created_at,
    updated_at
from elected_officials
