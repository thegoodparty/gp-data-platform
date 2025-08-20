{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    candidates_wo_suffix as (
        select
            candidate_id_source,
            first_name,
            last_name,
            candidate_type,
            email,
            phone,
            candidate_id_tier,
            party,
            website_url,
            linkedin_url,
            instagram_handle,
            twitter_handle,
            facebook_url,
            birth_date,
            street_address,
            postal_code,
            district,
            city,
            state,
            official_office_name,
            candidate_office,
            office_type,
            office_level,
            filing_deadline,
            primary_election_date,
            general_election_date,
            election_type,
            uncontested,
            number_of_candidates,
            number_of_seats_available,
            open_seat,
            partisan,
            population,
            ballotready_race_id,
            type,
            contact_owner,
            owner_name,
            uploaded,
            _airbyte_extracted_at,
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    regexp_replace(
                                                        regexp_replace(
                                                            regexp_replace(
                                                                last_name, ' Jr$', ''
                                                            ),
                                                            ' Jr\.$',
                                                            ''
                                                        ),
                                                        ' Sr$',
                                                        ''
                                                    ),
                                                    ' Sr\.$',
                                                    ''
                                                ),
                                                ' II$',
                                                ''
                                            ),
                                            ' Ii$',
                                            ''
                                        ),
                                        ' III$',
                                        ''
                                    ),
                                    ' Iii$',
                                    ''
                                ),
                                ' Iv$',
                                ''
                            ),
                            ' I ii$',
                            ''
                        ),
                        ' V$',
                        ''
                    ),
                    ' Iiii$',
                    ''
                )
            ) as last_name_no_suffix
        from {{ ref("int__techspeed_candidates") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    ),
    candidates_w_clean_comma as (
        select
            * except (last_name_no_suffix),
            -- Remove trailing commas
            regexp_replace(last_name_no_suffix, ',$', '') as last_name_no_comma
        from candidates_wo_suffix
    ),
    candidates_w_clean_space as (
        select
            * except (last_name_no_comma),
            case
                when
                    length(last_name_no_comma) >= 2
                    and substring(last_name_no_comma, length(last_name_no_comma) - 1, 1)
                    = ' '
                then substring(last_name_no_comma, 1, length(last_name_no_comma) - 2)
                else last_name_no_comma
            end as last_name_trimmed
        from candidates_w_clean_comma
    ),
    candidates_w_clean_initials as (
        select
            * except (last_name_trimmed),
            -- Handle middle initial with period but no space
            case
                when
                    length(last_name_trimmed) >= 3
                    and substring(last_name_trimmed, 2, 1) = '.'
                    and substring(last_name_trimmed, 3, 1) != ' '
                then substring(last_name_trimmed, 3)
                else last_name_trimmed
            end as last_name_clean
        from candidates_w_clean_space
    ),
    candidates_w_extracted_last_name as (
        select
            * except (last_name_clean),
            -- Extract suggested last name (everything after last space)
            case
                when last_name_clean like '% %'
                then
                    case
                        -- Handle special prefixes that should be kept
                        when last_name_clean like '%De La %'
                        then
                            'De La ' || substring(
                                last_name_clean,
                                position('De La ' in last_name_clean) + 7
                            )
                        -- Hugh: Why is the offset 7 here, instead of 5 (= len('de la
                        -- '))?
                        when last_name_clean like '%Van %'
                        then
                            'Van ' || substring(
                                last_name_clean, position('Van ' in last_name_clean) + 4
                            )
                        when last_name_clean like '% van %'
                        then
                            ' van ' || substring(
                                last_name_clean,
                                position(' van ' in last_name_clean) + 5
                            )
                        when last_name_clean like '%Le %'
                        then
                            'Le ' || substring(
                                last_name_clean, position('Le ' in last_name_clean) + 3
                            )
                        else
                            -- Default: take everything after the last space
                            substring(
                                last_name_clean,
                                length(last_name_clean)
                                - position(' ' in reverse(last_name_clean))
                                + 2
                            )
                    end
                else last_name_clean
            end as suggested_last
        from candidates_w_clean_initials
    )

select
    candidate_id_source,
    first_name,
    suggested_last as last_name,
    candidate_type,
    email,
    case
        when first_name is null
        then null
        when last_name is null
        then null
        when state is null
        then null
        when office_type is null
        then null
        else
            lower(
                concat_ws(
                    '__',
                    regexp_replace(
                        regexp_replace(trim(first_name), ' ', '-'), '[^a-zA-Z0-9-]', ''
                    ),
                    regexp_replace(
                        regexp_replace(trim(last_name), ' ', '-'), '[^a-zA-Z0-9-]', ''
                    ),
                    regexp_replace(
                        regexp_replace(trim(state), ' ', '-'), '[^a-zA-Z0-9-]', ''
                    ),
                    regexp_replace(
                        regexp_replace(trim(office_type), ' ', '-'), '[^a-zA-Z0-9-]', ''
                    )
                )
            )
    end as techspeed_candidate_code,
    phone,
    candidate_id_tier,
    party,
    website_url,
    linkedin_url,
    instagram_handle,
    twitter_handle,
    facebook_url,
    birth_date,
    street_address,
    postal_code,
    district,
    city,
    state,
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    filing_deadline,
    primary_election_date,
    general_election_date,
    election_type,
    uncontested,
    number_of_candidates,
    number_of_seats_available,
    open_seat,
    partisan,
    population,
    ballotready_race_id,
    type,
    contact_owner,
    owner_name,
    uploaded,
    _airbyte_extracted_at
from candidates_w_extracted_last_name
