{#- A table, unlike the other gp_api pass-through views: the shaping below runs
    once per build instead of on every app query. -#}
{{ config(materialized="table", auto_liquid_cluster=True) }}

{#- Staging columns folded into a voter-contract column below, or replaced by a raw
    identifier, so the trailing commercial-only columns do not repeat them. -#}
{%- set folded = [
    "state",
    "inferred_age",
    "gender",
    "business_owner",
    "education_of_person",
    "estimated_income_code",
    "homeowner_probability_model",
    "language_code",
    "marital_status",
    "presence_of_children_in_hh",
    "veteran",
    "lat_long_geo_rc_code",
    "latitude",
    "longitude",
    "city_name",
    "zip_code",
    "zip4",
    "unit_designator",
    "county_name",
    "county_code",
    "district_congressional",
    "district_state_senate",
    "district_state_house",
    "district_legislative",
    "cell_phone_confidencecode",
    "landline_phone_confidencecode",
    "individual_id_hash",
    "address_id_hash",
    "family_id_hash",
    "lalvoterid_hash",
    "maid1_hash",
    "maid2_hash",
    "maid3_hash",
    "maid4_hash",
    "maid5_hash",
] -%}
{%- set label_fields = [
    "Business_Owner",
    "Education_Of_Person",
    "Estimated_Income_Amount",
    "Ethnic_Description",
    "EthnicGroups_EthnicGroup1Desc",
    "Homeowner_Probability_Model",
    "Language_Code",
    "Marital_Status",
    "Residence_Addresses_LatLongAccuracy",
] -%}

with
    commercial as (
        select * from {{ ref("stg_l2_commercial__l2_commercial_20260912_mi") }}
    ),
    identifiers as (
        select *
        from {{ ref("stg_l2_commercial__l2_commercial_20260912_mi_identifiers") }}
    ),
    labels as (
        select
            {%- for field in label_fields %}
                map_from_entries(
                    collect_list(struct(code, label)) filter (
                        where field = '{{ field }}'
                    )
                ) as {{ field | lower }}_labels
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ ref("l2_commercial_code_labels") }}
    )

select
    {{
        generate_salted_uuid(
            fields=["identifiers.individual_id"], salt="l2_commercial"
        )
    }} as id,
    identifiers.lalvoterid as `LALVOTERID`,
    commercial.state as `State`,
    -- L2 writes 0 when the age is unknown; the file holds no one under 18.
    cast(nullif(commercial.inferred_age, 0) as string) as `Age`,
    nullif(commercial.inferred_age, 0) as `Age_Int`,
    -- The voter file is mixed case; the commercial file is upper case.
    initcap(identifiers.fname) as `FirstName`,
    initcap(identifiers.mname) as `MiddleName`,
    initcap(identifiers.lname) as `LastName`,
    commercial.gender as `Gender`,
    try_element_at(
        labels.business_owner_labels, commercial.business_owner
    ) as `Business_Owner`,
    try_element_at(
        labels.education_of_person_labels, commercial.education_of_person
    ) as `Education_Of_Person`,
    try_element_at(
        labels.estimated_income_amount_labels, commercial.estimated_income_code
    ) as `Estimated_Income_Amount`,
    cast(
        regexp_replace(
            try_element_at(
                labels.estimated_income_amount_labels, commercial.estimated_income_code
            ),
            '[^0-9.]',
            ''
        ) as int
    ) as `Estimated_Income_Amount_Int`,
    try_element_at(
        labels.ethnicgroups_ethnicgroup1desc_labels, commercial.ethnic_code
    ) as `EthnicGroups_EthnicGroup1Desc`,
    try_element_at(
        labels.ethnic_description_labels, commercial.ethnic_code
    ) as `Ethnic_Description`,
    try_element_at(
        labels.homeowner_probability_model_labels,
        commercial.homeowner_probability_model
    ) as `Homeowner_Probability_Model`,
    try_element_at(
        labels.language_code_labels, commercial.language_code
    ) as `Language_Code`,
    -- The commercial file has one CASS-certified postal address, so it fills both
    -- the mailing and residence blocks.
    {%- for prefix in ["Mailing_Addresses", "Residence_Addresses"] %}
        -- initcap lowers two-letter directionals, which the voter file keeps upper.
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        initcap(
                            concat_ws(
                                ' ',
                                identifiers.primary_address,
                                identifiers.secondary_address
                            )
                        ),
                        '\\bNe\\b',
                        'NE'
                    ),
                    '\\bNw\\b',
                    'NW'
                ),
                '\\bSe\\b',
                'SE'
            ),
            '\\bSw\\b',
            'SW'
        ) as `{{ prefix }}_AddressLine`,
        identifiers.unit_designator_number as `{{ prefix }}_ApartmentNum`,
        initcap(commercial.unit_designator) as `{{ prefix }}_ApartmentType`,
        -- Delivery point barcode (2 digits) followed by its check digit.
        try_cast(
            substr(identifiers.delivery_point_check_digit, 3, 1) as int
        ) as `{{ prefix }}_CheckDigit`,
        initcap(commercial.city_name) as `{{ prefix }}_City`,
        initcap(identifiers.street_suffix) as `{{ prefix }}_Designator`,
        substr(identifiers.delivery_point_check_digit, 1, 2) as `{{ prefix }}_DPBC`,
        identifiers.house_number as `{{ prefix }}_HouseNumber`,
        {%- if prefix == "Residence_Addresses" %}
            try_element_at(
                labels.residence_addresses_latlongaccuracy_labels,
                commercial.lat_long_geo_rc_code
            ) as `Residence_Addresses_LatLongAccuracy`,
            identifiers.latitude as `Residence_Addresses_Latitude`,
            identifiers.longitude as `Residence_Addresses_Longitude`,
        {%- endif %}
        identifiers.pre_direction as `{{ prefix }}_PrefixDirection`,
        commercial.state as `{{ prefix }}_State`,
        initcap(identifiers.street_name) as `{{ prefix }}_StreetName`,
        identifiers.post_direction as `{{ prefix }}_SuffixDirection`,
        commercial.zip_code as `{{ prefix }}_Zip`,
        commercial.zip4 as `{{ prefix }}_ZipPlus4`,
    {%- endfor %}
    try_element_at(
        labels.marital_status_labels, commercial.marital_status
    ) as `Marital_Status`,
    identifiers.name_suffix as `NameSuffix`,
    case
        when commercial.presence_of_children_in_hh
        then 'Y'
        when not commercial.presence_of_children_in_hh
        then 'N'
    end as `Presence_Of_Children`,
    case when commercial.veteran then 'Yes' end as `Veteran_Status`,
    try_cast(
        commercial.cell_phone_confidencecode as int
    ) as `VoterTelephones_CellConfidenceCode`,
    {{ clean_l2_phone_number("identifiers.cell_phone") }}
    as `VoterTelephones_CellPhoneFormatted`,
    try_cast(
        commercial.landline_phone_confidencecode as int
    ) as `VoterTelephones_LandlineConfidenceCode`,
    -- L2 documents 7 digits, but the file carries the area code too.
    {{ clean_l2_phone_number("identifiers.landline_phone_number") }}
    as `VoterTelephones_LandlineFormatted`,
    -- The voter file writes district numbers without leading zeros.
    upper(commercial.county_name) as `County`,
    regexp_replace(
        commercial.district_state_house, '^0+(?=.)', ''
    ) as `State_House_District`,
    regexp_replace(
        commercial.district_legislative, '^0+(?=.)', ''
    ) as `State_Legislative_District`,
    regexp_replace(
        commercial.district_state_senate, '^0+(?=.)', ''
    ) as `State_Senate_District`,
    regexp_replace(
        commercial.district_congressional, '^0+(?=.)', ''
    ) as `US_Congressional_District`,
    commercial.county_code as `FIPS`,
    identifiers.individual_id,
    identifiers.address_id,
    identifiers.family_id,
    commercial.* except ({{ folded | join(", ") }})
from commercial
inner join identifiers on commercial.individual_id_hash = identifiers.individual_id_hash
cross join labels
