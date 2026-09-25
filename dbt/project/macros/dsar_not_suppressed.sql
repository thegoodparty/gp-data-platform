{% macro dsar_identifier_types() %}
    {#- Only types a staging filter matches on. A type nothing filters would let a
        register row silently do nothing. lalvoterid is absent: L2 is out of scope. -#}
    {{
        return(
            [
                "email",
                "phone",
                "gp_api_user_id",
                "hs_contact_id",
                "br_person_id",
                "br_candidacy_id",
                "ddhq_candidate_id",
            ]
        )
    }}
{% endmacro %}


{% macro dsar_normalize(expression, identifier_type) %}
    {#- Applied to both sides of every register comparison. -#}
    {%- if identifier_type not in dsar_identifier_types() -%}
        {{
            exceptions.raise_compiler_error(
                "dsar_normalize: unknown identifier_type '"
                ~ identifier_type
                ~ "'. Allowed: "
                ~ dsar_identifier_types()
                | join(", ")
            )
        }}
    {%- endif -%}

    {%- if identifier_type == "email" -%} lower(trim({{ expression }}))
    {%- elif identifier_type == "phone" -%}
        regexp_replace({{ clean_phone_number(expression) }}, '^1([0-9]{10})$', '$1')
    {%- else -%} trim(cast({{ expression }} as string))
    {%- endif -%}
{% endmacro %}


{% macro dsar_not_suppressed(column_name, identifier_type) %}
    {#- Anti-join against the register; a null identifier passes. -#}
    not exists (
        select 1
        from {{ ref("stg_source_dsar__suppressed_identifiers") }} as register
        where
            register.identifier_type = '{{ identifier_type }}'
            and {{ dsar_normalize("register.identifier_value", identifier_type) }}
            = {{ dsar_normalize(column_name, identifier_type) }}
    )
{% endmacro %}


{% macro dsar_first_party_not_suppressed(
    id_column, id_type, email_column, phone_column
) %}
    {#- First-party rows carry the person's own contact fields, so those count too. -#}
    {{ dsar_not_suppressed(id_column, id_type) }}
    and {{ dsar_not_suppressed(email_column, "email") }}
    and {{ dsar_not_suppressed(phone_column, "phone") }}
{% endmacro %}


{% macro dsar_br_not_suppressed(person_id_column, candidacy_id_column) %}
    {#- Vendor civic rows match on ids only; their contact fields are the office's. -#}
    {{ dsar_not_suppressed(person_id_column, "br_person_id") }}
    and {{ dsar_not_suppressed(candidacy_id_column, "br_candidacy_id") }}
{% endmacro %}


{#-
    The lookups below wrap their source in a derived table with suppressed_* column
    names. Callers correlate unqualified outer columns in; a same-named inner column
    would shadow them and make the predicate a tautology.
-#}
{% macro dsar_not_suppressed_via_br_office_holder(office_holder_id_column) %}
    {#- TechSpeed's office_holder_id is BallotReady's. -#}
    not exists (
        select 1
        from
            (
                select try_cast(office_holder_id as int) as suppressed_office_holder_id
                from {{ source("airbyte_source", "ballotready_s3_office_holders_v3") }}
                where not ({{ dsar_br_not_suppressed("candidate_id", "candidacy_id") }})
            ) as suppressed
        where
            suppressed.suppressed_office_holder_id
            = try_cast({{ office_holder_id_column }} as int)
    )
{% endmacro %}


{% macro dsar_not_suppressed_via_br_candidacy(
    race_id_column, first_name_column, cleaned_last_name_column
) %}
    {#- TechSpeed carries no person id; within a BallotReady race, a name is one candidacy. -#}
    not exists (
        select 1
        from
            (
                select
                    cast(race_id as string) as suppressed_race_id,
                    lower(trim(first_name)) as suppressed_first_name,
                    lower(
                        {{ clean_techspeed_last_name("last_name") }}
                    ) as suppressed_last_name
                from {{ source("airbyte_source", "ballotready_s3_candidacies_v3") }}
                where
                    race_id is not null
                    and first_name is not null
                    and last_name is not null
                    and not (
                        {{ dsar_br_not_suppressed("candidate_id", "candidacy_id") }}
                    )
            ) as suppressed
        where
            suppressed.suppressed_race_id = cast({{ race_id_column }} as string)
            and suppressed.suppressed_first_name = lower(trim({{ first_name_column }}))
            and suppressed.suppressed_last_name
            = lower(trim({{ cleaned_last_name_column }}))
    )
{% endmacro %}


{% macro hubspot_contact_not_suppressed(contact_id_column) %}
    {#- For models that carry a contact id but not the contact's own fields. A contact
        missing from the source passes; the caller's hs_contact_id predicate covers it. -#}
    not exists (
        select 1
        from
            (
                select id as suppressed_contact_id
                from {{ source("airbyte_source", "hubspot_api_contacts") }}
                where
                    not (
                        {{
                            dsar_first_party_not_suppressed(
                                "id",
                                "hs_contact_id",
                                "get_json_object(properties, '$.email')",
                                "get_json_object(properties, '$.phone')",
                            )
                        }}
                    )
            ) as suppressed
        where suppressed.suppressed_contact_id = {{ contact_id_column }}
    )
{% endmacro %}
