{% macro dsar_identifier_types() %}
    {#- lalvoterid is deliberately absent: the L2 voter file is out of scope. -#}
    {{
        return(
            [
                "email",
                "phone",
                "gp_api_user_id",
                "gp_person_id",
                "clerk_id",
                "stripe_customer_id",
                "hs_contact_id",
                "br_person_id",
                "br_candidacy_id",
                "ddhq_candidate_id",
                "ts_candidate_code",
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
    {#-
        Keeps a row whose identifier is absent from the register. Match on the
        identifier that belongs to exactly one person in the source: record ids and
        personal contact fields for first-party sources, vendor ids only for civic
        records. Anti-join shape; a null identifier passes.
    -#}
    not exists (
        select 1
        from {{ ref("stg_source_dsar__suppressed_identifiers") }} as register
        where
            register.identifier_type = '{{ identifier_type }}'
            and {{ dsar_normalize("register.identifier_value", identifier_type) }}
            = {{ dsar_normalize(column_name, identifier_type) }}
    )
{% endmacro %}


{% macro dsar_br_registered(person_id_column, candidacy_id_column) %}
    {#- A BallotReady row whose person or candidacy is in the register. -#}
    not (
        {{ dsar_not_suppressed(person_id_column, "br_person_id") }}
        and {{ dsar_not_suppressed(candidacy_id_column, "br_candidacy_id") }}
    )
{% endmacro %}


{#-
    The two BallotReady lookups below wrap the raw source in a derived table whose
    columns are named suppressed_*. Callers correlate unqualified outer columns into
    the subquery; an inner column with the same name (first_name, last_name, id)
    would shadow them and turn the predicate into a tautology.
-#}
{% macro dsar_not_suppressed_via_br_office_holder(office_holder_id_column) %}
    {#- TechSpeed's office_holder_id is BallotReady's office_holder_id. -#}
    not exists (
        select 1
        from
            (
                select try_cast(office_holder_id as int) as suppressed_office_holder_id
                from {{ source("airbyte_source", "ballotready_s3_office_holders_v3") }}
                where {{ dsar_br_registered("candidate_id", "candidacy_id") }}
            ) as suppressed
        where
            suppressed.suppressed_office_holder_id
            = try_cast({{ office_holder_id_column }} as int)
    )
{% endmacro %}


{% macro dsar_not_suppressed_via_br_candidacy(
    race_id_column, first_name_column, last_name_column
) %}
    {#- TechSpeed carries no person id; within a BallotReady race, a name is one candidacy. -#}
    not exists (
        select 1
        from
            (
                {#- Both sides cleaned the same way, so a suffix on either does not break the match. -#}
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
                    and {{ dsar_br_registered("candidate_id", "candidacy_id") }}
            ) as suppressed
        where
            suppressed.suppressed_race_id = cast({{ race_id_column }} as string)
            and suppressed.suppressed_first_name = lower(trim({{ first_name_column }}))
            and suppressed.suppressed_last_name = lower(trim({{ last_name_column }}))
    )
{% endmacro %}


{% macro hubspot_contact_not_suppressed(contact_id_column) %}
    {#-
        For models that carry a HubSpot contact id but not the contact's own fields:
        the row goes when the contact itself is suppressed by any identifier, so a
        phone-only registration removes the contact's submissions along with the
        contact. A missing contact row (already deleted in HubSpot) passes here and is
        caught by the caller's own hs_contact_id predicate.
    -#}
    not exists (
        select 1
        from
            (
                {#- Renamed so the caller's outer `properties` or `id` is never shadowed. -#}
                select id as suppressed_contact_id
                from {{ source("airbyte_source", "hubspot_api_contacts") }}
                where
                    not (
                        {{ dsar_not_suppressed("id", "hs_contact_id") }}
                        and {{
                            dsar_not_suppressed(
                                "get_json_object(properties, '$.email')", "email"
                            )
                        }}
                        and {{
                            dsar_not_suppressed(
                                "get_json_object(properties, '$.phone')", "phone"
                            )
                        }}
                    )
            ) as suppressed
        where suppressed.suppressed_contact_id = {{ contact_id_column }}
    )
{% endmacro %}


{% macro segment_staging(
    source_name, table_name, identifiers=[["user_id", "gp_api_user_id"]]
) %}
    {#- A Segment staging model: the source, filtered on each identifier that belongs to one person. -#}
    select *
    from {{ source(source_name, table_name) }}
    where
        {%- for column_name, identifier_type in identifiers %}
            {{ "and " if not loop.first }}
            {{ dsar_not_suppressed(column_name, identifier_type) }}
        {%- endfor %}
{% endmacro %}
