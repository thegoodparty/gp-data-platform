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
    {#-
        Applied to both sides of every register comparison. Phones drop a leading US
        country code when ten digits remain.
    -#}
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
        records, where a phone is the office's and shared.

        Anti-join shape: it plans as a hash join with the normalized column computed
        once per row, where `not in` planned as a nested loop. A null identifier never
        equals anything, so it passes.
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


{% macro dsar_not_suppressed_via_br_office_holder(office_holder_id_column) %}
    {#-
        TechSpeed's office_holder_id is BallotReady's office holder record id. Reads
        the raw source; the staging model hides exactly the rows this needs.
    -#}
    not exists (
        select 1
        from
            {{ source("airbyte_source", "ballotready_s3_office_holders_v3") }}
            as suppressed
        where
            cast(suppressed.id as int) = cast({{ office_holder_id_column }} as int)
            and {{
                dsar_br_registered(
                    "suppressed.candidate_id", "suppressed.candidacy_id"
                )
            }}
    )
{% endmacro %}


{% macro dsar_not_suppressed_via_br_candidacy(
    race_id_column, first_name_column, last_name_column
) %}
    {#-
        TechSpeed carries no person id; within a BallotReady race, a name is one
        candidacy. The inner alias is qualified on every column so the caller's
        unqualified outer columns are never shadowed.
    -#}
    not exists (
        select 1
        from
            {{ source("airbyte_source", "ballotready_s3_candidacies_v3") }}
            as suppressed
        where
            cast(suppressed.race_id as string) = cast({{ race_id_column }} as string)
            and lower(trim(suppressed.first_name))
            = lower(trim({{ first_name_column }}))
            and lower(trim(suppressed.last_name)) = lower(trim({{ last_name_column }}))
            and {{
                dsar_br_registered(
                    "suppressed.candidate_id", "suppressed.candidacy_id"
                )
            }}
    )
{% endmacro %}


{% macro segment_staging(
    source_name, table_name, identifiers=[["user_id", "gp_api_user_id"]]
) %}
    {#-
        A Segment staging model: the source, filtered on every identifier that
        belongs to one person there. Segment's warehouse sync re-ingests daily.
    -#}
    select *
    from {{ source(source_name, table_name) }}
    where
        {%- for column_name, identifier_type in identifiers %}
            {{ "and " if not loop.first }}
            {{ dsar_not_suppressed(column_name, identifier_type) }}
        {%- endfor %}
{% endmacro %}
