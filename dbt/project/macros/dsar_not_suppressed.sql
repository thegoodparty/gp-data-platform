{% macro dsar_identifier_types() %}
    {#-
        The identifier types the suppression register may hold. lalvoterid is
        deliberately absent: the L2 voter file is out of scope for deletion, so a
        suppression entry for one must not exist.
    -#}
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
        Renders the expression that puts a value into the register's comparable
        shape. Both sides of every register comparison go through this, the source
        column and the register value alike, so a formatting difference between
        intake and source can never undo a suppression.

        Phones: digits only, then a leading US country code is dropped when what
        remains is a ten-digit number. `+1 (202) 555-0100` and `(202) 555-0100` both
        become `2025550100`; a genuinely international number keeps its digits.
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
        regexp_replace(
            regexp_replace({{ expression }}, '[^0-9]', ''), '^1([0-9]{10})$', '$1'
        )
    {%- else -%} trim(cast({{ expression }} as string))
    {%- endif -%}
{% endmacro %}


{% macro dsar_register_values(identifier_type) %}
    {#- The register's values of one type, normalized the same way as the source side. -#}
    select {{ dsar_normalize("identifier_value", identifier_type) }} as identifier_value
    from {{ ref("stg_source_dsar__suppressed_identifiers") }}
    where identifier_type = '{{ identifier_type }}' and identifier_value is not null
{% endmacro %}


{% macro dsar_not_suppressed(column_name, identifier_type) %}
    {#-
        Keeps a row only when its identifier is absent from the DSAR suppression
        register.

        Which identifier a model checks follows one rule: the one that belongs to
        exactly one person in that source. First-party sources (gp-api, HubSpot)
        check their record id and the personal email and phone. Vendor civic records
        check the vendor's id only, never a contact field: a councilmember's office
        phone is the whole council's phone, and an email or phone is shared by
        more than one BallotReady officeholder in a third of the file.
    -#}
    {%- set normalized = dsar_normalize(column_name, identifier_type) -%}

    {#-
        Parenthesized as a whole: callers chain these with `and`, and a bare `or`
        would rebind across the adjacent predicate. A null identifier is nothing to
        match on, so it always passes rather than colliding with a blank register entry.
    -#}
    (
        {{ normalized }} is null
        or {{ normalized }} not in ({{ dsar_register_values(identifier_type) }})
    )
{% endmacro %}


{% macro dsar_br_suppressed_office_holder_ids() %}
    {#-
        BallotReady office holder record ids belonging to a registered person or
        candidacy. Read from the raw source, not the filtered staging model, which
        would hide exactly the rows this needs to find.
    -#}
    select cast(id as int) as office_holder_id
    from {{ source("airbyte_source", "ballotready_s3_office_holders_v3") }}
    where
        -- A null in a NOT IN list makes the whole predicate unknown.
        cast(id as int) is not null
        and (
            {{ dsar_normalize("candidate_id", "br_person_id") }}
            in ({{ dsar_register_values("br_person_id") }})
            or {{ dsar_normalize("candidacy_id", "br_candidacy_id") }}
            in ({{ dsar_register_values("br_candidacy_id") }})
        )
{% endmacro %}


{% macro dsar_br_suppressed_candidacy_keys() %}
    {#-
        The (race, first name, last name) keys of a registered person's BallotReady
        candidacies. TechSpeed's candidate feed carries no person id, but every row
        carries the BallotReady race id and the name, and within one race a name is
        one candidacy. Read from the raw source for the same reason as above.
    -#}
    {#-
        Column names deliberately differ from any source column. The caller
        correlates unqualified outer columns into this subquery, and an inner column
        of the same name would shadow them and compare the key list to itself.
    -#}
    select
        cast(race_id as string) as suppressed_race_id,
        lower(trim(first_name)) as suppressed_first_name,
        lower(trim(last_name)) as suppressed_last_name
    from {{ source("airbyte_source", "ballotready_s3_candidacies_v3") }}
    where
        race_id is not null
        and first_name is not null
        and last_name is not null
        and (
            {{ dsar_normalize("candidate_id", "br_person_id") }}
            in ({{ dsar_register_values("br_person_id") }})
            or {{ dsar_normalize("candidacy_id", "br_candidacy_id") }}
            in ({{ dsar_register_values("br_candidacy_id") }})
        )
{% endmacro %}


{% macro dsar_not_suppressed_via_br_office_holder(office_holder_id_column) %}
    {#-
        For TechSpeed's officeholder feed, whose office_holder_id is BallotReady's
        office holder record id on every row. A registered BallotReady person or
        candidacy resolves to those ids and removes the TechSpeed rows exactly.
    -#}
    (
        cast({{ office_holder_id_column }} as int) is null
        or cast({{ office_holder_id_column }} as int)
        not in ({{ dsar_br_suppressed_office_holder_ids() }})
    )
{% endmacro %}


{% macro dsar_not_suppressed_via_br_candidacy(
    race_id_column, first_name_column, last_name_column
) %}
    {#-
        For TechSpeed's candidate feed, keyed on the BallotReady race plus the name.
        The one case this cannot separate is two people with the same first and last
        name in the same race, which is accepted and documented in the runbook.
    -#}
    not exists (
        select 1
        from ({{ dsar_br_suppressed_candidacy_keys() }}) as suppressed
        where
            suppressed.suppressed_race_id = cast({{ race_id_column }} as string)
            and suppressed.suppressed_first_name = lower(trim({{ first_name_column }}))
            and suppressed.suppressed_last_name = lower(trim({{ last_name_column }}))
    )
{% endmacro %}
