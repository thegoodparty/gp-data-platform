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


{% macro dsar_normalize(column_name, identifier_type) %}
    {#-
        Renders the expression that puts a source column into the register's shape.
        Every reader of the register goes through this, the staging filter and the
        delete operation alike, so a row the filter hides is the same row the
        operation removes. The CHECK constraints on the register enforce the same
        shape on insert.
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

    {%- if identifier_type == "email" -%} lower(trim({{ column_name }}))
    {%- elif identifier_type == "phone" -%}
        regexp_replace({{ column_name }}, '[^0-9]', '')
    {%- else -%} trim(cast({{ column_name }} as string))
    {%- endif -%}
{% endmacro %}


{% macro dsar_not_suppressed(column_name, identifier_type) %}
    {#-
        Keeps a row only when its identifier is absent from the DSAR suppression
        register. Normalization lives in dsar_normalize so every caller matches the
        register the same way its CHECK constraints enforce on insert; a caller that
        normalized differently would silently fail to suppress.
    -#}
    {%- set normalized = dsar_normalize(column_name, identifier_type) -%}

    {#-
        Parenthesized as a whole: callers chain these with `and`, and a bare `or`
        would rebind across the adjacent predicate. A null identifier is nothing to
        match on, so it always passes rather than colliding with a blank register entry.
    -#}
    (
        {{ normalized }} is null
        or {{ normalized }} not in (
            select identifier_value
            from {{ ref("stg_source_dsar__suppressed_identifiers") }}
            where
                identifier_type = '{{ identifier_type }}'
                and identifier_value is not null
        )
    )
{% endmacro %}
