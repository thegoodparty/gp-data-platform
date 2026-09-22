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
        register. Normalization lives in dsar_normalize so every caller matches the
        register the same way; a caller that normalized differently would silently
        fail to suppress.
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


{% macro dsar_none_suppressed(array_expression, identifier_type) %}
    {#-
        The array form of dsar_not_suppressed: keeps a row only when no element of
        the array is in the register. For sources that carry a list of contacts
        rather than one column, checking only the element a model happens to pick
        leaves the rest of the list to flow downstream unsuppressed.

        Nulls are stripped from both arrays first, because arrays_overlap returns
        null rather than false when either side contains a null and nothing matches,
        and `not null` would drop the row. A null or empty list overlaps nothing.
    -#}
    {%- set normalized_elements = (
        "filter(transform(coalesce("
        ~ array_expression
        ~ ", array()), x -> "
        ~ dsar_normalize("x", identifier_type)
        ~ "), x -> x is not null)"
    ) -%}
    (
        not arrays_overlap(
            {{ normalized_elements }},
            (
                select coalesce(collect_list(identifier_value), array())
                from ({{ dsar_register_values(identifier_type) }})
            )
        )
    )
{% endmacro %}
