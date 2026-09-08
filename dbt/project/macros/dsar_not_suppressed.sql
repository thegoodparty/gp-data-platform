{% macro dsar_not_suppressed(column_name, identifier_type) %}
    {#-
        Keeps a row only when its identifier is absent from the DSAR suppression
        register. Normalization lives here so every caller matches the register the
        same way its CHECK constraints enforce on insert; a caller that normalized
        differently would silently fail to suppress.

        lalvoterid is deliberately not an allowed type. The L2 voter file is out of
        scope for deletion, so a suppression entry for one must not exist.
    -#}
    {%- set known_types = [
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
    ] -%}
    {%- if identifier_type not in known_types -%}
        {{
            exceptions.raise_compiler_error(
                "dsar_not_suppressed: unknown identifier_type '"
                ~ identifier_type
                ~ "'. Allowed: "
                ~ known_types
                | join(", ")
            )
        }}
    {%- endif -%}

    {%- if identifier_type == "email" -%}
        {%- set normalized = "lower(trim(" ~ column_name ~ "))" -%}
    {%- elif identifier_type == "phone" -%}
        {%- set normalized = "regexp_replace(" ~ column_name ~ ", '[^0-9]', '')" -%}
    {%- else -%} {%- set normalized = "trim(cast(" ~ column_name ~ " as string))" -%}
    {%- endif -%}

    coalesce({{ normalized }}, '') not in (
        select identifier_value
        from {{ ref("stg_source_dsar__suppressed_identifiers") }}
        where identifier_type = '{{ identifier_type }}'
    )
{% endmacro %}
