{% macro generate_gp_election_id(table_alias="", is_special_expr="false") %}
    {#-
      Hashes office attributes + election year into a stable election ID.
      When is_special_expr is provided and evaluates true at the row level,
      the year field is suffixed with '_special' so a special election and
      a regular election in the same office+year produce distinct IDs.
      The literal 'false' default short-circuits at parse time, so the
      generated SQL stays unchanged at callsites without a special signal.
    -#}
    {% set prefix = table_alias ~ "." if table_alias else "" %}
    {% set year_field %}
        coalesce(try_cast(year({{ prefix }}election_date) as string), '')
        {%- if is_special_expr != "false" %}
            || case when {{ is_special_expr }} then '_special' else '' end
        {%- endif %}
    {% endset %}
    {{
        generate_salted_uuid(
            fields=[
                "coalesce(" ~ prefix ~ "official_office_name, '')",
                "coalesce(" ~ prefix ~ "candidate_office, '')",
                "coalesce(" ~ prefix ~ "office_level, '')",
                "coalesce(" ~ prefix ~ "office_type, '')",
                "coalesce(" ~ prefix ~ "state, '')",
                "coalesce(" ~ prefix ~ "city, '')",
                "coalesce(" ~ prefix ~ "district, '')",
                "coalesce(" ~ prefix ~ "seat_name, '')",
                year_field,
                "coalesce(try_cast(" ~ prefix ~ "seats_available as string), '')",
            ]
        )
    }}
{% endmacro %}
