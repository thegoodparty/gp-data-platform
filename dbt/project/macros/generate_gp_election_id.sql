{% macro generate_gp_election_id(table_alias="") %}
    {% set prefix = table_alias ~ "." if table_alias else "" %}
    {{
        generate_salted_uuid(
            fields=[
                prefix ~ "official_office_name",
                prefix ~ "candidate_office",
                prefix ~ "office_level",
                prefix ~ "office_type",
                prefix ~ "state",
                prefix ~ "city",
                prefix ~ "district",
                prefix ~ "seat_name",
                prefix ~ "election_date",
            ]
        )
    }}
{% endmacro %}
