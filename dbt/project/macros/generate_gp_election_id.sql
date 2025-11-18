{% macro generate_gp_election_id(table_alias="") %}
    {% set prefix = table_alias ~ "." if table_alias else "" %}
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
                "coalesce(" ~ prefix ~ "election_date, '')",
            ]
        )
    }}
{% endmacro %}
