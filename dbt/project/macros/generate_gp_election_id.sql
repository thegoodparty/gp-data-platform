{% macro generate_gp_election_id(table_alias="", is_special_expr="false") %}
    {#-
      Hashes office attributes + election year into a stable election ID.
      When is_special_expr is provided and evaluates true, the year field is
      suffixed with '_special' so a special election and a regular election
      in the same office+year produce distinct IDs. Default 'false' leaves
      the hash unchanged for callsites that have no special signal (HubSpot
      archive, m_general__*).
    -#}
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
                "coalesce(try_cast(year("
                ~ prefix
                ~ "election_date) as string), '') || case when "
                ~ is_special_expr
                ~ " then '_special' else '' end",
                "coalesce(try_cast(" ~ prefix ~ "seats_available as string), '')",
            ]
        )
    }}
{% endmacro %}
