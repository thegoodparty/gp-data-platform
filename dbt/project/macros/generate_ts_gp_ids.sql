-- TechSpeed-specific gp_* hashing helpers.
-- These wrap generate_salted_uuid with the exact field sets consumed by
-- int__civics_*_techspeed and the int__civics_er_canonical_ids crosswalk,
-- so we don't repeat the column lists across (a) fallback values after an
-- xw miss and (b) window partitions used for cross-row cascade propagation.
{% macro generate_ts_gp_candidate_id() %}
    {{
        generate_salted_uuid(
            fields=[
                "first_name",
                "last_name",
                "state_postal_code",
                "cast(birth_date_parsed as string)",
                "email",
                "phone",
            ]
        )
    }}
{% endmacro %}


{% macro generate_ts_gp_candidacy_id() %}
    {{
        generate_salted_uuid(
            fields=[
                "first_name",
                "last_name",
                "state",
                "party",
                "candidate_office",
                "cast(coalesce(general_election_date_parsed, primary_election_date_parsed) as string)",
                "district",
            ]
        )
    }}
{% endmacro %}


{% macro generate_ts_gp_election_stage_id() %}
    {{
        generate_salted_uuid(
            fields=[
                "'techspeed'",
                "state",
                "candidate_office",
                "official_office_name",
                "district",
                "city",
                "cast(stage_election_date as string)",
                "stage_type",
            ]
        )
    }}
{% endmacro %}
