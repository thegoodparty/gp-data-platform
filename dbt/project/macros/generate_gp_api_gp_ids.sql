-- gp_api gp_* hashing helpers — wrap generate_salted_uuid so the column
-- lists stay in sync across the four int__civics_*_gp_api models. Arg
-- defaults match candidate_gp_api; siblings using aliased user_* columns
-- pass overrides instead of renaming upstream.
{% macro generate_gp_api_gp_candidate_id(
    first_name="first_name",
    last_name="last_name",
    state="state",
    email="email",
    phone="phone_number"
) %}
    -- Slot 4 is the birth_date position in the cross-source person hash
    -- [first_name, last_name, state, birth_date, email, phone]. Product DB
    -- doesn't track birth dates, so we emit null (matches int__civics_candidate_
    -- ballotready, which also has no birth date). HubSpot and TechSpeed put a
    -- real value there. The slot must stay for hashes to converge across
    -- sources — dropping it would shift every subsequent field and break ER.
    {{
        generate_salted_uuid(
            fields=[
                first_name,
                last_name,
                state,
                "cast(null as string)",
                email,
                phone,
            ]
        )
    }}
{% endmacro %}


{% macro generate_gp_api_gp_candidacy_id(
    first_name="first_name",
    last_name="last_name",
    state="state",
    party_affiliation="party_affiliation",
    candidate_office="candidate_office",
    general_election_date="general_election_date",
    district="district"
) %}
    {{
        generate_salted_uuid(
            fields=[
                first_name,
                last_name,
                state,
                party_affiliation,
                candidate_office,
                "cast(" ~ general_election_date ~ " as string)",
                district,
            ]
        )
    }}
{% endmacro %}
