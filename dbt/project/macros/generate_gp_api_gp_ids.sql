-- Product DB (gp_api) gp_* hashing helpers.
-- These wrap generate_salted_uuid with the exact field sets consumed by
-- int__civics_*_gp_api so we don't repeat the column lists across
-- (a) fallback values after an ER xw miss, (b) window partitions used for
-- cross-row cascade propagation, and (c) sibling models that must compute
-- identical hashes to preserve referential integrity across the four gp_api
-- intermediates.
--
-- Arguments default to the unprefixed column names used in
-- int__civics_candidate_gp_api. Sibling models that source user fields
-- through a join alias (e.g. int__civics_candidacy_gp_api uses user_* from
-- the campaigns mart) pass overrides rather than renaming columns upstream.
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
