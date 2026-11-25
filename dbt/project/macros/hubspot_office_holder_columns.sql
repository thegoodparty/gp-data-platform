{#-
    The published column list of the Office Holder staging model, as (name, type,
    source expression) triples.

    One list drives both branches of the model: the real select projects each
    expression under its name, and the branch for a loader that has never landed a
    complete run emits a typed null under the same name. Two hand-written column lists
    would drift, and a drift shows up only as a broken build or, worse, a column quietly
    reading the wrong property.
-#}
{% macro hubspot_office_holder_columns() %}
    {%- set columns = [
        ("record_id", "string", "record_id"),
        ("br_office_holder_id", "string", "br_office_holder_id"),
        ("br_position_id", "string", "br_position_id"),
        ("candidate_office", "string", "candidate_office"),
        ("city", "string", "city"),
        ("district", "string", "district"),
        ("elected_date", "date", "try_cast(elected_date as date)"),
        (
            "gp_api_elected_office_id",
            "string",
            "nullif(trim(gp_api_elected_office_id), '')",
        ),
        ("gp_candidacy_id", "string", "gp_candidacy_id"),
        (
            "gp_elected_official_term_id",
            "string",
            "nullif(trim(gp_elected_official_term_id), '')",
        ),
        ("gp_person_id", "string", "gp_person_id"),
        ("is_appointed", "boolean", cast_to_boolean("is_appointed")),
        ("is_judicial", "boolean", cast_to_boolean("is_judicial")),
        ("is_serve_icp", "boolean", cast_to_boolean("is_serve_icp")),
        ("is_vacant", "boolean", cast_to_boolean("is_vacant")),
        ("is_win_icp", "boolean", cast_to_boolean("is_win_icp")),
        ("name", "string", "name"),
        ("office_email", "string", "office_email"),
        ("office_level", "string", "office_level"),
        ("office_phone", "string", "office_phone"),
        ("office_type", "string", "office_type"),
        (
            "onboarding_completed_at",
            "timestamp",
            "try_cast(onboarding_completed_at as timestamp)",
        ),
        ("party_affiliation", "string", "party_affiliation"),
        ("pledged_at", "timestamp", "try_cast(pledged_at as timestamp)"),
        ("position_name", "string", "position_name"),
        ("self_reported", "boolean", cast_to_boolean("self_reported")),
        ("source_systems", "string", "source_systems"),
        ("state", "string", "state"),
        ("status", "string", "status"),
        ("sworn_in_date", "date", "try_cast(sworn_in_date as date)"),
        ("term_end_date", "date", "try_cast(term_end_date as date)"),
        ("term_start_date", "date", "try_cast(term_start_date as date)"),
        ("hubspot_owner_id", "string", "hubspot_owner_id"),
        ("created_at", "timestamp", "try_cast(hs_createdate as timestamp)"),
        (
            "updated_at",
            "timestamp",
            "try_cast(hs_lastmodifieddate as timestamp)",
        ),
        (
            "contact_ids",
            "array<string>",
            "from_json(contact_ids, 'array<string>')",
        ),
        (
            "company_ids",
            "array<string>",
            "from_json(company_ids, 'array<string>')",
        ),
        ("origin", "string", "origin"),
    ] -%}
    {{ return(columns) }}
{% endmacro %}
