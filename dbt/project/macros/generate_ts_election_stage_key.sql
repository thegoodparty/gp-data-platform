-- TechSpeed election-stage natural key. TS has no native race id, so this
-- attribute hash is the stable per-race key: it is the ER source_id for TS
-- election stages and the self-mint ingredient for unclustered TS races. It is
-- an internal key, not a published id (the published gp_election_stage_id is
-- cluster-minted or self-minted from this key).
{% macro generate_ts_election_stage_key() %}
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
