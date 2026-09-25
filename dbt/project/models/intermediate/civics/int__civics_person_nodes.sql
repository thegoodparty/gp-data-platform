-- Person record universe: one row per record_key participating in person
-- identity. Two things go in: every record the sources hold, so a person with
-- no link is still a node, and every endpoint int__civics_person_links emits.
-- matcha closes over the links across exactly this set and the mint falls
-- back to it for any record a published vintage has not seen yet.
--
-- Reading the link endpoints directly, rather than re-deriving the same joins,
-- keeps the invariant structural: an endpoint missing here would be a record
-- matcha links but the mint never sees.
with
    clustered as (
        select source_id, source_name
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    record_keys as (
        select 'ballotready|' || cast(br_candidate_id as string) as record_key
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union
        select 'ballotready|' || cast(br_candidate_id as string)
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
        union
        select 'gp_api|' || cast(id as string)
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        union
        select 'hubspot|' || cast(id as string)
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        union
        select 'techspeed_officeholder|' || cast(ts_officeholder_id as string)
        from {{ ref("int__civics_elected_official_canonical_ids") }}
        where not ts_officeholder_id_is_reused
        union
        select source_name || '|' || source_id
        from clustered
        where source_name in ('techspeed', 'ddhq')
        union
        select record_key_1
        from {{ ref("int__civics_person_links") }}
        union
        select record_key_2
        from {{ ref("int__civics_person_links") }}
    )

select record_key, substring_index(record_key, '|', 1) as source_name
from record_keys
