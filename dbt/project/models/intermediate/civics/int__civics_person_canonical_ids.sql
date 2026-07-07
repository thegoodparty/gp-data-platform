-- Canonical gp_person_id per record. One row per record_key. The id is minted
-- from the group member earliest by first_seen_at, ties broken by (source_name,
-- source_id): first-in wins uniformly across sources, so the id is stable when a
-- later record (e.g. a BR row) joins a group minted from a gp_api user.
-- first_seen_at is computed inline where each record's native id and source
-- timestamp share a row -- never reconstructed and re-joined (the centralized
-- attempt failed that way). See canonical-person-plan.md decision 2.
with
    groups as (
        select record_key, source_name, person_group_key
        from {{ ref("int__civics_person_groups") }}
    ),

    -- BallotReady: earliest creation across both S3 feeds, keyed on the person
    -- grain (br_candidate_id). Cast because candidacy_created_at is un-cast in
    -- staging and must union with the timestamp office_holder_created_at.
    br_created as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            cast(candidacy_created_at as timestamp) as created_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union all
        select
            cast(br_candidate_id as string), cast(office_holder_created_at as timestamp)
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
    ),

    br_first_seen as (
        select
            'ballotready|' || br_candidate_id as record_key,
            min(created_at) as first_seen_at
        from br_created
        group by 1
    ),

    gp_api_first_seen as (
        select
            'gp_api|' || cast(id as string) as record_key,
            cast(created_at as timestamp) as first_seen_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    hubspot_first_seen as (
        select
            'hubspot|' || cast(id as string) as record_key,
            contact_created_at as first_seen_at
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    -- techspeed_officeholder: earliest parsed processing date for the id, with
    -- the airbyte extract as a deterministic fallback for unparsed dates.
    ts_officeholder_first_seen as (
        select
            'techspeed_officeholder|'
            || cast(ts_officeholder_id as string) as record_key,
            min(
                coalesce(cast(date_processed_date as timestamp), _airbyte_extracted_at)
            ) as first_seen_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        where ts_officeholder_id is not null
        group by 1
    ),

    -- techspeed / ddhq candidacy records: first_seen_at rides the prematch row
    -- where source_id is built, so unique_id equals record_key (a lookup, not a
    -- reconstruction).
    vendor_first_seen as (
        select unique_id as record_key, first_seen_at
        from {{ ref("int__er_prematch_candidacy_stages") }}
        where source_name in ('techspeed', 'ddhq')
    ),

    -- Deterministic fallback for the ~184 ddhq records that survive in the
    -- stale cluster table but whose source rows have churned out of both the
    -- prematch and ddhq staging. DDHQ ships as a single CSV with one shared
    -- extract time (finding 5), so this is the exact value in-prematch ddhq
    -- rows already carry -- full-refresh safe, never current_timestamp.
    ddhq_load_date as (
        select min(_airbyte_extracted_at) as first_seen_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
    ),

    first_seen as (
        select *
        from br_first_seen
        union all
        select *
        from gp_api_first_seen
        union all
        select *
        from hubspot_first_seen
        union all
        select *
        from ts_officeholder_first_seen
        union all
        select *
        from vendor_first_seen
    ),

    records as (
        select
            g.record_key,
            g.source_name,
            g.person_group_key,
            substring_index(g.record_key, '|', -1) as source_id,
            coalesce(
                fs.first_seen_at,
                case when g.source_name = 'ddhq' then dl.first_seen_at end
            ) as first_seen_at
        from groups as g
        left join first_seen as fs using (record_key)
        cross join ddhq_load_date as dl
    ),

    -- Earliest member mints the id. nulls last keeps a stray missing timestamp
    -- from hijacking a mint; the first_seen_at not_null test surfaces the gap.
    minting_member as (
        select
            person_group_key,
            source_name as minting_source_name,
            source_id as minting_source_id,
            count(*) over (partition by person_group_key) as group_size
        from records
        qualify
            row_number() over (
                partition by person_group_key
                order by first_seen_at asc nulls last, source_name asc, source_id asc
            )
            = 1
    )

select
    r.record_key,
    r.source_name,
    r.person_group_key,
    r.first_seen_at,
    m.minting_source_name,
    m.minting_source_id,
    m.group_size,
    {{
        generate_salted_uuid(
            fields=["m.minting_source_name", "m.minting_source_id"], salt="person"
        )
    }} as gp_person_id
from records as r
inner join minting_member as m using (person_group_key)
