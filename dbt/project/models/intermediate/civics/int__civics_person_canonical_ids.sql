-- Canonical gp_person_id per record. One row per record_key. The id is minted
-- from the group member earliest by first_seen_at, ties broken by (source_name,
-- source_id): first-in wins uniformly across sources, so the id is stable when a
-- later record (e.g. a BR row) joins a group minted from a gp_api user.
-- first_seen_at is computed inline where each record's native id and source
-- timestamp share a row -- never reconstructed and re-joined (the centralized
-- attempt failed that way: join drift, coverage gaps, inherited filters).
with
    groups as (
        select record_key, source_name, person_group_key
        from {{ ref("int__civics_person_groups") }}
    ),

    -- BallotReady: earliest creation across both S3 feeds, keyed on the person
    -- grain (br_candidate_id).
    br_created as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            candidacy_created_at as created_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union all
        select cast(br_candidate_id as string), office_holder_created_at
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
            'gp_api|' || cast(id as string) as record_key, created_at as first_seen_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    -- createdat (Airbyte top-level) guards a contact whose properties JSON
    -- lacks createdate; both are fully populated today.
    hubspot_first_seen as (
        select
            'hubspot|' || cast(id as string) as record_key,
            coalesce(contact_created_at, created_at) as first_seen_at
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

    -- Fallbacks for vendor records that survive in the stale cluster table but
    -- have no prematch row (~184 ddhq today, 0 techspeed). Keyed lookup first,
    -- so a ddhq record still in staging (e.g. dropped by a prematch filter)
    -- gets its own row's extract time; records absent from staging entirely
    -- fall back to their source's min extract time. All deterministic -- never
    -- current_timestamp. DDHQ ships as one master CSV, so today all its
    -- extract times share one value.
    ddhq_extracts as (
        select
            'ddhq|'
            || cast(candidate_id as string)
            || '_'
            || cast(ddhq_race_id as string) as record_key,
            min(_airbyte_extracted_at) as first_seen_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where candidate_id is not null and ddhq_race_id is not null
        group by 1
    ),

    ddhq_load_date as (
        select min(_airbyte_extracted_at) as first_seen_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
    ),

    -- No keyed variant for techspeed: its source_id embeds a generated
    -- candidate_code, so a keyed lookup would mean reconstructing the key.
    techspeed_load_date as (
        select min(_airbyte_extracted_at) as first_seen_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
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
                de.first_seen_at,
                case when g.source_name = 'ddhq' then dl.first_seen_at end,
                case when g.source_name = 'techspeed' then tl.first_seen_at end
            ) as first_seen_at
        from groups as g
        left join first_seen as fs using (record_key)
        left join ddhq_extracts as de using (record_key)
        cross join ddhq_load_date as dl
        cross join techspeed_load_date as tl
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
