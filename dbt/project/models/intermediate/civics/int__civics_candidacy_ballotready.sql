-- BallotReady candidacies → Civics mart candidacy schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
--
-- Grain: One row per candidacy (candidate + position + election year)
--
-- The BallotReady S3 data is at the RACE grain (one row per candidate per stage:
-- primary, general, runoff). We roll up to the CANDIDACY grain by grouping on
-- candidate + position + election, then extracting stage-specific dates.
--
-- Ids are cluster-derived (earliest-member mint): gp_candidacy_id rolls up the
-- stage-grain mint, gp_candidate_id is the person id. Matched TS/DDHQ/gp_api
-- rows adopt these values via int__civics_er_canonical_ids, so cross-source
-- alignment no longer relies on matching attribute hashes.
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    -- Stage-grain cluster mint; stages that never went through ER self-mint
    -- from br_candidacy_id (single-member semantics).
    candidacy_mint as (
        select source_id as br_candidacy_id, minted_gp_candidacy_id
        from {{ ref("int__civics_minted_candidacy_ids") }}
        where source_name = 'ballotready' and cluster_br_members = 1
    ),

    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    br_person as (select * from {{ ref("int__ballotready_person") }}),

    person_emails as (
        select
            database_id as person_database_id,
            get(filter(contacts, x -> x.email is not null), 0).email as api_email
        from br_person
        where database_id is not null
    ),

    candidacies_with_fields as (
        select
            candidacies.*,
            coalesce(
                cm.minted_gp_candidacy_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "'ballotready'",
                            "cast(candidacies.br_candidacy_id as string)",
                        ],
                        salt="candidacy",
                    )
                }}
            ) as stage_minted_candidacy_id,
            person_emails.api_email,
            {{
                generate_candidate_office_from_position(
                    "candidacies.position_name",
                    "candidacies.normalized_position_name",
                )
            }} as candidate_office,
            initcap(candidacies.level) as office_level,
            {{ extract_city_from_office_name("candidacies.position_name") }} as city,
            {{ extract_district_geographic("candidacies.position_name") }} as district,
            coalesce(
                regexp_extract(
                    candidacies.position_name, '[-, ] (?:Seat|Group) ([^,]+)'
                ),
                regexp_extract(candidacies.position_name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,
            br_position.is_partisan,
            -- is_special tracks whether this row belongs to a special election;
            -- it partitions the rollup grain and feeds the gp_election_id hash
            -- so special and regular cycles for the same position+year resolve
            -- to distinct IDs (matching int__civics_election_stage_ballotready).
            coalesce(
                lower(candidacies.election_name) like '%special%', false
            ) as is_special
        from candidacies
        left join
            candidacy_mint as cm
            on cast(candidacies.br_candidacy_id as string) = cm.br_candidacy_id
        left join br_position on candidacies.br_position_id = br_position.database_id
        left join
            person_emails
            on candidacies.br_candidate_id = person_emails.person_database_id
    ),

    -- Group by candidate + position + election YEAR (not br_election_id, which
    -- differs between primary and general stages in BallotReady data).
    -- is_special is derived per candidacy via bool_or so the lookup join and
    -- gp_election_id hash can distinguish special vs regular cycles without
    -- splitting the rollup grain on data-quality-fragile signals (some BR
    -- stages have NULL or inconsistent election_name).
    candidacy_rolled_up as (
        select
            br_candidate_id,
            br_position_id,
            year(election_day) as election_year,
            bool_or(is_special) as is_special,

            -- Candidacy-grain rollup of the stage-grain mint: candidacy-stage
            -- clusters are single-date, so the candidacy id is the min minted
            -- id over the candidacy's stages (co-clustered candidacies
            -- converge because they share clusters). Restricted to rows
            -- meeting the ER name/state filter so the same min is
            -- reproducible in int__civics_candidacy_stage_ballotready (which
            -- drops those rows); groups with no qualifying row fall back to
            -- the unrestricted min.
            coalesce(
                min(
                    case
                        when
                            first_name is not null
                            and last_name is not null
                            and state is not null
                        then stage_minted_candidacy_id
                    end
                ),
                min(stage_minted_candidacy_id)
            ) as rolled_gp_candidacy_id,

            -- Take candidate fields from any row (they're the same across stages)
            any_value(first_name) as first_name,
            any_value(last_name) as last_name,
            any_value(state) as state,
            any_value(email) as email,
            any_value(api_email) as api_email,
            any_value(phone) as phone,
            any_value(position_name) as official_office_name,
            any_value(candidate_office) as candidate_office,
            any_value(office_level) as office_level,
            any_value(normalized_position_name) as normalized_position_name,
            any_value(city) as city,
            any_value(district) as district,
            any_value(seat_name) as seat_name,
            any_value(parties) as parties,
            any_value(is_partisan) as is_partisan,
            any_value(number_of_seats) as seats_available,
            any_value(_airbyte_extracted_at) as _airbyte_extracted_at,

            max(
                case when is_primary and not is_runoff then election_day end
            ) as primary_election_date,
            max(
                case when is_primary and is_runoff then election_day end
            ) as primary_runoff_election_date,
            max(
                case when not is_primary and not is_runoff then election_day end
            ) as general_election_date,
            max(
                case when not is_primary and is_runoff then election_day end
            ) as general_runoff_election_date,

            -- The general election result is the canonical candidacy result
            -- Fall back to primary result if no general yet
            coalesce(
                max(
                    case when not is_primary and not is_runoff then election_result end
                ),
                max(case when is_primary then election_result end)
            ) as raw_election_result,

            max(candidacy_updated_at) as candidacy_updated_at,

            -- BallotReady native IDs (one per stage; take any for candidacy grain)
            any_value(br_candidacy_id) as br_candidacy_id,
            any_value(cast(br_race_id as string)) as br_race_id

        from candidacies_with_fields
        group by br_candidate_id, br_position_id, year(election_day)
    ),

    -- Look up the general election date and seats from the election_stage model
    -- (which uses the API race data and has the same is_special-partitioned
    -- year-based general-date lookup). This ensures the candidacy model
    -- computes the same gp_election_id as the election and election_stage
    -- tables, including for special elections.
    general_election_date_lookup as (
        select
            br_position_id,
            year(election_date) as election_year,
            is_special,
            max(election_date) as general_election_date,
            any_value(number_of_seats) as seats_available
        from {{ ref("int__civics_election_stage_ballotready") }}
        where not is_primary and not is_runoff
        group by br_position_id, year(election_date), is_special
    ),

    candidacies_enriched as (
        select
            -- For gp_election_id, we need the general election date
            -- First try the election_stage model's general date (source of
            -- truth), then the candidacy's own general date from S3, then
            -- fall back to primary/runoff date
            coalesce(
                ged.general_election_date,
                rolled.general_election_date,
                rolled.primary_election_date,
                rolled.general_runoff_election_date,
                rolled.primary_runoff_election_date
            ) as election_date,

            -- seats_available from the election_stage model (API race data) for
            -- consistency with gp_election_id generation in the election models
            coalesce(ged.seats_available, rolled.seats_available) as seats_available,

            -- parties format: [{"name"=>"Nonpartisan", "short_name"=>"NP"}]
            {{ parse_party_affiliation("parties") }} as party_affiliation,

            case
                when raw_election_result in ('WON', 'GENERAL_WIN')
                then 'Won'
                when raw_election_result in ('LOST', 'LOSS')
                then 'Lost'
                when raw_election_result = 'PRIMARY_WIN'
                then 'Won'
                when raw_election_result = 'RUNOFF'
                then 'Runoff'
                when raw_election_result = ''
                then null
                else null
            end as candidacy_result,

            -- Compute office_type here so it's available for generate_gp_election_id
            {{ map_office_type("candidate_office") }} as office_type,

            rolled.* except (seats_available)

        from candidacy_rolled_up as rolled
        left join
            general_election_date_lookup as ged
            on rolled.br_position_id = ged.br_position_id
            and rolled.election_year = ged.election_year
            and rolled.is_special = ged.is_special
    ),

    candidacies_with_ids as (
        select
            rolled_gp_candidacy_id as gp_candidacy_id,

            -- gp_candidate_id is the person id (person-group earliest-member
            -- mint), shared with int__civics_candidate_ballotready. Every
            -- br_candidate_id is in the person universe by construction; the
            -- coalesce is a full-refresh guard with identical single-member
            -- semantics.
            coalesce(
                p.gp_person_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "'ballotready'",
                            "cast(br_candidate_id as string)",
                        ],
                        salt="person",
                    )
                }}
            ) as gp_candidate_id,

            -- gp_election_id - use the generate_gp_election_id macro
            -- The macro expects columns without table prefix in the current
            -- scope. is_special partitions special vs regular cycles into
            -- distinct hashes, matching int__civics_election_stage_ballotready.
            {{ generate_gp_election_id(is_special_expr="is_special") }}
            as gp_election_id,

            -- External IDs (NULL for BR-sourced records until we link them in
            -- followup work)
            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,

            -- Source tracking
            'ballotready' as candidate_id_source,

            -- Candidacy attributes
            party_affiliation,
            -- BallotReady does not provide incumbent or open seat data
            cast(null as boolean) as is_incumbent,
            cast(null as boolean) as is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            -- Hardcoded until we link BallotReady candidacies with Product DB
            cast(null as boolean) as is_pledged,
            cast(null as boolean) as is_verified,
            cast(null as string) as verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,

            -- BallotReady IDs
            br_position_id as br_position_database_id,
            br_candidacy_id,
            br_race_id,

            -- Assessment fields. BallotReady provides none of these: it does not
            -- compute a viability_score, and supplies no win number at all.
            -- Downstream, the candidacy mart fills viability_score from
            -- TechSpeed's model; win_number / win_number_model have no live
            -- source (only the 2023-2025 HubSpot archive in
            -- int__civics_candidacy_2025 carries win_number), so they stay null
            -- on the BR path.
            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            -- Timestamps
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from candidacies_enriched
        left join
            person_ids as p
            on 'ballotready|' || cast(candidacies_enriched.br_candidate_id as string)
            = p.record_key
        where
            -- Must have at least a general or primary election date for ID generation
            coalesce(
                general_election_date,
                primary_election_date,
                general_runoff_election_date,
                primary_runoff_election_date
            )
            is not null
    ),

    -- gp_candidate_id is the same person lookup int__civics_candidate_ballotready
    -- performs, so every candidacy's gp_candidate_id is in the candidate table
    -- by construction; the candidate <-> candidacy relationships tests guard
    -- the invariant.
    deduplicated as (
        select *
        from candidacies_with_ids
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    gp_candidacy_id,
    gp_candidate_id,
    gp_election_id,
    product_campaign_id,
    hubspot_contact_id,
    hubspot_company_ids,
    candidate_id_source,
    party_affiliation,
    is_incumbent,
    is_open_seat,
    candidate_office,
    official_office_name,
    office_level,
    office_type,
    candidacy_result,
    is_pledged,
    is_verified,
    verification_status_reason,
    is_partisan,
    primary_election_date,
    primary_runoff_election_date,
    general_election_date,
    general_runoff_election_date,
    br_position_database_id,
    br_candidacy_id,
    br_race_id,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at
from deduplicated
