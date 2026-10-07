-- One row per BallotReady position (br_position_database_id) carrying the
-- canonical office_type for that position. office_type is derived from
-- BallotReady's normalized position name through the same two-stage transform
-- the BR candidacy path uses:
-- map_office_type(generate_candidate_office_from_position(name, normalized_name))
-- so every source can inherit a single, position-stable office_type by joining
-- on br_position_database_id, instead of deriving office_type from
-- its own free-text office string. Mirrors how int__icp_offices keys on the
-- position. Built from the position universe (every BR position), not from BR
-- candidacies, so positions where only a product or TechSpeed candidate ran are
-- still covered.
with
    position as (
        select database_id, name, normalized_position
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    normalized_position as (
        select database_id, name from {{ ref("int__ballotready_normalized_position") }}
    ),

    with_candidate_office as (
        select
            position.database_id as br_position_database_id,
            normalized_position.name as normalized_position_name,
            {{
                generate_candidate_office_from_position(
                    "position.name", "normalized_position.name"
                )
            }} as candidate_office
        from position
        left join
            normalized_position
            on position.normalized_position.databaseid = normalized_position.database_id
    ),

    with_office_type as (
        select
            br_position_database_id,
            normalized_position_name,
            candidate_office,
            {{ map_office_type("candidate_office") }} as office_type
        from with_candidate_office
    )

select
    br_position_database_id,
    office_type,
    -- office_type feeds the gp_election_id hash, so its categories cannot
    -- move without re-minting election ids. office_category is the reporting
    -- grain: office_type wherever it is specific, and the position's
    -- normalized name sorted into named groups where office_type says Other.
    -- Order matters; earlier rules are the more specific ones.
    case
        when office_type <> 'Other'
        then office_type
        when
            lower(normalized_position_name) like 'president of the united states%'
            or lower(normalized_position_name)
            like 'vice president of the united states%'
        then 'President'
        when
            lower(normalized_position_name) like 'u.s. %'
            or lower(normalized_position_name) like 'delegate to the u.s. house%'
        then 'Congressional'
        when lower(normalized_position_name) like 'judicial %'
        then 'Judge'
        when lower(normalized_position_name) like 'party office%'
        then 'Party Office'
        when
            lower(normalized_position_name) like '%public defender%'
            or lower(normalized_position_name) like '%solicitor general%'
            or lower(normalized_position_name) like '%prosecutor%'
        then 'Attorney'
        when
            lower(normalized_position_name) like '%constable%'
            or lower(normalized_position_name) like '%police chief%'
            or lower(normalized_position_name) like '%marshal%'
            or lower(normalized_position_name) like '%jailer%'
            or lower(normalized_position_name) like '%bailiff%'
            or lower(normalized_position_name) like '%deputy sheriff%'
        then 'Sheriff'
        when
            lower(normalized_position_name) like '%election%'
            or lower(normalized_position_name) like '%registrar of voters%'
            or lower(normalized_position_name) like '%precinct inspector%'
            or lower(normalized_position_name) like '%supervisor of the checklist%'
        then 'Elections Official'
        when
            lower(normalized_position_name) like 'state %'
            or lower(normalized_position_name) like 'secretary of state%'
        then 'Statewide/Governor'
        when
            lower(normalized_position_name) like '%community college%'
            or lower(normalized_position_name) like '%education%'
            or lower(normalized_position_name) like '%school%'
        then 'Education Board'
        when
            lower(normalized_position_name) like '%district board%'
            or lower(normalized_position_name) like '%authority board%'
            or lower(normalized_position_name) = 'council of governments'
        then 'Special District Board'
        -- A joint office that names a clerk is a clerk's office first, so any
        -- clerk title skips the tax rule and reaches Clerk/Treasurer below.
        when
            lower(normalized_position_name) not like '%clerk%'
            and (
                lower(normalized_position_name) like '%assessor%'
                or lower(normalized_position_name) like '%(tax)%'
                or lower(normalized_position_name) like '%tax collector%'
                or lower(normalized_position_name) like '%revenue%'
                or lower(normalized_position_name) like '%tax commissioner%'
                or lower(normalized_position_name) like '%receiver%'
                or lower(normalized_position_name) like '%property%review board%'
            )
        then 'Assessor/Tax Collector'
        when
            lower(normalized_position_name) like '%clerk%'
            or lower(normalized_position_name) like '%recorder%'
            or lower(normalized_position_name) like '%register of%'
            or lower(normalized_position_name) like '%auditor%'
            or lower(normalized_position_name) like '%comptroller%'
            or lower(normalized_position_name) like '%controller%'
            or lower(normalized_position_name) like '%treasurer%'
            or lower(normalized_position_name) like '%prothonotary%'
            or lower(normalized_position_name) like '%probate administrator%'
        then 'Clerk/Treasurer'
        when
            lower(normalized_position_name) like '%highway%'
            or (
                lower(normalized_position_name) like '%road%'
                and lower(normalized_position_name) not like '%railroad%'
            )
            or lower(normalized_position_name) like '%surveyor%'
            or lower(normalized_position_name) like '%engineer%'
            or lower(normalized_position_name) like '%drain commissioner%'
            or lower(normalized_position_name) like '%public works%'
            or lower(normalized_position_name) like '%street%'
            or lower(normalized_position_name) like '%water commissioner%'
            or lower(normalized_position_name) like '%sewer commissioner%'
        then 'Public Works/Roads'
        when
            lower(normalized_position_name) like '%coroner%'
            or lower(normalized_position_name) like '%medical examiner%'
        then 'Coroner'
        when
            lower(normalized_position_name) like '%vice-mayor%'
            or lower(normalized_position_name) like '%mayor pro tem%'
            or lower(normalized_position_name) like '%borough president%'
            or lower(normalized_position_name) like 'city manager%'
        then 'Mayor'
        when
            lower(normalized_position_name) like 'metro council%'
            or lower(normalized_position_name) like '%legislature chair%'
            or lower(normalized_position_name) like '%board of selectmen%'
        then 'City Council'
        -- The township's chief elected officer, beside Township Supervisor.
        when lower(normalized_position_name) = 'township board head'
        then 'Town Council'
        when
            lower(normalized_position_name) like '%board%'
            or lower(normalized_position_name) like '%commission%'
            or lower(normalized_position_name) like '%committee%'
        then 'Board/Commission'
        else 'Other'
    end as office_category,
    -- True when no candidate_office could be derived for the position (the
    -- normalized-position model has no row for it and no position-name pattern
    -- matched), making office_type the NULL-input fallback 'Other' rather than
    -- a real classification. Expected all-false; true rows mean the incremental
    -- normalized-position model is lagging the position universe.
    candidate_office is null as is_office_type_fallback
from with_office_type
where br_position_database_id is not null
