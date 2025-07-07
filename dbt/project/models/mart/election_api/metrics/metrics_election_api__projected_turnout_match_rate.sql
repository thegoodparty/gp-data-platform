{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        on_schema_change="append",
    )
}}


-- use levenshtein distance to match br_position_id to inferred_geoid
-- then calculate the match rate
-- https://spark.apache.org/docs/latest/api/sql/index.html#levenshtein
-- with br_positions as (
-- select distinct br_position_id
-- from {{ ref('m_election_api__race')}}
-- ),
-- l2_matches as (
-- select distinct br_position_id
-- from {{ ref('m_election_api__projected_turnout')}}
-- )
-- select br_positions
select
    levenshtein(
        lower(l2_office_type || ' ' || l2_office_name),
        lower(normalized_position_name || ' ' || position_names[0])
    ) as edit_distance,  -- unbounded metric
    (soundex(l2_office_name) = soundex(position_names[0])) as is_phonetic_match,  -- boolean
    (
        lower(l2_office_type || ' ' || l2_office_name) like lower(
            concat(
                '%', lower(normalized_position_name || ' ' || position_names[0]), '%'
            )
        )
        or lower(
            concat(
                '%', lower(normalized_position_name || ' ' || position_names[0]), '%'
            )
        )
        like lower(l2_office_type || ' ' || l2_office_name)
    ) as has_substring,
    (
        lower(l2_office_type || ' ' || l2_office_name)
        regexp lower(normalized_position_name || ' ' || position_names[0])
        or lower(normalized_position_name || ' ' || position_names[0])
        regexp lower(l2_office_type || ' ' || l2_office_name)
    ) as regex_match,
    abs(
        (
            length(lower(l2_office_type || ' ' || l2_office_name))
            + length(lower(normalized_position_name || ' ' || position_names[0]))
            - 2
            * levenshtein(
                lower(l2_office_type || ' ' || l2_office_name),
                lower(normalized_position_name || ' ' || position_names[0])
            )
        ) / (
            length(lower(l2_office_type || ' ' || l2_office_name))
            + length(lower(normalized_position_name || ' ' || position_names[0]))
        )
    ) as char_overlap_score,  -- check this is correct, abs may not be needed
    tbl_turnout.id as turnout_id,
    tbl_turnout.state as turnout_state,
    tbl_race.state as race_state,
    tbl_turnout.geoid as turnout_geoid,
    tbl_race.position_geoid as race_position_geoid,
    -- l2_district_type,
    -- l2_district_name,
    tbl_turnout.l2_office_type,
    tbl_turnout.l2_office_name,
    tbl_race.position_level,
    tbl_race.normalized_position_name,
    tbl_race.position_names,
    tbl_race.slug,
    tbl_turnout.projected_turnout,
    tbl_turnout.inference_at,
    tbl_turnout.election_year,
    tbl_turnout.election_code,
    tbl_turnout.model_version,
    tbl_turnout.br_position_id as turnout_br_position_id,
    tbl_race.id as race_id,
    tbl_race.place_id,
    tbl_race.br_position_id as race_br_position_id
from {{ ref("m_election_api__projected_turnout") }} as tbl_turnout
left join
    {{ ref("m_election_api__race") }} as tbl_race
    on tbl_turnout.br_position_id = tbl_race.br_position_id
